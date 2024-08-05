/*
   Copyright The Overlaybd Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#include "zfile.h"
#include <vector>
#include <algorithm>
#include <memory>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>
#include <photon/common/utility.h>
#include <photon/common/uuid.h>
#include <photon/fs/virtual-file.h>
#include <photon/fs/forwardfs.h>
#include <photon/photon.h>
#include <photon/thread/thread.h>
#include "crc32/crc32c.h"
#include "compressor.h"
#include <atomic>
#include <thread>

using namespace photon::fs;

#ifdef BENCHMARK
bool io_test = false;
uint32_t zfile_io_cnt = 0;
uint64_t zfile_io_size = 0;
uint64_t zfile_blk_cnt = 0;
#endif

namespace ZFile {

const static size_t BUF_SIZE = 512;
const static uint32_t NOI_WELL_KNOWN_PRIME = 100007;
const static uint8_t FLAG_VALID_FALSE = 0;
const static uint8_t FLAG_VALID_TRUE = 1;
const static uint8_t FLAG_VALID_CRC_CHECK = 2;

inline uint32_t crc32c_salt(void *buf, size_t size) {
    return crc32::crc32c_extend(buf, size, NOI_WELL_KNOWN_PRIME);
}
/* ZFile Format:
    | Header (512B) | dict (optional) | compressed block 0 [checksum0] | compressed block 1
   [checksum1] | ... | compressed block N [checksumN] | jmp_table(index) | Trailer (512 B)|
*/
class CompressionFile : public VirtualReadOnlyFile {
public:
    struct HeaderTrailer {
        static const uint32_t SPACE = 512;

        static uint64_t MAGIC0() {
            static char magic0[] = "ZFile\0\1";
            return *(uint64_t *)magic0;
        }

        static constexpr UUID MAGIC1() {
            return {0x696a7574, 0x792e, 0x6679, 0x4140, {0x6c, 0x69, 0x62, 0x61, 0x62, 0x61}};
        }

        // offset 0, 8
        uint64_t magic0 = MAGIC0();
        UUID magic1 = MAGIC1();
        bool verify_magic() const {
            return magic0 == MAGIC0() && magic1 == MAGIC1();
        }

        // offset 24, 28, 32
        uint32_t size = sizeof(HeaderTrailer);
        // uint32_t __padding = 0;
        uint32_t digest = 0;
        uint64_t flags;

        static const uint32_t FLAG_SHIFT_HEADER = 0; // 1:header     0:trailer
        static const uint32_t FLAG_SHIFT_TYPE = 1;   // 1:data file, 0:index file
        static const uint32_t FLAG_SHIFT_SEALED = 2; // 1:YES,       0:NO
        static const uint32_t FLAG_SHIFT_HEADER_OVERWRITE = 3; // overwrite trailer info to header
        static const uint32_t FLAG_SHIFT_CALC_DIGEST = 4; // caculate digest for zfile header/trailer and jumptable
        static const uint32_t FLAG_SHIFT_IDX_COMP = 5; // compress zfile index(jumptable)

        uint32_t get_flag_bit(uint32_t shift) const {
            return flags & (1 << shift);
        }
        void set_flag_bit(uint32_t shift) {
            flags |= (1 << shift);
        }
        void clr_flag_bit(uint32_t shift) {
            flags &= ~(1 << shift);
        }
        bool is_header() const {
            return get_flag_bit(FLAG_SHIFT_HEADER);
        }
        bool is_header_overwrite() const {
            return get_flag_bit(FLAG_SHIFT_HEADER_OVERWRITE);
        }
        bool is_trailer() const {
            return !is_header();
        }
        bool is_data_file() const {
            return get_flag_bit(FLAG_SHIFT_TYPE);
        }
        bool is_index_file() const {
            return !is_data_file();
        }
        bool is_sealed() const {
            return get_flag_bit(FLAG_SHIFT_SEALED);
        }
        bool is_digest_enabled() {
            return get_flag_bit(FLAG_SHIFT_CALC_DIGEST);
        }
        bool is_valid() {
            if (!is_digest_enabled()) {
                LOG_WARN("digest not found in current zfile.");
                return true;
            }
            auto saved_crc = this->digest;
            this->digest = 0;
            DEFER(this->digest = saved_crc;);
            auto crc = crc32::crc32c(this, CompressionFile::HeaderTrailer::SPACE);
            LOG_INFO("zfile digest: ` (` expected)", HEX(crc).width(8), HEX(saved_crc).width(8));
            return crc == saved_crc;
        }
        void set_header() {
            set_flag_bit(FLAG_SHIFT_HEADER);
        }
        void set_trailer() {
            clr_flag_bit(FLAG_SHIFT_HEADER);
        }
        void set_data_file() {
            set_flag_bit(FLAG_SHIFT_TYPE);
        }
        void set_index_file() {
            clr_flag_bit(FLAG_SHIFT_TYPE);
        }
        void set_sealed() {
            set_flag_bit(FLAG_SHIFT_SEALED);
        }
        void clr_sealed() {
            clr_flag_bit(FLAG_SHIFT_SEALED);
        }
        void set_header_overwrite() {
            set_flag_bit(FLAG_SHIFT_HEADER_OVERWRITE);
        }

        void set_digest_enable() {
            set_flag_bit(FLAG_SHIFT_CALC_DIGEST);
        }

        void set_compress_index() {
            set_flag_bit(FLAG_SHIFT_IDX_COMP);
        }

        void set_compress_option(const CompressOptions &opt) {
            this->opt = opt;
        }

        // offset 40, 48, 56, 64
        uint64_t index_offset; // in bytes
        uint64_t index_size;   // # of SegmentMappings
        uint64_t original_file_size;
        uint32_t index_crc;
        uint32_t reserved_0;
        // offset 72
        CompressOptions opt;

    } /* __attribute__((packed)) */;
    static_assert(sizeof(HeaderTrailer) == 96, "sizeof(HeaderTrailer) != 96");

    struct JumpTable {
        typedef uint16_t uinttype;
        static const uinttype uinttype_max = UINT16_MAX;
        int group_size;

        std::vector<uint64_t> partial_offset;
        std::vector<uinttype> deltas; // partial sum in each page;

        off_t operator[](size_t idx) const {
            // return BASE  + deltas[ idx % (page_size) ]
            off_t part_idx = idx / group_size;
            off_t inner_idx = idx & (group_size - 1);
            auto part_offset = partial_offset[part_idx];
            if (inner_idx) {
                return part_offset + deltas[idx];
            }
            return part_offset;
        }

        size_t size() const {
            return deltas.size();
        }

        int build(const uint32_t *ibuf, size_t n, off_t offset_begin, uint32_t block_size,
                  bool enable_crc) {
            partial_offset.clear();
            deltas.clear();
            group_size = (uinttype_max + 1) / block_size;
            partial_offset.reserve(n / group_size + 1);
            deltas.reserve(n + 1);
            auto raw_offset = offset_begin;
            partial_offset.push_back(raw_offset);
            deltas.push_back(0);
            size_t min_blksize = (enable_crc ? sizeof(uint32_t) : 0);
            for (ssize_t i = 1; i < (ssize_t)n + 1; i++) {
                if (ibuf[i - 1] <= min_blksize) {
                    LOG_ERRNO_RETURN(EIO, -1, "unexpected block size(id: `):", i - 1, ibuf[i - 1]);
                }
                raw_offset += ibuf[i - 1];
                if ((i % group_size) == 0) {
                    partial_offset.push_back(raw_offset);
                    deltas.push_back(0);
                    continue;
                }
                if ((uint64_t)deltas[i - 1] + ibuf[i - 1] >= (uint64_t)uinttype_max) {
                    LOG_ERROR_RETURN(ERANGE, -1, "build block[`] length failed `+` > ` (exceed)",
                                     i - 1, deltas[i - 1], ibuf[i - 1], (uint64_t)uinttype_max);
                }
                deltas.push_back(deltas[i - 1] + ibuf[i - 1]);
            }
            LOG_INFO("create jump table done. {part_count: `, deltas_count: `, size: `}",
                     partial_offset.size(), deltas.size(),
                     deltas.size() * sizeof(deltas[0]) +
                         partial_offset.size() * sizeof(partial_offset[0]));
            return 0;
        }

    } m_jump_table;

    HeaderTrailer m_ht;
    IFile *m_file = nullptr;
    std::unique_ptr<ICompressor> m_compressor;
    bool m_ownership = false;
    uint8_t valid = FLAG_VALID_TRUE;

    CompressionFile(IFile *file, bool ownership) : m_file(file), m_ownership(ownership){};

    ~CompressionFile() {
        if (m_ownership) {
            delete m_file;
        }
    }

    UNIMPLEMENTED_POINTER(IFileSystem *filesystem() override);

    virtual int close() override {
        return 0;
    }

    virtual int fstat(struct stat *buf) override {
        auto ret = m_file->fstat(buf);
        if (ret != 0)
            return ret;
        buf->st_size = m_ht.original_file_size;
        return ret;
    }

    class BlockReader {
    public:
        BlockReader(){};
        BlockReader(const CompressionFile *zfile, size_t offset, size_t count)
            : m_zfile(zfile), m_offset(offset) /* , m_count(count)  */
        {
            m_verify = zfile->m_ht.opt.verify;
            m_block_size = zfile->m_ht.opt.block_size; //+ m_verify * sizeof(uint32_t);
            m_begin_idx = m_offset / m_block_size;
            m_idx = m_begin_idx;
            m_end = m_offset + count - 1;
            m_end_idx = (m_offset + count - 1) / m_block_size + 1;
        }

        int reload(size_t idx) {
            auto read_size = get_blocks_length(idx, idx + 1);
            auto begin_offset = m_zfile->m_jump_table[idx];
            LOG_WARN("trim and reload. (idx: `, offset: `, len: `)", idx, begin_offset, read_size);
            int trim_res = m_zfile->m_file->trim(begin_offset, read_size);
            if (trim_res < 0) {
                LOG_ERRNO_RETURN(0, -1, "trim block failed. (idx: `, offset: `, len: `)", idx,
                                 begin_offset, read_size);
            }
            auto readn = m_zfile->m_file->pread(m_buf + m_buf_offset, read_size, begin_offset);
            if (readn != (ssize_t)read_size) {
                LOG_ERRNO_RETURN(0, -1,
                                 "read compressed blocks failed. (idx: `, offset: `, len: `)", idx,
                                 begin_offset, read_size);
            }
            return 0;
        }

        int read_blocks(size_t begin, size_t end) {
            auto read_size = std::min(MAX_READ_SIZE, get_blocks_length(begin, end));
            auto begin_offset = m_zfile->m_jump_table[begin];
            auto readn = m_zfile->m_file->pread(m_buf, read_size, begin_offset);
            if (readn != (ssize_t)read_size) {
                m_eno = (errno ? errno : EIO);
                LOG_ERRNO_RETURN(0, -1, "read compressed blocks failed. (offset: `, len: `, ret: `)",
                                 begin_offset, read_size, readn);
            }
            return 0;
        }

        __attribute__((always_inline)) size_t get_blocks_length(size_t begin, size_t end) const {
            assert(begin <= end);
            return m_zfile->m_jump_table[end] - m_zfile->m_jump_table[begin];
        }

        __attribute__((always_inline)) size_t get_buf_offset(size_t idx) {
            return get_blocks_length(m_begin_idx, idx);
        }

        __attribute__((always_inline)) bool buf_exceed(size_t idx) {
            return get_blocks_length(m_begin_idx, idx + 1) > sizeof(m_buf);
        }

        __attribute__((always_inline)) size_t get_inblock_offset(size_t offset) {
            return offset % m_block_size;
        }

        __attribute__((always_inline)) size_t block_size() const {
            return m_block_size;
        }

        __attribute__((always_inline)) size_t compressed_size() const {
            return get_blocks_length(m_idx, m_idx + 1) - (m_verify ? sizeof(uint32_t) : 0);
        }

        __attribute__((always_inline)) uint32_t crc32_code() const {
            if (!m_verify) {
                LOG_WARN("crc32 not support.");
                return -1;
            }
            return *(uint32_t *)&(m_buf[m_buf_offset + compressed_size()]);
        }

        struct iterator {
            size_t compressed_size;
            off_t cp_begin;
            size_t cp_len;
            iterator(BlockReader *reader) : m_reader(reader) {
                if (get_current_block() != 0) {
                    m_reader = nullptr;
                }
            }
            iterator() {
            }

            const iterator operator*() {
                return *this;
            };

            const unsigned char *buffer() const {
                return (m_reader->m_buf + m_reader->m_buf_offset);
            }

            const uint32_t crc32_code() const {
                return m_reader->crc32_code();
            }

            int get_current_block() {
                m_reader->m_buf_offset = m_reader->get_buf_offset(m_reader->m_idx);
                if ((size_t)(m_reader->m_buf_offset) >= sizeof(m_buf)) {
                    m_reader->m_eno = ERANGE;
                    LOG_ERRNO_RETURN(0, -1, "get inner buffer offset failed.");
                }

                auto blk_idx = m_reader->m_idx;
                compressed_size = m_reader->compressed_size();
                if ((size_t)(m_reader->m_buf_offset) + compressed_size > sizeof(m_buf)) {
                    m_reader->m_eno = ERANGE;
                    LOG_ERRNO_RETURN(0, -1,
                                     "inner buffer offset (`) + compressed size (`) overflow.",
                                     m_reader->m_buf_offset, compressed_size);
                }

                if (blk_idx == m_reader->m_begin_idx) {
                    cp_begin = m_reader->get_inblock_offset(m_reader->m_offset);
                    m_reader->m_offset = 0;
                } else {
                    cp_begin = 0;
                }
                cp_len = m_reader->block_size();
                if (blk_idx == m_reader->m_end_idx - 1) {
                    cp_len = m_reader->get_inblock_offset(m_reader->m_end) + 1;
                }
                cp_len -= cp_begin;
                return 0;
            }

            iterator &operator++() {
                m_reader->m_idx++;
                if (m_reader->m_idx == m_reader->m_end_idx) {
                    goto end;
                }
                if (m_reader->buf_exceed(m_reader->m_idx)) {
                    if (m_reader->read_blocks(m_reader->m_idx, m_reader->m_end_idx) != 0) {
                        goto end;
                    }
                    m_reader->m_begin_idx = m_reader->m_idx;
                }
                if (get_current_block() != 0) {
                    goto end;
                }
                return *this;
            end:
                m_reader->m_idx = m_reader->m_end_idx;
                this->m_reader = nullptr;
                return *this; // end();
            }

            bool operator!=(const iterator &rhs) const {
                return this->m_reader != rhs.m_reader;
            }

            const int reload() const {
                return m_reader->reload(m_reader->m_idx);
            }

            BlockReader *m_reader = nullptr;
        };

        iterator begin() {
            if (read_blocks(m_idx, m_end_idx) != 0) {
                return iterator();
            };
            return iterator(this);
        }

        iterator end() {
            return iterator();
        }

        const CompressionFile *m_zfile = nullptr;
        off_t m_buf_offset = 0;
        size_t m_begin_idx = 0;
        size_t m_idx = 0;
        size_t m_end_idx = 0;
        off_t m_offset = 0;
        off_t m_end = 0;
        uint8_t m_verify = 0;
        uint32_t m_block_size = 0;
        uint8_t m_eno = 0;
        unsigned char m_buf[MAX_READ_SIZE]; //{};
    };

    virtual ssize_t pread(void *buf, size_t count, off_t offset) override {

        if (m_ht.opt.block_size > MAX_READ_SIZE) {
            LOG_ERROR_RETURN(ENOMEM, -1, "block_size: ` > MAX_READ_SIZE (`)", m_ht.opt.block_size,
                             MAX_READ_SIZE);
        }
        ssize_t cnt = count;
        if (offset + cnt > (ssize_t)m_ht.original_file_size) {
            LOG_WARN("the read range exceeds raw_file_size.(`>`)", cnt + offset,
                     m_ht.original_file_size);
            cnt = m_ht.original_file_size - offset;
        }
        if (cnt <= 0) {
            LOG_WARN("the read offset exceeds raw_file_size.(`>`)", offset,
                     m_ht.original_file_size);
            return 0;
        }
        ssize_t readn = 0; // final will equal to count
        unsigned char raw[MAX_READ_SIZE];
        BlockReader br(this, offset, cnt);
        for (auto &block : br) {
            if (buf == nullptr) {
                // used for prefetch; no copy, no decompress;
                readn += block.cp_len;
                continue;
            }
            int retry = 3;
        again:
            if (m_ht.opt.verify) {
                auto c = crc32c_salt((void *)block.buffer(), block.compressed_size);
                if (c != block.crc32_code()) {
                    if ((valid == FLAG_VALID_TRUE) && (retry--)) {
                        int reload_res = block.reload();
                        LOG_ERROR(
                            "checksum failed {offset: `, length: `} (expected ` but got `), reload result: `",
                            block.m_reader->m_buf_offset, block.compressed_size, HEX(block.crc32_code()).width(8),
                            HEX(c).width(8), reload_res);
                        if (reload_res < 0) {
                            LOG_ERROR_RETURN(ECHECKSUM, -1,
                                             "checksum verification and reload failed");
                        }
                        goto again;
                    } else {
                        LOG_ERROR_RETURN(
                            ECHECKSUM, -1,
                            "checksum verification failed after retries {offset: `, length: `}",
                            block.m_reader->m_buf_offset, block.compressed_size);
                    }
                }
            }
            if (valid == FLAG_VALID_CRC_CHECK) {
                LOG_DEBUG("only check crc32 and skip decompression.");
                readn += block.cp_len;
                continue;
            }
            int dret = -1;
            if (block.cp_len == m_ht.opt.block_size) {
                dret = m_compressor->decompress(block.buffer(), block.compressed_size,
                                                (unsigned char *)buf, m_ht.opt.block_size);
            } else {
                dret = m_compressor->decompress(block.buffer(), block.compressed_size, raw,
                                                m_ht.opt.block_size);
                if (dret != -1)
                    memcpy(buf, raw + block.cp_begin, block.cp_len);
            }
            if (dret == -1) {
                if (retry--) {
                    int reload_res = block.reload();
                    LOG_ERROR("decompression failed {offset: `, length: `}, reload result: `",
                        block.m_reader->m_buf_offset, block.compressed_size, reload_res);
                    if (reload_res < 0) {
                        LOG_ERRNO_RETURN(0, -1, "decompression and reload failed");
                    }
                    goto again;
                }
                LOG_ERRNO_RETURN(0, -1,
                                 "decompression failed after retries, {offset: `, length: `}",
                                 block.m_reader->m_buf_offset, block.compressed_size);
            }
            readn += block.cp_len;
            buf = (unsigned char *)buf + block.cp_len;
        }
        if (br.m_eno != 0) {
            LOG_ERRNO_RETURN(br.m_eno, -1, "read compressed data failed.");
        }
        return readn;
    }
};

static int write_header_trailer(IFile *file, bool is_header, bool is_sealed, bool is_data_file,
                                CompressionFile::HeaderTrailer *pht, off_t offset = -1);

ssize_t compress_data(ICompressor *compressor, const unsigned char *buf, size_t count,
                      unsigned char *dest_buf, size_t dest_len, bool gen_crc) {

    ssize_t compressed_len = 0;
    auto ret = compressor->compress((const unsigned char *)buf, count, dest_buf, dest_len);
    if (ret <= 0) {
        LOG_ERRNO_RETURN(0, -1, "compress data failed.");
    }
    // LOG_DEBUG("compress buffer {offset: `, count: `} into ` bytes.", i, step, ret);
    compressed_len = ret;
    if (gen_crc) {
        auto crc32_code = crc32c_salt(dest_buf, compressed_len);
        *((uint32_t *)&dest_buf[compressed_len]) = crc32_code;
        LOG_DEBUG("append ` bytes crc32_code: `", sizeof(uint32_t), crc32_code);
        compressed_len += sizeof(uint32_t);
    }
    LOG_DEBUG("compressed ` bytes into ` bytes.", count, compressed_len);
    return compressed_len;
}

class ZFileBuilderBase : public VirtualReadOnlyFile {
public:
    virtual int init() = 0;
    virtual int fini() = 0;
};

class ZFileBuilder : public ZFileBuilderBase {
public:
    ZFileBuilder(IFile *file, const CompressArgs *args, bool ownership)
        : m_dest(file), m_args(args), m_ownership(ownership) {
        m_opt = m_args->opt;
        LOG_INFO("create stream compressing object. [ block size: `, type: `, enable_checksum: `]",
                 m_opt.block_size, m_opt.algo, m_opt.verify);
    }

    int init() {
        m_compressor = create_compressor(m_args);
        if (m_compressor == nullptr) {
            LOG_ERRNO_RETURN(0, -1, "create compressor failed.");
        }
        auto pht = new (m_ht)(CompressionFile::HeaderTrailer);
        pht->set_compress_option(m_opt);
        LOG_INFO("write header.");
        auto ret = write_header_trailer(m_dest, true, false, true, pht);
        if (ret < 0) {
            LOG_ERRNO_RETURN(0, -1, "failed to write header");
        }
        moffset =
            CompressionFile::HeaderTrailer::SPACE + 0; // opt.dict_size;
                                                       // currently dictionary is not supported.
        m_buf_size = m_opt.block_size + BUF_SIZE;
        compressed_data = new unsigned char[m_buf_size];
        reserved_buf = new unsigned char[m_buf_size];
        return 0;
    }

    int write_buffer(const unsigned char *buf, size_t count) {
        auto compressed_len =
            compress_data(m_compressor, buf, count, compressed_data, m_buf_size, m_opt.verify);
        if (compressed_len <= 0) {
            LOG_ERRNO_RETURN(EIO, -1, "compress buffer failed.");
        }
        if (m_dest->write(compressed_data, compressed_len) != compressed_len) {
            LOG_ERRNO_RETURN(0, -1, "write compressed data failed.");
        }
        m_block_len.push_back(compressed_len);
        moffset += compressed_len;
        return 0;
    }

    int fini() {
        if (reserved_size) {
            LOG_INFO("compress reserved data.");
            if (write_buffer(reserved_buf, reserved_size) != 0)
                return -1;
        }
        uint64_t index_offset = moffset;
        uint64_t index_size = m_block_len.size();
        ssize_t index_bytes = index_size * sizeof(uint32_t);
        LOG_INFO("write index (offset: `, count: ` size: `)", index_offset, index_size,
                 index_bytes);
        if (m_dest->write(&m_block_len[0], index_bytes) != index_bytes) {
            LOG_ERRNO_RETURN(0, -1, "failed to write index.");
        }
        auto pht = (CompressionFile::HeaderTrailer *)m_ht;
        pht->index_crc = crc32::crc32c(&m_block_len[0], index_bytes);
        pht->index_offset = index_offset;
        pht->index_size = index_size;
        pht->original_file_size = raw_data_size;
        LOG_INFO("write trailer.");
        auto ret = write_header_trailer(m_dest, false, true, true, pht);
        if (ret < 0)
            LOG_ERRNO_RETURN(0, -1, "failed to write trailer");
        if (m_args->overwrite_header) {
            LOG_INFO("overwrite file header.");
            ret = write_header_trailer(m_dest, true, false, true, pht, 0);
            if (ret < 0) {
                LOG_ERRNO_RETURN(0, -1, "failed to overwrite header");
            }
        }
        return 0;
    }

    virtual int close() override {
        if (fini() < 0)
            return -1;
        delete m_compressor;
        delete[] compressed_data;
        if (m_ownership) {
            m_dest->close();
        }
        return 0;
    }

    virtual ssize_t write(const void *buf, size_t count) override {
        LOG_DEBUG("generate zfile data(raw_data size: `)", count);
        raw_data_size += count;
        auto expected_ret = count;
        if (reserved_size != 0) {
            if (reserved_size + count < m_opt.block_size) {
                memcpy(reserved_buf + reserved_size, buf, count);
                reserved_size += count;
                return expected_ret; // save uncompressed buffer and return write count;
            }
            auto delta = m_opt.block_size - reserved_size;
            memcpy(reserved_buf + reserved_size, (unsigned char *)buf, delta);
            buf = (const void *)((unsigned char *)buf + delta);
            count -= delta;
            if (write_buffer(reserved_buf, reserved_size + delta) != 0) {
                LOG_ERRNO_RETURN(EIO, -1, "compress buffer failed.");
            }
            reserved_size = 0;
        }
        for (off_t i = 0; i < (ssize_t)count; i += m_opt.block_size) {
            if (i + m_opt.block_size > (ssize_t)count) {
                memcpy(reserved_buf, (unsigned char *)buf + i, count - i);
                reserved_size = count - i;
                LOG_DEBUG("reserved data size: `", reserved_size);
                break;
            }
            auto ret = write_buffer((unsigned char *)buf + i, m_opt.block_size);
            if (ret != 0) {
                LOG_ERRNO_RETURN(EIO, -1, "compress buffer failed.");
            }
        }
        LOG_DEBUG("compressed ` bytes done. reserved: `", expected_ret, reserved_size);
        return expected_ret;
    }

    IFile *m_dest;
    off_t moffset = 0;
    size_t raw_data_size = 0;
    size_t m_buf_size = 0;
    const CompressArgs *m_args;
    CompressOptions m_opt;
    ICompressor *m_compressor = nullptr;
    bool m_ownership = false;
    std::vector<uint32_t> m_block_len;
    unsigned char *compressed_data = nullptr;
    unsigned char *reserved_buf = nullptr;
    size_t reserved_size = 0;
    char m_ht[CompressionFile::HeaderTrailer::SPACE]{};

    UNIMPLEMENTED_POINTER(IFileSystem *filesystem() override);
    UNIMPLEMENTED(int fstat(struct stat *buf) override);
};

// multi-processor supported zfile builder
class ZFileBuilderMP : public ZFileBuilderBase {
public:
    ZFileBuilderMP(IFile *file, const CompressArgs *args, bool ownership)
        : m_dest(file), m_args(args), m_ownership(ownership) {
        m_workers = args->workers;
        m_opt = m_args->opt;
        LOG_INFO("create multi-processor stream compressing object. [ block size: `, alog: `, enable_checksum: `, workers: `]",
                 m_opt.block_size, m_opt.algo, m_opt.verify, m_workers);
    }

    class WorkerCtx {
    public:
        int id;
        bool writable = false;
        unsigned char* ibuf = nullptr;
        unsigned char* obuf = nullptr;
        size_t size;
        size_t buf_size;
        photon::semaphore writable_sem;
        photon::semaphore compress_sem;
        photon::semaphore write_sem;
        int result = 0;

        WorkerCtx(int id, size_t buf_size)
            : id(id), buf_size(buf_size), writable_sem(1), compress_sem(0), write_sem(0) {
            ibuf = new unsigned char[buf_size];
            obuf = new unsigned char[buf_size];
        }

        ~WorkerCtx() {
            delete ibuf;
            delete obuf;
        }
        void start_compress(int isize) {
            writable = false;
            size = isize;
            compress_sem.signal(1);
        }
    };

    int init() {
        auto pht = new (m_ht)(CompressionFile::HeaderTrailer);
        pht->set_compress_option(m_opt);
        LOG_INFO("write header.");
        auto ret = write_header_trailer(m_dest, true, false, true, pht);
        if (ret < 0) {
            LOG_ERRNO_RETURN(0, -1, "failed to write header");
        }
        moffset =
            CompressionFile::HeaderTrailer::SPACE + 0; // opt.dict_size;
                                                       // currently dictionary is not supported.
        m_buf_size = m_opt.block_size + BUF_SIZE;
        cur_id = 0;
        for (int i = 0; i < m_workers; i++)
            workers.emplace_back(new WorkerCtx(i, m_buf_size));

        for (int i = 0; i < m_workers; i++) {
            ths.emplace_back([&, id=i] {
                photon::init(photon::INIT_EVENT_EPOLL, photon::INIT_IO_NONE);
                DEFER(photon::fini());

                auto ctx = workers[id];
                auto next_ctx = workers[(id+1)%m_workers];
                auto compressor = create_compressor(m_args);
                if (compressor == nullptr) {
                    ctx->result = -1;
                    LOG_ERRNO_RETURN(0, -1, "failed to create compressor");
                }
                DEFER(delete compressor);

                while (true) {
                    ctx->compress_sem.wait(1);
                    if (m_stop_flag && ctx->size == 0) {
                        break;
                    }
                    auto compressed_size =
                        compress_data(compressor, ctx->ibuf, ctx->size, ctx->obuf, ctx->buf_size, m_opt.verify);
                    if (compressed_size < 0) {
                        ctx->result = -1;
                        LOG_ERRNO_RETURN(EIO, -1, "failed to compress");
                    }

                    ctx->size = 0;
                    // ibuf is ready to write
                    ctx->writable_sem.signal(1);

                    ctx->write_sem.wait(1);
                    moffset += compressed_size;
                    m_block_len.push_back(compressed_size);
                    if (m_dest->write(ctx->obuf, compressed_size) != compressed_size) {
                        ctx->result = -1;
                        LOG_ERRNO_RETURN(0, -1, "failed to write compressed data");
                    }
                    next_ctx->write_sem.signal(1);
                }
                return 0;
            });
        }

        workers[0]->write_sem.signal(1);
        return 0;
    }

    int fini() {
        if (reserved_size) {
            workers[cur_id]->start_compress(reserved_size);
        }

        // wait for workers
        m_stop_flag = true;
        for (int i = 0; i < m_workers; i++)
            workers[i]->compress_sem.signal(1);
        for (auto &th : ths) {
            th.join();
        }
        for (int i = 0; i < m_workers; i++) {
            if (workers[i]->result < 0) {
                LOG_ERROR_RETURN(0, -1, "failed to compress data");
            }
        }

        // compress done
        uint64_t index_offset = moffset;
        uint64_t index_size = m_block_len.size();
        ssize_t index_bytes = index_size * sizeof(uint32_t);
        LOG_INFO("write index (offset: `, count: ` size: `)", index_offset, index_size, index_bytes);
        if (m_dest->write(&m_block_len[0], index_bytes) != index_bytes) {
            LOG_ERRNO_RETURN(0, -1, "failed to write index.");
        }
        auto pht = (CompressionFile::HeaderTrailer *)m_ht;
        pht->index_crc = crc32::crc32c(&m_block_len[0], index_bytes);
        LOG_INFO("index crc: ", pht->index_crc);
        pht->index_offset = index_offset;
        pht->index_size = index_size;
        pht->original_file_size = raw_data_size;
        LOG_INFO("write trailer.");
        auto ret = write_header_trailer(m_dest, false, true, true, pht);
        if (ret < 0)
            LOG_ERRNO_RETURN(0, -1, "failed to write trailer");
        if (m_args->overwrite_header) {
            LOG_INFO("overwrite file header.");
            ret = write_header_trailer(m_dest, true, false, true, pht, 0);
            if (ret < 0) {
                LOG_ERRNO_RETURN(0, -1, "failed to overwrite header");
            }
        }
        return 0;
    }

    virtual int close() override {
        if (fini() < 0) {
            return -1;
        }
        if (m_ownership) {
            m_dest->close();
        }
        return 0;
    }

    inline void copy(WorkerCtx *ctx, const void *from, size_t count, off_t offset) {
        if (!ctx->writable) {
            ctx->writable_sem.wait(1);
            ctx->writable = true;
        }
        memcpy(ctx->ibuf + offset, from, count);
    }

    virtual ssize_t write(const void *buf, size_t count) override {
        raw_data_size += count;
        auto expected_ret = count;
        auto ctx = workers[cur_id];

        if (reserved_size != 0) {
            if (reserved_size + count < m_opt.block_size) {
                copy(ctx, buf, count, reserved_size);
                reserved_size += count;
                return expected_ret; // save uncompressed buffer and return write count;
            }
            auto delta = m_opt.block_size - reserved_size;
            copy(ctx, buf, delta, reserved_size);
            buf = (const void *)((const char*)buf + delta);
            count -= delta;

            ctx->start_compress(reserved_size + delta);
            cur_id = (cur_id+1)%m_workers;
            ctx = workers[cur_id];
            reserved_size = 0;
        }

        for (off_t i = 0; i < (ssize_t)count; i += m_opt.block_size) {
            if (i + m_opt.block_size > (ssize_t)count) {
                copy(ctx, buf+i, count-i, 0);
                reserved_size = count - i;
                break;
            }
            copy(ctx, buf+i, m_opt.block_size, 0);
            ctx->start_compress(m_opt.block_size);
            cur_id = (cur_id+1)%m_workers;
            ctx = workers[cur_id];
        }
        LOG_DEBUG("compressed ` bytes done. reserved: `", expected_ret, reserved_size);
        return expected_ret;
    }

    std::vector<WorkerCtx*> workers;
    bool m_stop_flag = false;
    int m_workers;
    IFile *m_dest;
    off_t moffset = 0;
    size_t raw_data_size = 0;
    size_t m_buf_size = 0;
    const CompressArgs *m_args;
    CompressOptions m_opt;
    bool m_ownership = false;
    std::vector<uint32_t> m_block_len;
    std::vector<std::thread> ths;
    size_t reserved_size = 0;
    char m_ht[CompressionFile::HeaderTrailer::SPACE]{};
    int cur_id;

    UNIMPLEMENTED_POINTER(IFileSystem *filesystem() override);
    UNIMPLEMENTED(int fstat(struct stat *buf) override);
};

bool load_jump_table(IFile *file, CompressionFile::HeaderTrailer *pheader_trailer,
                     CompressionFile::JumpTable &jump_table, bool trailer = true) {
    char buf[CompressionFile::HeaderTrailer::SPACE];
    auto ret = file->pread(buf, CompressionFile::HeaderTrailer::SPACE, 0);
    if (ret < (ssize_t)CompressionFile::HeaderTrailer::SPACE) {
        LOG_ERRNO_RETURN(0, false, "failed to read file header.");
    }

    auto pht = (CompressionFile::HeaderTrailer *)buf;
    if (!pht->verify_magic() || !pht->is_header()) {
        LOG_ERROR_RETURN(0, false, "header magic/type don't match");
    }
    if (pht->is_valid() == false) {
        LOG_ERROR_RETURN(0, false, "digest verification failed.");
    }
    struct stat stat;
    ret = file->fstat(&stat);
    if (ret < 0) {
        LOG_ERRNO_RETURN(0, false, "failed to stat file.");
    }
    uint64_t index_bytes = 0;
    if (!pht->is_header_overwrite()) {
        struct stat stat;
        ret = file->fstat(&stat);
        if (ret < 0) {
            LOG_ERRNO_RETURN(0, false, "failed to stat file.");
        }
        if (!pht->is_data_file()) {
            LOG_ERROR_RETURN(0, false, "uncognized file type");
        }

        auto trailer_offset = stat.st_size - CompressionFile::HeaderTrailer::SPACE;
        ret = file->pread(buf, CompressionFile::HeaderTrailer::SPACE, trailer_offset);
        if (ret < (ssize_t)CompressionFile::HeaderTrailer::SPACE)
            LOG_ERRNO_RETURN(0, false, "failed to read file trailer.");

        if (!pht->verify_magic() || !pht->is_trailer() || !pht->is_data_file() ||
            !pht->is_sealed()) {
            LOG_ERROR_RETURN(0, false,
                             "trailer magic, trailer type, file type or sealedness doesn't match");
        }

        index_bytes = pht->index_size * sizeof(uint32_t);
        LOG_INFO("trailer_offset: `, idx_offset: `, idx_bytes: `, dict_size: `, use_dict: `",
                 trailer_offset, pht->index_offset, index_bytes, pht->opt.dict_size,
                 pht->opt.use_dict);

        if (index_bytes > trailer_offset - pht->index_offset)
            LOG_ERROR_RETURN(0, false, "invalid index bytes or size. ");
    } else {
        index_bytes = pht->index_size * sizeof(uint32_t);
        LOG_INFO("read overwrite header. idx_offset: `, idx_bytes: `, dict_size: `, use_dict: `",
                 pht->index_offset, index_bytes, pht->opt.dict_size, pht->opt.use_dict);
    }
    auto ibuf = new uint32_t[pht->index_size];
    DEFER(delete[] ibuf);
    // auto ibuf = std::unique_ptr<uint32_t[]>(new uint32_t[pht->index_size]);
    LOG_INFO(VALUE((void*) ibuf), VALUE(index_bytes));
    LOG_DEBUG("index_offset: `", pht->index_offset);
    ret = file->pread((void *)(ibuf), index_bytes, pht->index_offset);
    if (ret < (ssize_t)index_bytes) {
        LOG_ERRNO_RETURN(0, false, "failed to read index");
    }
    if (pht->is_digest_enabled()) {
        LOG_INFO("check jumptable CRC32 (` expected)", HEX(pht->index_crc).width(8));
        LOG_INFO(VALUE((void*) ibuf), VALUE(index_bytes));
        auto crc = crc32::crc32c(ibuf, index_bytes);
        if (crc != pht->index_crc) {
            LOG_ERRNO_RETURN(0, false, "checksum of jumptable is incorrect. {got: `, expected: `}",
                 HEX(crc).width(8),  HEX(pht->index_crc).width(8)
            );
        }
    }
    ret = jump_table.build(ibuf, pht->index_size,
                           CompressionFile::HeaderTrailer::SPACE + pht->opt.dict_size,
                           pht->opt.block_size, pht->opt.verify);
    if (ret != 0) {
        LOG_ERRNO_RETURN(0, false, "failed to build jump table");
    }

    if (pheader_trailer)
        *pheader_trailer = *pht;
    return true;
}

IFile *zfile_open_ro(IFile *file, bool verify, bool ownership) {
    if (!file) {
        LOG_ERRNO_RETURN(EINVAL, nullptr, "invalid file ptr. (NULL)");
    }
    CompressionFile::HeaderTrailer ht;
    CompressionFile::JumpTable jump_table;
    int retry = 2;
again:
    if (!load_jump_table(file, &ht, jump_table, true)) {
        if (verify) {
            // verify means the source can be evicted. evict and retry
            auto res = file->fallocate(0, 0, -1);
            LOG_ERROR("failed to load jump table, fallocate result: `", res);
            if (res < 0) {
                LOG_ERRNO_RETURN(0, nullptr, "failed to load jump table and failed to evict");
            }
            if (retry--) {
                LOG_INFO("retry loading jump table");
                goto again;
            }
        }
        LOG_ERRNO_RETURN(0, nullptr, "failed to load jump table");
    }
    auto zfile = new CompressionFile(file, ownership);
    zfile->m_ht = ht;
    zfile->m_jump_table = std::move(jump_table);
    CompressArgs args(ht.opt);
    ht.opt.verify = ht.opt.verify && verify;
    LOG_INFO("digest: `, compress type: `, bs: `, data_verify: `",
        HEX(ht.digest).width(8), ht.opt.algo, ht.opt.block_size, ht.opt.verify);

    zfile->m_compressor.reset(create_compressor(&args));
    zfile->m_ownership = ownership;
    zfile->valid = FLAG_VALID_TRUE;
    return zfile;
}

static int write_header_trailer(IFile *file, bool is_header, bool is_sealed, bool is_data_file,
                                CompressionFile::HeaderTrailer *pht, off_t offset) {

    if (is_header)
        pht->set_header();
    else
        pht->set_trailer();
    if (is_sealed)
        pht->set_sealed();
    else
        pht->clr_sealed();
    if (is_data_file)
        pht->set_data_file();
    else
        pht->set_index_file();
    if (offset != -1)
        pht->set_header_overwrite();

    pht->set_digest_enable(); // by default
    pht->digest = 0;
    pht->digest = crc32::crc32c(pht, CompressionFile::HeaderTrailer::SPACE);
    LOG_INFO("save header/trailer with digest: `", HEX(pht->digest).width(8));
    if (offset == -1) {
        return (int)file->write(pht, CompressionFile::HeaderTrailer::SPACE);
    }
    return (int)file->pwrite(pht, CompressionFile::HeaderTrailer::SPACE, offset);
}

int zfile_compress(IFile *file, IFile *as, const CompressArgs *args) {
    if (args == nullptr) {
        LOG_ERROR_RETURN(EINVAL, -1, "CompressArgs is null");
    }
    if (file == nullptr || as == nullptr) {
        LOG_ERROR_RETURN(EINVAL, -1, "file ptr is NULL (file: `, as: `)", file, as);
    }
    CompressOptions opt = args->opt;
    LOG_INFO("create compress file. [ block size: `, type: `, enable_checksum: `]", opt.block_size,
             opt.algo, opt.verify);
    auto compressor = create_compressor(args);
    DEFER(delete compressor);
    if (compressor == nullptr)
        return -1;
    char buf[CompressionFile::HeaderTrailer::SPACE] = {};
    auto pht = new (buf) CompressionFile::HeaderTrailer;
    pht->set_compress_option(opt);
    LOG_INFO("write header.");
    auto ret = write_header_trailer(as, true, false, true, pht);
    if (ret < 0) {
        LOG_ERRNO_RETURN(0, -1, "failed to write header");
    }
    auto block_size = opt.block_size;
    LOG_INFO("block size: `", block_size);
    auto buf_size = block_size + BUF_SIZE;
    bool crc32_verify = opt.verify;
    std::vector<uint32_t> block_len{};
    uint64_t moffset = CompressionFile::HeaderTrailer::SPACE + opt.dict_size;
    int nbatch = compressor->nbatch();
    LOG_DEBUG("nbatch: `, buffer need allocate: `", nbatch, nbatch * buf_size);
    auto raw_data = new unsigned char[nbatch * buf_size];
    DEFER(delete[] raw_data);
    auto compressed_data = new unsigned char[nbatch * buf_size];
    DEFER(delete[] compressed_data);
    std::vector<size_t> raw_chunk_len, compressed_len;
    compressed_len.resize(nbatch);
    raw_chunk_len.resize(nbatch);
    LOG_INFO("compress with start....");
    off_t infile_size = 0;
    while (true) {
        int n = 0;
        auto readn = file->read(raw_data, block_size * nbatch);
        if (readn == 0) {
            break;
        }
        if (readn < 0) {
            LOG_ERRNO_RETURN(0, -1, "failed to read from source file. (readn: `)", readn);
        }
        infile_size += readn;
        while (readn > 0) {
            if (readn < block_size) {
                raw_chunk_len[n++] = readn;
                break;
            }
            raw_chunk_len[n++] = block_size;
            readn -= block_size;
        }
        readn = compressor->compress_batch(raw_data, &(raw_chunk_len[0]), compressed_data,
                                         n * buf_size, &(compressed_len[0]), n);
        if (readn != 0)
            return -1;
        for (off_t j = 0; j < n; j++) {
            readn = as->write(&compressed_data[j * buf_size], compressed_len[j]);
            if (readn < (ssize_t)compressed_len[j]) {
                LOG_ERRNO_RETURN(0, -1, "failed to write compressed data.");
            }
            if (crc32_verify) {
                auto crc32_code = crc32c_salt(&compressed_data[j * buf_size], compressed_len[j]);
                LOG_DEBUG("append ` bytes crc32_code: {offset: `, count: `, crc32: `}",
                          sizeof(uint32_t), moffset, compressed_len[j], HEX(crc32_code).width(8));
                compressed_len[j] += sizeof(uint32_t);
                readn = as->write(&crc32_code, sizeof(uint32_t));
                if (readn < (ssize_t)sizeof(uint32_t)) {
                    LOG_ERRNO_RETURN(0, -1, "failed to write crc32code, offset: `, crc32: `",
                                     moffset, HEX(crc32_code).width(8));
                }
            }
            block_len.push_back(compressed_len[j]);
            moffset += compressed_len[j];
        }
    }
    uint64_t index_offset = moffset;
    uint64_t index_size = block_len.size();
    ssize_t index_bytes = index_size * sizeof(uint32_t);
    LOG_INFO("write index (offset: `, count: ` size: `)", index_offset, index_size, index_bytes);
    if (as->write(&block_len[0], index_bytes) != index_bytes) {
        LOG_ERRNO_RETURN(0, -1, "failed to write index.");
    }
    pht->index_crc = crc32::crc32c(&block_len[0], index_bytes);
    LOG_INFO("index checksum: `", HEX(pht->index_crc).width(8));
    pht->index_offset = index_offset;
    pht->index_size = index_size;
    pht->original_file_size = infile_size;
    LOG_INFO("write trailer. (source file size: `)", infile_size);
    ret = write_header_trailer(as, false, true, true, pht);
    if (ret < 0)
        LOG_ERRNO_RETURN(0, -1, "failed to write trailer");
    if (args->overwrite_header) {
        LOG_INFO("overwrite file header.");
        ret = write_header_trailer(as, true, false, true, pht, 0);
        if (ret < 0) {
            LOG_ERRNO_RETURN(0, -1, "failed to overwrite header");
        }
    }
    return 0;
}

int zfile_decompress(IFile *src, IFile *dst) {
    auto file = (CompressionFile *)zfile_open_ro(src, /*verify = */ true);
    DEFER(delete file);
    if (file == nullptr) {
        LOG_ERROR_RETURN(0, -1, "failed to read file.");
    }
    struct stat _st;
    file->fstat(&_st);
    auto raw_data_size = _st.st_size;
    size_t block_size = file->m_ht.opt.block_size;

    auto raw_buf = std::unique_ptr<unsigned char[]>(new unsigned char[block_size]);
    for (off_t offset = 0; offset < raw_data_size; offset += block_size) {
        auto len = (ssize_t)std::min(block_size, (size_t)raw_data_size - offset);
        auto readn = file->pread(raw_buf.get(), len, offset);
        LOG_DEBUG("readn: `, crc32: `", readn, HEX(crc32c_salt(raw_buf.get(), len)).width(8));
        if (readn != len)
            return -1;
        if (dst->write(raw_buf.get(), readn) != readn) {
            LOG_ERRNO_RETURN(0, -1, "failed to write file into dst");
        }
    }
    return 0;
}

int zfile_validation_check(IFile *src) {
    auto file = (CompressionFile *)zfile_open_ro(src, /*verify = */ true);
    DEFER(delete file);
    if (file == nullptr) {
        LOG_ERROR_RETURN(0, -1, "failed to read file.");
    }
    if (file->m_ht.opt.verify == 0) {
        LOG_ERROR_RETURN(0, -1, "source file doesn't have checksum.");
    }
    file->valid = FLAG_VALID_CRC_CHECK;
    struct stat _st;
    file->fstat(&_st);
    auto raw_data_size = _st.st_size;
    size_t block_size = file->m_ht.opt.block_size;
    auto raw_buf = std::unique_ptr<unsigned char[]>(new unsigned char[block_size]);
    for (off_t offset = 0; offset < raw_data_size; offset += block_size) {
        auto len = (ssize_t)std::min(block_size, (size_t)raw_data_size - offset);
        auto readn = file->pread(raw_buf.get(), len, offset);
        if (readn != len) {
            LOG_ERROR_RETURN(0, -1, "crc check error in block `", offset / block_size);
        }
    }
    return 0;
}

int is_zfile(IFile *file) {
    if (!file) {
        LOG_ERROR_RETURN(0, -1, "file is nullptr.");
    }
    char buf[CompressionFile::HeaderTrailer::SPACE];
    auto ret = file->pread(buf, CompressionFile::HeaderTrailer::SPACE, 0);
    if (ret < (ssize_t)CompressionFile::HeaderTrailer::SPACE)
        LOG_ERRNO_RETURN(0, -1, "failed to read file header.");
    auto pht = (CompressionFile::HeaderTrailer *)buf;
    if (!pht->verify_magic() || !pht->is_header()) {
        LOG_DEBUG("file: ` is not a zfile object", file);
        return 0;
    }
    if (!pht->is_valid()) {
        LOG_ERRNO_RETURN(0, -1,
            "file: ` is a zfile object but verify digest failed.", file);
    }
    LOG_DEBUG("file: ` is a zfile object", file);
    return 1;
}

IFile *new_zfile_builder(IFile *file, const CompressArgs *args, bool ownership) {
    ZFileBuilderBase *builder;
    if (args->workers == 1) {
        builder = new ZFileBuilder(file, args, ownership);
    } else {
        builder = new ZFileBuilderMP(file, args, ownership);
    }
    if (builder->init() != 0) {
        delete builder;
        LOG_ERRNO_RETURN(0, nullptr, "init zfileStreamWriter failed.");
    }
    return builder;
}
} // namespace ZFile
