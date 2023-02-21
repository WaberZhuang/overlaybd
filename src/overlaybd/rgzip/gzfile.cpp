#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <list>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <algorithm>
#include "gzfile.h"
#include <photon/common/alog.h>
#include <photon/common/alog-stdstring.h>
#include <photon/fs/localfs.h>
#include <photon/photon.h>
#include <photon/fs/virtual-file.h>
#include <photon/common/checksum/crc32c.h>
#define CHUNK 16384

namespace FileSystem {
using namespace photon::fs;

class GzFile : public VirtualReadOnlyFile {
public:
	GzFile() = delete;
    explicit GzFile(photon::fs::IFile* gzip_file, photon::fs::IFile* index);
    virtual ~GzFile(){};

public:
    virtual ssize_t pread(void *buf, size_t count, off_t offset) override;
    virtual int fstat(struct stat *buf) override { return gzip_file_->fstat(buf);};
    virtual IFileSystem* filesystem() { return NULL; };
    virtual int close() override {return 0;};
    virtual ssize_t read(void *buf, size_t count) override;
    virtual off_t lseek(off_t offset, int whence) override;

    UNIMPLEMENTED(ssize_t write(const void *buf, size_t count));
    UNIMPLEMENTED(ssize_t writev(const struct iovec *iov, int iovcnt));

    UNIMPLEMENTED(ssize_t readv(const struct iovec *iov, int iovcnt));


private:
    IFile *gzip_file_ = nullptr;
    IFile *index_file_ = nullptr;
    ssize_t file_size_=0;
    struct IndexFileHeader index_header_;
    INDEX index_;
    bool inited_ = false;
    int init();
    IndexEntry *seek_index(const INDEX &index, off_t offset);
    IndexEntry *seek_index2(INDEX &index, off_t offset);
    off_t read_p = 0;
};

GzFile::GzFile(photon::fs::IFile* gzip_file, photon::fs::IFile* index) {
	gzip_file_ = gzip_file;
	index_file_ = index;
}

int GzFile::init() {
	if (inited_) {
		return 0;
	}

	//从索引文件读取索引header
	if (index_file_->pread(&index_header_, sizeof(index_header_), 0) != sizeof(index_header_)) {
		LOG_ERRNO_RETURN(0, -1, "Failed to index_file_->pread");
	}

	if (index_header_.cal_crc() != index_header_.crc) {
		LOG_ERRNO_RETURN(0, -1, "Faild to check CRC of index_header");
	}

	if (sizeof(IndexEntry) != index_header_.index_size) {
		int32_t idx_size_tmp = index_header_.index_size;
		LOG_ERRNO_RETURN(0, -1, "Failed check index_header_.index_size. ` != `", sizeof(IndexEntry), idx_size_tmp);
	}
	if (strncmp(GZFILE_INDEX_MAGIC, index_header_.magic, strlen(GZFILE_INDEX_MAGIC)) != 0) {
		LOG_ERRNO_RETURN(0, -1, "Wrong magic ` != `", GZFILE_INDEX_MAGIC, index_header_.magic);
	}
	struct stat stat_buf;
	if (gzip_file_->fstat(&stat_buf) != 0) {
		LOG_ERRNO_RETURN(0, -1, "Wrong magic ` != `", GZFILE_INDEX_MAGIC, index_header_.magic);
	}
	if (index_header_.gzip_size != stat_buf.st_size) {
		int64_t gzip_tmp = index_header_.gzip_size;
		int64_t st_size = stat_buf.st_size;
		LOG_ERRNO_RETURN(0, -1, "Failed check size of gzfile. ` != `", gzip_tmp, st_size);
	}

	//读取相应的index
	for (int64_t i=0; i<index_header_.index_num ; i++) {
		struct IndexEntry *p= new (struct IndexEntry);
		off_t offset = sizeof(IndexFileHeader) + i * sizeof(IndexEntry);
		if (index_file_->pread(p, sizeof(*p), offset) != sizeof(*p)) {
			LOG_ERRNO_RETURN(0, -1, "Failed to index_file_->pread, offset:`", offset);
		}
		index_.push_back(p);
	}

	inited_ = true;
	LOG_INFO("IndexFileHeader: span:`,window:`,index_size:`,index_num:`,gzip_size:`",
			index_header_.span+0, index_header_.window+0, index_header_.index_size+0,index_header_.index_num+0,index_header_.gzip_size+0);
	return 0;
}
static bool indx_compare(struct IndexEntry* i, struct IndexEntry* j) { return i->de_pos < j->de_pos; }
IndexEntry *GzFile::seek_index2(INDEX &index, off_t offset) {
    if (index.size() == 0) {
    	return nullptr;
    }
    struct IndexEntry tmp;
    tmp.de_pos = offset;
    INDEX::iterator iter = std::upper_bound(index.begin(), index.end(), &tmp, indx_compare);
    if (iter == index.end()) {
    	return index.at(index.size() - 1);
    }
    int idx = iter - index.begin();
    if (idx > 0) {
    	idx --;
    }
    return index.at(idx);
}

IndexEntry *GzFile::seek_index(const INDEX &index, off_t offset) {
    if (index.size() == 0) {
    	return nullptr;
    }
    int pos = 0;
    int cnt = index.size();
    while(--cnt && index.at(pos+1)->de_pos <= offset) {//TODO for performance
    	pos++;
    }
    return index.at(pos);
}
static ssize_t extract(photon::fs::IFile *gzip_file, const struct IndexEntry *found_idx, off_t offset,
                  unsigned char *buf, int len) {
    int ret;
    z_stream strm;
    unsigned char input[CHUNK];
    unsigned char discard[WINSIZE];

    strm.zalloc = Z_NULL;
    strm.zfree = Z_NULL;
    strm.opaque = Z_NULL;
    strm.avail_in = 0;
    strm.next_in = Z_NULL;
    ret = inflateInit2(&strm, -15);
    if (ret != Z_OK) {
    	LOG_ERRNO_RETURN(0, -1, "Fail to inflateInit2(&strm, -15)");
    }
    DEFER(inflateEnd(&strm));

    off_t start_pos = found_idx->en_pos - (found_idx->bits ? 1 : 0);
    // ret = gzip_file->lseek(start_pos, SEEK_SET);
    // if (ret == -1){
    // 	LOG_ERRNO_RETURN(0, -1, "Fail to gzip_file->lseek(`, SEEK_SET)", start_pos);
    // }
    if (found_idx->bits) {
    	unsigned char tmp;
    	if (gzip_file->pread(&tmp, 1, start_pos) != 1) {
    		LOG_ERRNO_RETURN(0, -1, "Fail to gzip_file->read");
    	}
        start_pos++;
    	ret = tmp;
    	inflatePrime(&strm, found_idx->bits, ret >> (8 - found_idx->bits));
    }
    inflateSetDictionary(&strm, found_idx->window, WINSIZE);

    offset -= found_idx->de_pos;
    strm.avail_in = 0;
    bool skip = true;
    do {
        if (offset == 0 && skip) {
            strm.avail_out = len;
            strm.next_out = buf;
            skip = false;
        }
        if (offset > WINSIZE) {
            strm.avail_out = WINSIZE;
            strm.next_out = discard;
            offset -= WINSIZE;
        } else if (offset != 0) {// last skip
            strm.avail_out = (unsigned)offset;
            strm.next_out = discard;
            offset = 0;
        }

        do {
            if (strm.avail_in == 0) {
                ssize_t read_cnt = gzip_file->pread(input, CHUNK, start_pos);
                if (read_cnt < 0 ) {
                	LOG_ERRNO_RETURN(0, -1, "Fail to gzip_file->read(input, CHUNK)");
                }
                if (read_cnt == 0) {
                	LOG_ERRNO_RETURN(Z_DATA_ERROR, -1, "Fail to gzip_file->read(input, CHUNK)");
                }
                start_pos += read_cnt;
                strm.avail_in = read_cnt;
                strm.next_in = input;
            }
            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_NEED_DICT) {
                ret = Z_DATA_ERROR;
            }
            if (ret == Z_MEM_ERROR || ret == Z_DATA_ERROR) {
            	LOG_ERRNO_RETURN(ret, -1, "Fail to gzip_file->read(input, CHUNK). ret:`", ret);
            }
            if (ret == Z_STREAM_END) {//reach the end of stream
                break;
            }
        } while (strm.avail_out != 0);
        if (ret == Z_STREAM_END) {
            break;
        }
    } while (skip);

    ret = skip ? 0 : len - strm.avail_out;
    //LOG_DEBUG("offset:`,len:`,ret:`", offset, len, ret);
    return ret;
}

ssize_t GzFile::pread(void *buf, size_t count, off_t offset) {
	if (!inited_) {
		if (init() != 0 ) {
			LOG_ERRNO_RETURN(0, -1, "Fail init()", offset);
		}
	}
	/*//NOTE can't get the file size of uncompressed file
    if (offset >= (off_t)file_size_) {
        return 0;
    }
    if (offset + count > (size_t)file_size_) {
        count = file_size_ - offset;
    }*/

    //TODO 第一块/最后一块数据需要字典吗？或者根本没有index时，该如何处理？
	struct IndexEntry * p = seek_index2(index_, offset);
	if (p == nullptr) {
		LOG_ERRNO_RETURN(0, -1, "Failed to seek_index(,`)", offset);
	}
	//LOG_DEBUG("offset:`, index->de_pos:", offset, p->de_pos+0);

	return extract(gzip_file_, p, offset, (unsigned char*)buf, count);
}


ssize_t GzFile::read(void *buf, size_t count) {
    auto rc = pread(buf, count, read_p);
    read_p += rc;
    return rc;
}

off_t GzFile::lseek(off_t offset, int whence) {
    if (whence == SEEK_CUR)
        read_p += offset;
    else if (whence == SEEK_SET)
        read_p = offset;
    return read_p;
}
} // namespace FileSystem


photon::fs::IFile* new_gzfile(photon::fs::IFile* gzip_file, photon::fs::IFile* index) {
	return new FileSystem::GzFile(gzip_file, index);
}


bool is_gzfile(photon::fs::IFile* file) {
    char buf[4] = {0};
    file->read(buf, 2);
    file->lseek(0, 0);
    return (uint8_t)buf[0] == 0x1f && (uint8_t)buf[1] == 0x8b;
    // return (static_cast<uint8_t>(buf[0]) == 0x1F && static_cast<uint8_t>(buf[1]) == 0x8B);
}
