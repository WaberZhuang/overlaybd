#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zlib.h>
#include <list>
#include <sys/fcntl.h>
#include "gzfile.h"
#include "photon/common/alog.h"
#include "photon/common/alog-stdstring.h"
#include "photon/fs/localfs.h"
#include "photon/photon.h"


static void add_index_entry(int bits, off_t en_pos, off_t de_pos,
		unsigned int left, unsigned char *window, std::list<struct IndexEntry *> &index) {
	struct IndexEntry* p = new  IndexEntry;
    p->bits = bits;
    p->en_pos = en_pos;
    p->de_pos = de_pos;
    if (left) {
        memcpy(p->window, window + WINSIZE - left, left);
    }
    if (left < WINSIZE) {
        memcpy(p->window + left, window, WINSIZE - left);
    }
    index.push_back(p);
}

static int build_index(int32_t chunk_size, photon::fs::IFile *gzfile, off_t span, std::list<struct IndexEntry *> &index)
{
    int ret;
    off_t totin, totout;
    off_t last;
    z_stream strm;

    unsigned char *input = new unsigned char[chunk_size];
    DEFER(delete []input);
    unsigned char window[WINSIZE];

    strm.zalloc = nullptr;
    strm.zfree = nullptr;
    strm.opaque = nullptr;
    strm.avail_in = 0;
    strm.next_in = nullptr;
    ret = inflateInit2(&strm, 47);
    if (ret != Z_OK) {
        return ret;
    }
    DEFER(inflateEnd(&strm));

    totin = totout = last = 0;
    strm.avail_out = 0;
    do {
    	ssize_t read_cnt = gzfile->read(input, chunk_size);//TODO 文件尾和error如何区分？
    	if (read_cnt < 0){
    		LOG_ERRNO_RETURN(Z_ERRNO, -1, "Failed to gzfile->read");
    	}
    	strm.avail_in = read_cnt;
        if (strm.avail_in == 0) {
        	LOG_ERRNO_RETURN(Z_DATA_ERROR, -1, "Failed to read data");
        }
        strm.next_in = input;

        do {
            if (strm.avail_out == 0) {
                strm.avail_out = WINSIZE;
                strm.next_out = window;
            }

            totin += strm.avail_in;
            totout += strm.avail_out;
            ret = inflate(&strm, Z_BLOCK);
            totin -= strm.avail_in;
            totout -= strm.avail_out;
            if (ret == Z_NEED_DICT) {
                ret = Z_DATA_ERROR;
            }
            if (ret == Z_MEM_ERROR || ret == Z_DATA_ERROR) {
            	LOG_ERRNO_RETURN(ret, -1, "Failed to inflate");
            }
            if (ret == Z_STREAM_END) {
                break;
            }

            if ((strm.data_type & 128) && !(strm.data_type & 64) &&
                (totout == 0 || totout - last > span)) {
            	add_index_entry(strm.data_type & 7, totin, totout, strm.avail_out, window, index);
                last = totout;
            }
        } while (strm.avail_in != 0);
    } while (ret != Z_STREAM_END);
    return 0;
}

static int save_index_to_file(const IndexFileHeader &header, const std::list<struct IndexEntry *>& index, std::string dest_path) {
	//TODO 需要增加O_TRUNC标记吗？truncate错了咋办？
	photon::fs::IFile *f = photon::fs::open_localfile_adaptor(dest_path.c_str(), O_RDWR | O_CREAT, 0644);
	if (f == nullptr) {
		LOG_ERROR_RETURN(0, -1, "Failed to open(`)", dest_path);
	}
	f->lseek(0, SEEK_SET);
	if (f->write(&header, sizeof(header)) != sizeof(header)) {
		LOG_ERROR_RETURN(0, -1, "Failed to write header");
	}
	for (auto i : index) {
		if (f->write(i, sizeof(IndexEntry)) != sizeof(IndexEntry)) {
			LOG_ERROR_RETURN(0, -1, "Failed to write index");
		}
	}
	return 0;
}

int create_gz_index(photon::fs::IFile* gzip_file, off_t span, const char *index_file_path) {
	if (span < 100) {
		LOG_ERRNO_RETURN(0, -1, "Span is too small, must be greater than 100, span:`", span);
	}

	struct stat sbuf;
	if (gzip_file->fstat(&sbuf) != 0) {
		LOG_ERRNO_RETURN(0, -1, "Faild to gzip_file->fstat()");
	}

    std::list<struct IndexEntry *> index;
    int ret = build_index(16384, gzip_file, span, index);
    if (ret != 0) {
    	LOG_ERRNO_RETURN(0, -1, "Faild to build_index");
    }

    IndexFileHeader h;
    memset(&h, 0, sizeof(h));
    strncpy(h.magic, "ddgzidx", sizeof(h.magic));
    h.index_size = sizeof(struct IndexEntry);
    h.index_num = index.size();
    h.span = span;
    h.version =1;
    h.window= WINSIZE;
    h.gzip_size= sbuf.st_size;
    h.crc = h.cal_crc();

	ret = save_index_to_file(h, index, index_file_path);
    if (ret != 0) {
    	LOG_ERRNO_RETURN(0, -1, "Failed to save_index_to_file(...)");
    }
    return 0;
}
