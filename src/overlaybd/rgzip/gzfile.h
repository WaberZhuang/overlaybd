#pragma once

#include <vector>
#include <photon/fs/filesystem.h>
#include <photon/common/checksum/crc32c.h>

#define WINSIZE 32768U

#define GZFILE_INDEX_MAGIC "ddgzidx"

struct IndexFileHeader {
	char magic[8];
	int32_t version;
	int32_t span;
	int32_t window;
	int32_t index_size;
	int64_t index_num;
	int64_t gzip_size; //size of gzipfile
	int32_t pads;
	uint32_t crc; //TODO
	uint32_t cal_crc() { return crc32c(this, sizeof(IndexFileHeader) - sizeof(crc));}
} __attribute__((packed));

struct IndexEntry {
    off_t de_pos;// decompressed num
    off_t en_pos;// compressed num
    int bits;
    unsigned char window[WINSIZE];
}__attribute__((packed));

typedef std::vector<struct IndexEntry *> INDEX;


photon::fs::IFile* new_gzfile(photon::fs::IFile* gzip_file, photon::fs::IFile* index);
int create_gz_index(photon::fs::IFile* gzip_file, off_t chunk_size, const char *index_file_path);
bool is_gzfile(photon::fs::IFile* file);
