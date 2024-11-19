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

#pragma once

#include <photon/fs/filesystem.h>
#include <photon/fs/fiemap.h>
#include <photon/fs/virtual-file.h>
#include <photon/common/alog.h>
#include <vector>
#include "erofs_common.h"
#include "erofs/internal.h"

class ErofsFileSystem: public photon::fs::IFileSystem {
public:
	struct erofs_sb_info sbi;
	struct liberofs_file target_file;

	ErofsFileSystem(photon::fs::IFile *imgfile, uint64_t blksize);
	~ErofsFileSystem();
	photon::fs::IFile* open(const char *pathname, int flags);
	photon::fs::IFile* open(const char *pathname, int flags, mode_t mode);
	photon::fs::IFile* creat(const char *pathname, mode_t mode);
	int mkdir(const char *pathname, mode_t mode);
	int rmdir(const char *pathname);
	int symlink(const char *oldname, const char *newname);
	ssize_t readlink(const char *path, char *buf, size_t bufsiz);
	int link(const char *oldname, const char *newname);
	int rename(const char *oldname, const char *newname);
	int unlink(const char *filename);
	int chmod(const char *pathname, mode_t mode);
	int chown(const char *pathname, uid_t owner, gid_t group);
	int lchown(const char *pathname, uid_t owner, gid_t group);
	int statfs(const char *path, struct statfs *buf);
	int statvfs(const char *path, struct statvfs *buf);
	int stat(const char *path, struct stat *buf);
	int lstat(const char *path, struct stat *buf);
	int access(const char *pathname, int mode);
	int truncate(const char *path, off_t length);
	int utime(const char *path, const struct utimbuf *file_times);
	int utimes(const char *path, const struct timeval times[2]);
	int lutimes(const char *path, const struct timeval times[2]);
	int mknod(const char *path, mode_t mode, dev_t dev);
	int syncfs();
	photon::fs::DIR* opendir(const char *name);
};

class ErofsFile: public photon::fs::VirtualReadOnlyFile {
public:
	ErofsFileSystem *fs;
	struct erofs_inode inode;

	ErofsFile(ErofsFileSystem *fs);
	photon::fs::IFileSystem *filesystem();
	int fstat(struct stat *buf);
	int fiemap(struct photon::fs::fiemap *map);
};

class ErofsDir: public photon::fs::DIR {
public:
	std::vector<::dirent> m_dirs;
	::dirent *direntp = nullptr;
	long loc;
	ErofsDir(std::vector<::dirent> &dirs);
	~ErofsDir();
	int closedir();
	dirent *get();
	int next();
	void rewinddir();
	void seekdir(long loc);
	long telldir();
};

bool erofs_check_fs(const photon::fs::IFile *imgfile);
photon::fs::IFileSystem *erofs_create_fs(photon::fs::IFile *imgfile, uint64_t blksz);
