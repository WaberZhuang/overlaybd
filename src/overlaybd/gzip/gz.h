#pragma once

#include <vector>
#include <photon/fs/filesystem.h>

photon::fs::IFile* open_gzfile_adaptor(const char *path);