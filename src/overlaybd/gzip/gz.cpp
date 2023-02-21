#include "gz.h"
#include <zlib.h>
#include <photon/common/alog.h>
#include <photon/fs/filesystem.h>
#include <photon/fs/virtual-file.h>


class GzAdaptorFile: public photon::fs::VirtualReadOnlyFile {
public:
    GzAdaptorFile(gzFile gzf): m_gzf(gzf) {}
    ~GzAdaptorFile() {
        gzclose(m_gzf);
    }
    virtual photon::fs::IFileSystem *filesystem() override {
        return nullptr;
    }
    off_t lseek(off_t offset, int whence) override {
        return gzseek(m_gzf, offset, whence);
    }
    ssize_t read(void *buf, size_t count) override {
        return gzread(m_gzf, buf, count);
    }
    int fstat(struct stat *buf) override {
        return 0;
    }
private:
    gzFile m_gzf;
    char m_buf[1024*1024];
    int m_cur = 0, m_left = 0;
    int load_data() {
        auto rc = gzread(m_gzf, m_buf, 1024*1024);
        if (rc < 0) {
            LOG_ERRNO_RETURN(0, -1, "failed to gzread");
        }
        m_cur = 0;
        m_left = rc;
        LOG_INFO(VALUE(rc));
        return rc;
    }
};

photon::fs::IFile* open_gzfile_adaptor(const char *path) {
    gzFile gzf = gzopen(path, "r");
    if (gzf == nullptr) 
        LOG_ERRNO_RETURN(0, nullptr, "failed to open gzip file ", VALUE(path));
    return new GzAdaptorFile(gzf);
}