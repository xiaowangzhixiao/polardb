#include "ValueLog.h"
#include "util.h"
#include <cstdlib>
#include <sys/stat.h>
#include <fcntl.h>

namespace polar_race {

    ValueLog::ValueLog():_offset(0),_fd(-1) {

    }

    ValueLog::~ValueLog() {
        if (_fd > 0) {
            close(_fd);
        }
    }


    RetCode ValueLog::init(const std::string &dir, int index) {
        // 创建或恢复文件
        std::string filename = dir + "/value_" + std::to_string(index);
        if (FileExists(filename)) {
            //恢复
            struct stat fileInfo;
            if (stat(filename.c_str(), &fileInfo) < 0) {
                perror(("get size failed" + filename).c_str());
                return kIOError;
            }
            _offset = fileInfo.st_size / 4096;
            _fd = open(filename.c_str(), O_RDWR);
            if (_fd < 0) {
                perror(("recover file " + filename + " failed\n").c_str());
                return kIOError;
            }
        } else {
            _fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
            if (_fd < 0) {
                perror(("open file " + filename + " failed\n").c_str());
                return kIOError;
            }
            _offset = 0;
        }

        return kSucc;
    }

    RetCode polar_race::ValueLog::append(const polar_race::PolarString &value, uint32_t &addr) {
        addr = _offset.fetch_add(1);
        if ( pwrite(_fd, value.data(), 4096, ((__off_t)addr)*4096) < 0 ) {
            return kIOError;
        }
        return kSucc;
    }

    RetCode ValueLog::read(const uint32_t &addr, std::string *value) {
        char buffer[4096];
        if (addr > _offset) {
            return kInvalidArgument;
        }
        if (pread(_fd, buffer, 4096, ((__off_t)addr) * 4096) < 0) {
            perror("read file error");
            return kIOError;
        }

        *value = std::string(buffer, 4096);

        return kInvalidArgument;
    }
}
