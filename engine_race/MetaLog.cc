#include "MetaLog.h"
#include "util.h"
#include <cstdlib>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>

namespace polar_race {

    MetaLog::MetaLog():_offset(0),_fd(-1), _firstRead(true), _table(32770) {

    }

    MetaLog::~MetaLog() {
        if (_fd > 0) {
            close(_fd);
        }
    }

    RetCode MetaLog::load() {
        // 插入skiplist准备读取
        for (int i = 0; i < _offset; ++i) {
            Location location{};
            if (pread(_fd, &location, 12, i*12) < 0) {
                perror("read meta file failed");
                return kIOError;
            }
//            _table[location.key] = location.addr;
            _table.addOrUpdate(location.key, location.addr);
        }
        return kSucc;
    }

    RetCode MetaLog::init(const std::string &dir, int index) {
        std::string filename = dir + "/meta_" + std::to_string(index);
        if (FileExists(filename)) {
            //恢复
            struct stat fileInfo{};
            if (stat(filename.c_str(), &fileInfo) < 0) {
                perror(("get size failed" + filename).c_str());
                return kIOError;
            }
            _offset = fileInfo.st_size / 12;
            _fd = open(filename.c_str(), O_RDWR);
            if (_fd < 0) {
                perror(("recover file " + filename + " failed\n").c_str());
                return kIOError;
            }

            RetCode retCode = load();
            _firstRead = false;
            std::cout <<"init meta:" + std::to_string(index) + " table size:" + std::to_string( _table.size()) + "\n";
            return retCode;
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

    RetCode MetaLog::append(const Location &location) {
        if (pwrite(_fd, &location, 12, (__off_t)_offset * 12) < 0) {
            return kIOError;
        }
        _offset++;
        return kSucc;
    }

    RetCode MetaLog::find(Location &location) {
        RetCode retCode;
//        bool firstRead = true;
//        _firstRead.compare_exchange_strong(firstRead, false);
//        if (firstRead) {
//            load();
//        }
        if (_firstRead || _table.size() == 0) {
            exit(0);
        }

        retCode = _table.find(location.key, location.addr);

        return retCode;
    }
}
