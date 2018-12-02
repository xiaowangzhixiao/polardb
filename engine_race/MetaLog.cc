#include "MetaLog.h"
#include "util.h"
#include <cstdlib>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>

namespace polar_race {

    MetaLog::MetaLog():_offset(0),_fd(-1), _firstRead(true),_loading(false) {

    }

    MetaLog::~MetaLog() {
        if (_fd > 0) {
            close(_fd);
        }
    }

    RetCode MetaLog::load() {
        // 插入skiplist准备读取
        _table = static_cast<Location *>(malloc(_offset * 16));
        pread(_fd, _table, _offset*16, 0);
        merge_sort(_table, _offset);
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
            _offset = fileInfo.st_size / 16;
            _fd = open(filename.c_str(), O_RDWR);
            if (_fd < 0) {
                perror(("recover file " + filename + " failed\n").c_str());
                return kIOError;
            }

            RetCode retCode = load();
            _firstRead = false;
//            std::cout <<"init meta:" + std::to_string(index)  + "\n";
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
        if (pwrite(_fd, &location, 16, (__off_t)_offset * 16) < 0) {
            return kIOError;
        }
        _offset++;
        return kSucc;
    }

    RetCode MetaLog::find(Location &location) {
        RetCode retCode;
        bool loading = false;
        _loading.compare_exchange_strong(loading, true);
        if (!loading) {
            if (_firstRead){
                load();
                _firstRead = false;
            }
            _loading = false;
        } else {
            while (_firstRead) {
                usleep(5);
            }
        }

        int addr = binary_search(_table, _offset, location.key);
        if (addr == -1) {
            return kNotFound;
        }
        location.addr = addr;
        return kSucc;
    }

    /**
     * 读取的所有数据
     * @return
     */
    Location* MetaLog::findAll() {
        bool loading = false;
        _loading.compare_exchange_strong(loading, true);
        if (!loading) {
            if (_firstRead){
                load();
                _firstRead = false;
            }
            _loading = false;
        } else {
            while (_firstRead) {
                usleep(5);
            }
        }
        return _table;
    }

    int MetaLog::getSize() {
        return _offset;
    }

}
