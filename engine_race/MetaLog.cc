#include "MetaLog.h"
#include "util.h"
#include <cstdlib>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>

namespace polar_race {

    MetaLog::MetaLog():_offset(0),_fd(-1), _firstRead(true),_loading(false),_table(nullptr) {

    }

    MetaLog::~MetaLog() {
        if (_fd > 0) {
            close(_fd);
        }
        if (_table != nullptr) {
            free(_table);
            _table = nullptr;
        }
    }

    void MetaLog::readAhread() {
//        readahead(_fd, 0, _offset*16);
    }

    RetCode MetaLog::load() {
        // 插入skiplist准备读取
        _table = static_cast<Location *>(malloc(_offset * 16));
        pread(_fd, _table, _offset*16, 0);
        merge_sort(_table, _offset);
        return kSucc;
    }

    RetCode MetaLog::init(const std::string &dir, int index, int offset) {
        std::string filename = "";
        filename.append(dir).append("/meta_").append(std::to_string(index));
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
        if (pwrite(_fd, &location, 16, (__off_t)_offset.fetch_add(1) * 16) < 0) {
            return kIOError;
        }
        return kSucc;
    }

    RetCode MetaLog::find(Location &location) {
        int addr = binary_search(_table, _offset, location.key);
        if (addr == -1) {
            std::cout <<"key not found "+std::to_string(location.key)+"\n";
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
//                std::cout << "load data ..."<<_offset <<std::endl;
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