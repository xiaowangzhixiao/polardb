#include "MetaLog.h"
#include "util.h"
#include <cstdlib>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>

#define MMAP_SIZE 63252*16
//#define MMAP_SIZE 50*16
namespace polar_race {

    MetaLog::MetaLog():_offset(0),_fd(-1), _firstRead(true),_loading(false),_table(nullptr),test_num(0) {

    }

    MetaLog::~MetaLog() {
        if (_fd > 0) {
            close(_fd);
        }
        if (_table != nullptr) {
//            free(_table);
//            _table = nullptr;
            munmap(_table, MMAP_SIZE);
            _table = nullptr;
        }
    }

    void MetaLog::readAhread() {
//        readahead(_fd, 0, _offset<<4);
    }

    RetCode MetaLog::load() {
        // 插入skiplist准备读取
//        _table = static_cast<Location *>(malloc(_offset << 4 ));
//        pread(_fd, _table, _offset<<4, 0);
        merge_sort(_table, _offset);
        if(test_num.fetch_add(1)<5) {
            std::cout << "size:"+std::to_string(_offset)+"\n";
            for (int i = 0; i < _offset ; ++i) {
                std::cout << "key:"+std::to_string(_table[i].key) +" addr:"+std::to_string(_table[i].addr)+"\n";
            }
            std::cout <<"\n";
        }
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
//            _fd = open(filename.c_str(), O_RDWR | O_ASYNC);
            _fd = open(filename.c_str(), O_RDWR);
            if (_fd < 0) {
                perror(("recover file " + filename + " failed\n").c_str());
                return kIOError;
            }
        } else {
//            _fd = open(filename.c_str(), O_RDWR | O_CREAT | O_ASYNC, 0644);
            _fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
            if (_fd < 0) {
                perror(("open file " + filename + " failed\n").c_str());
                return kIOError;
            }
        }
        _offset = offset;
        posix_fallocate(_fd, 0, MMAP_SIZE);
        _table = static_cast<Location *>(mmap(NULL, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0));
        return kSucc;
    }

    RetCode MetaLog::append(const Location &location) {
        _table[_offset.fetch_add(1)] = location;
//        if (pwrite(_fd, &location, 16, (__off_t)_offset.fetch_add(1) << 4) < 0) {
//            return kIOError;
//        }
        return kSucc;
    }

    RetCode MetaLog::find(Location &location) {
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
