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

    MetaLog::MetaLog() : _offset(0), _fd(-1), _firstRead(true), _loading(false), _table(nullptr), tmp_key(0), tmp_addr(0){

    }

    MetaLog::~MetaLog() {
        if (_table != nullptr) {
            munmap(_table, MMAP_SIZE);
            _table = nullptr;
        }
        if (_fd > 0) {
            close(_fd);
        }
    }

    void MetaLog::readAhread() {
//        readahead(_fd, 0, _offset<<4);
    }

    RetCode MetaLog::load() {
        _table = static_cast<Location *>(mmap(NULL, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0));
//        if (_offset == 62923 || _offset == 62731) {
//            print();
//        }
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
//            _fd = open(filename.c_str(), O_RDWR | O_ASYNC);
            _fd = open(filename.c_str(), O_RDWR);
            if (_fd < 0) {
                perror(("recover file " + filename + " failed\n").c_str());
                return kIOError;
            }

//            posix_fallocate(_fd, 0, MMAP_SIZE);
//            _table = static_cast<Location *>(mmap(NULL, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0));
        } else {
//            _fd = open(filename.c_str(), O_RDWR | O_CREAT | O_ASYNC, 0644);
            _fd = open(filename.c_str(), O_RDWR | O_CREAT, 0644);
            if (_fd < 0) {
                perror(("open file " + filename + " failed\n").c_str());
                return kIOError;
            }
            posix_fallocate(_fd, 0, MMAP_SIZE);
            _table = static_cast<Location *>(mmap(NULL, MMAP_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0));
        }
        _offset = offset;
        return kSucc;
    }

    RetCode MetaLog::append(const Location &location) {
        _table[_offset.fetch_add(1)] = location;
        return kSucc;
    }

    RetCode MetaLog::find(Location &location) {
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
    Location *MetaLog::findAll() {
        bool loading = false;
        _loading.compare_exchange_strong(loading, true);
        if (!loading) {
            if (_firstRead) {
                load();
                _firstRead = false;
            }
//            _loading = false;
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

    void MetaLog::print() {
        std::string out;
        out+="size:"+std::to_string(_offset)+"\n";
        for (int i = 0; i < _offset; i++) {
            out+= "key:" + std::to_string(_table[i].key) + " addr:"+ std::to_string(_table[i].addr) + "\n";
        }
        out+="print over\n\n";
        std::cout << out;
    }

}
