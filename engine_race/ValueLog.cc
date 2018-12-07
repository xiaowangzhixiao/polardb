#include "ValueLog.h"
#include "util.h"
#include <cstdlib>
#include <sys/stat.h>
#include <fcntl.h>
#include <iostream>
#include <thread>

#define VALUE_SIZE 4096

namespace polar_race {

    ValueLog::ValueLog():_offset(0),_fd(-1),_firstRead(true),_loading(false),_val(nullptr) {

    }

    ValueLog::~ValueLog() {
        if (_fd > 0) {
            close(_fd);
        }
        if (_val != nullptr) {
            free(_val);
            _val = nullptr;
        }
    }


    int ValueLog::init(const std::string &dir, int index) {
        // 创建或恢复文件
        std::string filename = "";
        filename.append(dir).append("/value_").append(std::to_string(index));
        if (FileExists(filename)) {
            //恢复
            struct stat fileInfo;
            if (stat(filename.c_str(), &fileInfo) < 0) {
                perror(("get size failed" + filename).c_str());
                return kIOError;
            }
            _offset = fileInfo.st_size >> 12;
//            _fd = open(filename.c_str(), O_RDWR | O_ASYNC) ;
            _fd = open(filename.c_str(), O_RDWR ) ;
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
//            int ret = fallocate(_fd, FALLOC_FL_KEEP_SIZE, 0, 256000000);
//            if (ret < 0) {
//                perror("fallocate failed\n");
//                return kIOError;
//            }

            _offset = 0;
        }

        return _offset;
    }



    RetCode polar_race::ValueLog::append(const polar_race::PolarString &value, uint32_t &addr) {
        addr = _offset.fetch_add(1);
        if ( pwrite(_fd, value.data(), VALUE_SIZE, ((__off_t)addr) << 12) < 0 ) {
            return kIOError;
        }
        return kSucc;
    }

    RetCode ValueLog::read(const uint32_t &addr, std::string *value) {
        char buffer[VALUE_SIZE];
        if (addr > _offset) {
            return kInvalidArgument;
        }
        if (pread(_fd, buffer, VALUE_SIZE, ((__off_t)addr) << 12) < 0) {
            perror("read file error");
            return kIOError;
        }

        *value = std::string(buffer, VALUE_SIZE);

        return kSucc;
    }

    /**
    * 读取所有的val值
    * @return
    */
    char* ValueLog::findAll() {
        bool loading = false;
        _loading.compare_exchange_strong(loading, true);
        if (!loading) {
            if (_firstRead) {
                _val = static_cast<char *>(malloc(_offset << 12));
                pread(_fd, _val, _offset<<12, 0);
                _firstRead = false;
            }
            _loading = false;
        } else {
            while (_firstRead) {
                usleep(5);
            }
        }
        return _val;
    }

    // 需要原子操作
    void ValueLog::clear() {
        mut.lock();
        if (_val != nullptr) {
            std::cout << "clear" <<std::endl;
            free(_val);
            _val = nullptr;
        }
        _firstRead = true;
        _loading = false;
        mut.unlock();
    }

}
