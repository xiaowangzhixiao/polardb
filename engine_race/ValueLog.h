#ifndef POLARDB_VALUE_LOG_H
#define POLARDB_VALUE_LOG_H

#include <atomic>
#include <unistd.h>
#include "include/engine.h"
#include <mutex>

namespace polar_race {

    class ValueLog {
    public:
        explicit ValueLog();
        ~ValueLog();
        RetCode init(const std::string &dir,int index);
        RetCode append(const PolarString &value, uint32_t &addr);
        RetCode read(const uint32_t &addr, std::string *value);
        char* findAll();
        void clear();
    private:
        std::atomic_uint_least32_t  _offset;
        std::atomic_bool _firstRead;
        std::atomic_bool _loading;
        char* _val;
        int _fd; // 文件描述符
//        int _cache_fd;  //用于缓存的文件
//        std::atomic_int_fast8_t _cache_offset;
        std::mutex mut;
    };

}
#endif
