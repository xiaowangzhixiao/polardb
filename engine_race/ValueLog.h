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
        int init(const std::string &dir,int index);
        RetCode append(const PolarString &value, uint32_t &addr);
        RetCode read(const uint32_t &addr, std::string *value);
        char* findAll();
        void clear();
    private:
        std::atomic_bool _firstRead;
        std::atomic_bool _loading;
        int _fd; // 文件描述符
        std::atomic_uint_least32_t  _offset;
        char* _val;
        std::mutex mut;
    };

}
#endif
