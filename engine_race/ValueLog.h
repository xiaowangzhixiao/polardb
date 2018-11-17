#ifndef POLARDB_VALUE_LOG_H
#define POLARDB_VALUE_LOG_H

#include <atomic>
#include <unistd.h>
#include "include/engine.h"

namespace polar_race {

    class ValueLog {
    public:
        explicit ValueLog();
        ~ValueLog();
        RetCode init(const std::string &dir,int index);
        RetCode append(const PolarString &value, uint32_t &addr);
        RetCode read(const uint32_t &addr, std::string *value);

    private:
        std::atomic_uint_least32_t  _offset;

        int _fd; // 文件描述符
    };

}
#endif
