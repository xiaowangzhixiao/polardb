#ifndef POLARDB_VALUE_LOG_H
#define POLARDB_VALUE_LOG_H

#include <atomic>
#include <unistd.h>
#include "include/engine.h"
#include <mutex>

#define SWITCH_NUM 4
#define VALUE_SIZE 4096
#define SWITCH_SIZE 16384
namespace polar_race {

    class ValueLog {
    public:
        explicit ValueLog();
        ~ValueLog();
        RetCode init(const std::string &dir,int index, int real_size);
        RetCode append(const PolarString &value, uint32_t &addr);
        RetCode read(const uint32_t &addr, std::string *value);
        RetCode preRead();
        char* findAll();
        void clear();
    private:
        std::atomic_bool _firstRead;
        std::atomic_bool _loading;
        int _fd; // 文件描述符
        std::atomic_uint_least32_t  _offset;
        char* _val;
        int _cache_fd;  //用于缓存的文件
        std::atomic_uint_least32_t _cache_offset;
        char* _tmp_val;
        std::mutex mut;
    };

}
#endif
