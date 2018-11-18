#ifndef POLARDB_META_LOG_H
#define POLARDB_META_LOG_H

#include <atomic>
#include <unistd.h>
#include <include/engine.h>
#include "HashTable.h"

namespace polar_race {

    struct Location {
        uint64_t key;
        uint32_t addr;
    };

    class MetaLog {
    public:
        explicit MetaLog();
        ~MetaLog();
        RetCode init(const std::string &dir, int index);
        RetCode append(const Location &);
        RetCode find(Location &);
    private:

        RetCode load();

        std::atomic_uint_least32_t _offset;
        int _fd;
        std::atomic_bool _firstRead;
        std::atomic_bool _loading;
        HashTable<uint64_t, uint32_t> _table;
    };

}
#endif
