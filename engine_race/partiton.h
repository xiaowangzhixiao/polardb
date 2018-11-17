#ifndef POLARDB_PARTITION_H
#define POLARDB_PARTITION_H

#include <cstdlib>
#include <string>
#include <atomic>
#include "SkipList.h"
#include "ValueLog.h"
#include "MetaLog.h"

namespace polar_race {

    struct Partition {
        ValueLog valueLog;
        MetaLog metaLog;
    };

}


#endif
