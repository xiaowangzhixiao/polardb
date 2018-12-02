// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_
#include <string>
#include "include/engine.h"
#include <cstdlib>
#include "partiton.h"

#define BUCKET_NUM 1024

namespace polar_race {

    union Str2Uint {
        char data[8];
        uint64_t key;
    };

    class EngineRace : public Engine  {
    public:
        static RetCode Open(const std::string& name, Engine** eptr);

        explicit EngineRace(const std::string& dir):_dir(dir),_waiting(true),_container(0),_range_count(0){

        }

        ~EngineRace();

        RetCode Write(const PolarString& key,
          const PolarString& value) override;

        RetCode Read(const PolarString& key,
          std::string* value) override;

        /*
        * NOTICE: Implement 'Range' in quarter-final,
        *         you can skip it in preliminary.
        */
        RetCode Range(const PolarString& lower,
          const PolarString& upper,
          Visitor &visitor) override;

        Partition partition[BUCKET_NUM];
        std::string _dir;
        std::atomic_bool _waiting;
        std::atomic_int _container;
        std::atomic_int _range_count;
    };

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
