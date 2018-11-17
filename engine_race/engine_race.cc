// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include <pthread.h>

#define THREAD_NUM 64

namespace polar_race {

    uint64_t chang2Uint(const PolarString &key) {
        union Str2Uint data;
        size_t size = key.size();
        for (size_t i = 0; i < 8; ++i)
        {
            if (i<size)
            {
                data.data[i] = key[i];
            }else{
                data.data[i] = 0;
            }
        }
        return data.key;
    }

    int getIndex(const PolarString &key) {
        if (key.size() > 1) {
            return (key[0] << 2) + ( key[1] >> 6 );
        } else {
            return key[0] << 2;
        }
    }

    RetCode Engine::Open(const std::string& name, Engine** eptr) {
        return EngineRace::Open(name, eptr);
    }

    Engine::~Engine() {
    }

    /*
     * Complete the functions below to implement you own engine
     */

    struct ThreadInfo{
        EngineRace *engineRace;
        uint8_t id;
    };

    void * initThread(void *arg) {
        ThreadInfo info = ((ThreadInfo *)arg)[0];
        for (int i = 0; i < 1024/THREAD_NUM; ++i) {
            uint32_t index = (uint32_t)info.id*THREAD_NUM + i;
            info.engineRace->partition[index].valueLog.init(info.engineRace->_dir, index);
            info.engineRace->partition[index].metaLog.init(info.engineRace->_dir, index);
        }
    }

    // 1. Open engine
    RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
        *eptr = nullptr;
        EngineRace *engine_race = new EngineRace(name);
        // 1. 读取最新metalog文件建立table
        // 2. 恢复valuelog文件offset

        pthread_t a_thread[THREAD_NUM];
        ThreadInfo info[THREAD_NUM];
        int res;
        for (uint8_t i = 0; i < THREAD_NUM; ++i) {
            info[i].engineRace = engine_race;
            info[i].id = i;
            res = pthread_create(&a_thread[i], nullptr, initThread, &info[i]);
            if (res != 0) {
                std::cout << "fail to create thread" << std::endl;
                return kIncomplete;
            }
        }

        for (pthread_t j : a_thread) {
            res = pthread_join(j, nullptr);
            if (res != 0) {
                std::cout << "fail to join thread" << std::endl;
                return kIncomplete;
            }
        }

        *eptr = engine_race;
        return kSucc;
    }

    // 2. Close engine
    EngineRace::~EngineRace() {
    }

    // 3. Write a key-value pair into engine
    /**
     * 1. 获取到对应的partition
     * 2. append写vlog， 获取vlog的偏移地址
     * 3. append写metalog
     * @param key
     * @param value
     * @return RetCode
     */
    RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
        Location location{};
        RetCode retCode;
        int index;

        // 1
        location.key = chang2Uint(key);
        index = getIndex(key);
        Partition & part = partition[index];

        // 2
        retCode = part.valueLog.append(value, location.addr);
        if (retCode != kSucc) {
            return retCode;
        }

        // 3
        retCode = part.metaLog.append(location);

        return retCode;
    }

    // 4. Read value of a key
    /**
     * 1. 获取到对应的partition
     * 2. skiplist查询
     * 3. 从ValueLog中读取
     * @param key
     * @param value
     * @return
     */
    RetCode EngineRace::Read(const PolarString& key, std::string* value) {
        Location location{};
        RetCode retCode;
        int index;

        // 1
        location.key = chang2Uint(key);
        index = getIndex(key);
        Partition & part = partition[index];

        // 2
        retCode = part.metaLog.find(location);
        if (retCode != kSucc) {
            return retCode;
        }

        // 3
        value->clear();
        retCode = part.valueLog.read(location.addr, value);

        return retCode;
    }

    /*
     * NOTICE: Implement 'Range' in quarter-final,
     *         you can skip it in preliminary.
     */
    // 5. Applies the given Vistor::Visit function to the result
    // of every key-value pair in the key range [first, last),
    // in order
    // lower=="" is treated as a key before all keys in the database.
    // upper=="" is treated as a key after all keys in the database.
    // Therefore the following call will traverse the entire database:
    //   Range("", "", visitor)
    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
        Visitor &visitor) {
        return kSucc;
    }

}  // namespace polar_race
