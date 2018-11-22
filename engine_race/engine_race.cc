// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "util.h"
#include <sys/stat.h>
#include <pthread.h>
#include <iostream>
#include <cstdio>

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

    uint16_t getIndex(const PolarString &key) {
        if (key.size() > 1) {
            return ((((uint16_t)key[0]) << 2) | (((uint16_t)key[1] >> 6) & 0x3)) & 0x03ff;
        } else {
            return (((uint16_t)key[0]) << 2) & 0x03ff;
        }
    }

    int getIndex(const uint64_t &key) {
        return static_cast<int>(key & 0x03FF);
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
        for (int i = 0; i < BUCKET_NUM/THREAD_NUM; ++i) {
            uint32_t index = (uint32_t)info.id*(BUCKET_NUM/THREAD_NUM) + i;
            info.engineRace->partition[index].valueLog.init(info.engineRace->_dir, index);
            info.engineRace->partition[index].metaLog.init(info.engineRace->_dir, index);
        }
    }

    // 1. Open engine
    RetCode EngineRace::Open(const std::string& name, Engine** eptr) {
        *eptr = nullptr;
        EngineRace *engine_race = new EngineRace(name);

        if (!FileExists(name)
            && 0 != mkdir(name.c_str(), 0755)) {
            return kIOError;
        }
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

        for (uint8_t i = 0; i < THREAD_NUM; ++i) {
            res = pthread_join(a_thread[i], nullptr);
            if (res != 0) {
                std::cout << "fail to join thread" << std::endl;
                return kIncomplete;
            }
        }

//        for (int i = 0; i < BUCKET_NUM; ++i) {
//            engine_race->partition[i].valueLog.init(engine_race->_dir, i);
//            engine_race->partition[i].metaLog.init(engine_race->_dir, i);
//        }

        std::cout << "open success" << std::endl;

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
        uint16_t index;

        // 1
        location.key = chang2Uint(key);
        index = getIndex(key);
        Partition & part = partition[index];

        // 2
        retCode = part.valueLog.append(value, location.addr);
        if (retCode != kSucc) {
            return retCode;
        }
//        std::cout << "write index:" << index << " addr:" << location.addr << std::endl;
        if (location.addr % 1000 == 0) {
            std::cout << "write index:" << index << " addr:" << location.addr << std::endl;
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
        uint16_t index;

        // 1
        location.key = chang2Uint(key);
        index = getIndex(key);
        Partition & part = partition[index];



        // 2
        retCode = part.metaLog.find(location);
        if (retCode != kSucc) {
            std::cout << " not found\n";
            return retCode;
        }

        std::cout << "read index:" + std::to_string(index) + " addr:" + std::to_string(location.addr) + "\n";
//        if (location.addr % 1000 == 0) {
//            std::cout << "write index:" << index << " addr:" << location.addr << std::endl;
//        }
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
