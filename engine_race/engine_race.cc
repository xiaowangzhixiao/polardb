// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "util.h"
#include <sys/stat.h>
#include <iostream>
#include <cstdio>
#include <map>
#include <thread>
#include <byteswap.h>

namespace polar_race {

    uint64_t chang2Uint(const PolarString &key) {
        union Str2Uint data;
        size_t size = key.size();
        for (size_t i = 0; i < 8; ++i) {
            if (i < size) {
                data.data[i] = key[i];
            } else {
                data.data[i] = 0;
            }
        }
        return data.key;
    }

    uint16_t getIndex(const PolarString &key) {
        if (key.size() > 1) {
            return ((((uint16_t) key[0]) << 2) | (((uint16_t) key[1] >> 6) & 0x3)) & 0x03ff;
        } else {
            return (((uint16_t) key[0]) << 2) & 0x03ff;
        }
    }

    int getIndex(const uint64_t &key) {
        return static_cast<int>(key & 0x03FF);
    }

    RetCode Engine::Open(const std::string &name, Engine **eptr) {
        return EngineRace::Open(name, eptr);
    }

    Engine::~Engine() {
    }

    void initThread(EngineRace *engineRace, int thread_id) {
        for (int i = 0; i < THREAD_CAP; ++i) {
            uint32_t index = (uint32_t) thread_id * THREAD_CAP + i;
            int offset = engineRace->partition[index].valueLog.init(engineRace->_dir, index); //初始化value
            engineRace->partition[index].metaLog.init(engineRace->_dir, index, offset);  //返回真正的size
        }
    }

    // 第一次读预读
    void preRead(EngineRace *engineRace, int thread_id) {
        for (int i = 0; i < THREAD_CAP; ++i) {
            uint32_t index = (uint32_t) thread_id * THREAD_CAP + i;
            if(index +1 < (thread_id +1) * THREAD_CAP ){
                engineRace->partition[index + 1].metaLog.readAhread();
            }
            engineRace->partition[index].metaLog.findAll();
        }
    }

    void preRange(EngineRace *engineRace, int thread_id) {
        for (int i = 0; i < THREAD_CAP; ++i) {
            uint32_t index = (uint32_t) thread_id * THREAD_CAP + i;
            if(index +1 < (thread_id +1) * THREAD_CAP ){
                engineRace->partition[index + 1].metaLog.readAhread();
            }
            engineRace->partition[index].valueLog.directOpen(engineRace->_dir, index);
            engineRace->partition[index].metaLog.findAll();
        }
    }

    void PreReadWithThread(EngineRace *engineRace, int shard_id) {
        engineRace->partition[shard_id - 2].valueLog.clear();
        engineRace->partition[shard_id].valueLog.findAll();
    }

    void prepareRange(EngineRace *engineRace) {
        int shard_id = 2;
        Partition* p = engineRace->partition;
        p[0].valueLog.findAll();
        p[1].valueLog.findAll();
        p[0].read = true;
        p[1].read = true;
        while (shard_id < 1024) {
            if (p[shard_id-2].shard_num == 0) {
                p[shard_id-2].valueLog.clear();
                p[shard_id].valueLog.findAll();
                p[shard_id++].read = true;
            } else {
                usleep(2);
            }
        }
    }

    // 1. Open engine
    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        *eptr = nullptr;
        auto openStart = std::chrono::high_resolution_clock::now();
        EngineRace *engine_race = new EngineRace(name);

        if (!FileExists(name)
            && 0 != mkdir(name.c_str(), 0755)) {
            return kIOError;
        }
        // 1. 读取最新metalog文件建立table
        // 2. 恢复valuelog文件offset

        std::vector<std::thread> initvec;
        for (uint8_t i = 0; i < THREAD_NUM; ++i) {
            initvec.emplace_back(std::thread(initThread, engine_race, i));
        }

        for (auto& th:initvec) {
            th.join();
        }

        auto openEnd = std::chrono::high_resolution_clock::now();
        std::cout << "Open takes: " +
                     std::to_string(std::chrono::duration<double, std::milli>(openEnd - openStart).count())
                     + " milliseconds" + "\n";

        *eptr = engine_race;
        return kSucc;
    }

    // 2. Close engine
    EngineRace::~EngineRace() {
        for (int i = 0; i < 1024; ++i) {
            partition[i].valueLog.clear();
        }
        std::cout << "engine close" << std::endl;
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
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        Location location{};
        RetCode retCode;
        uint16_t index;

        // 1
        location.key = bswap_64(chang2Uint(key));
        index = getIndex(key);
        Partition &part = partition[index];

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
    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        bool loading = false;
        _loading.compare_exchange_strong(loading, true);
        if (!loading) {
            if (_firstRead) {
                auto rreadStart = std::chrono::high_resolution_clock::now();
                std::vector<std::thread> initvec;
                for (uint8_t i = 0; i < THREAD_NUM; ++i) {
                    initvec.emplace_back(std::thread(preRead, this, i));
                }

                for (auto& th:initvec) {
                    th.join();
                }
                std::cout << "pre read over" <<std::endl;
                auto rreadEnd = std::chrono::high_resolution_clock::now();
                std::cout << "Random pre read takes: " +
                           std::to_string(std::chrono::duration<double, std::milli>(rreadEnd - rreadStart).count())
                          + " milliseconds" + "\n";
                {
                    std::unique_lock<std::mutex> lck(mtx);
                    _firstRead = false;
                    std::cout << "pread finish\n";
                    _finishReadCV.notify_all();
                }
            }
            _loading = false;
        } else {
            std::unique_lock <std::mutex> lck(mtx);
            while (_firstRead) {
                _finishReadCV.wait(lck);
                std::cout << "thread awake\n";
            }
        }

        Location location{};
        RetCode retCode;
        uint16_t index;
        // 1
        location.key = bswap_64(chang2Uint(key));
        index = getIndex(key);
        Partition &part = partition[index];

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
    RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                              Visitor &visitor) {

        bool loading = false;
        _loading.compare_exchange_strong(loading, true);
        if (!loading) {
            if (_firstRange) {
                auto rreadStart = std::chrono::high_resolution_clock::now();
                std::vector<std::thread> initvec;
                for (uint8_t i = 0; i < THREAD_NUM; ++i) {
                    initvec.emplace_back(std::thread(preRange, this, i));
                }

                for (auto& th:initvec) {
                    th.join();
                }
                auto rreadEnd = std::chrono::high_resolution_clock::now();
                std::cout << "Range pre read takes: " +
                             std::to_string(std::chrono::duration<double, std::milli>(rreadEnd - rreadStart).count())
                             + " milliseconds" + "\n";
                std::thread prepareth(prepareRange, this);
                prepareth.detach();
                _firstRange = false;
                {
                    std::unique_lock<std::mutex> lck(mtx);
                    _firstRange = false;
                    std::cout << "preRange finish\n";
                    _finishReadCV.notify_all();
                }
            }
        } else {
            std::unique_lock <std::mutex> lck(mtx);
            while (_firstRange) {
                _finishReadCV.wait(lck);
                std::cout << "thread awake\n";
            }
        }

        int thread_id = 0;
        if ((thread_id = _container.fetch_add(1)) < THREAD_NUM - 1) {
            // 开启多线程读
            int record = 0;
            while (_waiting) {
                if (record++ > 100) {
                    break;
                }
                usleep(2);
            }
        }
        _waiting = false;

        if (_container != 64) {
            for (int i = 0; i < BUCKET_NUM; ++i) {
                partition[i].shard_num = 1;
            }
        }
        // 2. 开始读
        prefetch(visitor, thread_id);

        int count = 0;
        while (_range_count <= THREAD_NUM - 1) {
            if (count++ > 1000) {
                break;
            }
            usleep(2);
        }
        return kSucc;
    }

    void EngineRace::prefetch(Visitor &visitor, int thread_id) {
        int i = 0;

        while (i < BUCKET_NUM) {
            if (partition[i].read == false) {
                usleep(1);
                continue;
            }
            int data_size = partition[i].metaLog.getSize();
            if (data_size == 0) {
                partition[i].shard_num.fetch_sub(1);
                i++;
                continue;
            }
            Location *p_loc = partition[i].metaLog.findAll();
            char *p_val = partition[i].valueLog.findAll();

            Location *j;
            for (j = p_loc; j < p_loc + data_size - 1; j++) {
                if (j->key != (j + 1)->key) {
                    uint64_t tmpKey = bswap_64(j->key);
                    PolarString pkey((char *) &tmpKey, 8);
                    int pos = j->addr;
                    PolarString pval(p_val + pos * 4096, 4096);
                    visitor.Visit(pkey, pval);
                }
            }

            uint64_t tmpKey = bswap_64(j->key);
            PolarString pkey((char *) &tmpKey, 8);
            int pos = j->addr;
            PolarString pval(p_val + pos * 4096, 4096);
            visitor.Visit(pkey, pval);
            partition[i].shard_num--;
            i++;
        }

        close(thread_id);
        _range_count--;
    }

    RetCode EngineRace::close(int thread_id) {
        std::string clostr = "close by thread:";
        clostr.append(std::to_string(thread_id)).append("\n");
        std::cout << clostr;
        for (int i = (thread_id) * THREAD_CAP; i < (thread_id + 1) * THREAD_CAP; i++) {
            partition[i].shard_num = 64;
            if (i!= BUCKET_NUM-1) {
                partition[i].read = false;
                partition[i].valueLog.clear();
            }
        }
        _container = 0;
        _waiting = true;
        return kSucc;
    }

}  // namespace polar_race
