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
        for (int i = 0; i < BUCKET_NUM / THREAD_NUM; ++i) {
            uint32_t index = (uint32_t) thread_id * (BUCKET_NUM / THREAD_NUM) + i;
            int real_size = engineRace->partition[index].metaLog.init(engineRace->_dir, index);  //返回真正的size
            engineRace->partition[index].valueLog.init(engineRace->_dir, index, real_size); //初始化value
        }
//        std::cout <<"init meta, thread id:" + std::to_string(thread_id)  + "\n";
    }

    // 第一次读预读
    void preRead(EngineRace *engineRace, int thread_id) {
        for (int i = 0; i < BUCKET_NUM / THREAD_NUM; ++i) {
            uint32_t index = (uint32_t) thread_id * (BUCKET_NUM / THREAD_NUM) + i;
            if(index +1 < (thread_id +1) * (BUCKET_NUM / THREAD_NUM) ){
                engineRace->partition[index + 1].metaLog.readAhread();
            }
            engineRace->partition[index].metaLog.findAll();
            engineRace->partition[index].valueLog.preRead();
        }
    }

    void PreReadWithThread(EngineRace *engineRace, int shard_id) {
        std::string pre = "pre read value ";
        pre.append(std::to_string(shard_id)).append("\n");
        engineRace->partition[shard_id - 2].valueLog.clear();
        engineRace->partition[shard_id].valueLog.findAll();
        std::cout << pre;
    }

    // 1. Open engine
    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        *eptr = nullptr;
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

        std::cout << "open success" << std::endl;

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
//        std::cout << "write index:" << index << " addr:" << location.addr << std::endl;
//        if (location.addr % 10000 == 0) {
//            std::cout << "write index:" << index << " addr:" << location.addr << std::endl;
//        }
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
                std::vector<std::thread> initvec;
                for (uint8_t i = 0; i < THREAD_NUM; ++i) {
                    initvec.emplace_back(std::thread(preRead, this, i));
                }

                for (auto& th:initvec) {
                    th.join();
                }
                std::cout << "pre read over" <<std::endl;
                _firstRead = false;
            }
            _loading = false;
        } else {
            while (_firstRead) {
                usleep(5);
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

//        if (location.addr % 11111 == 0) {
//            std::cout << "read index:" + std::to_string(index) + " addr:" + std::to_string(location.addr) + "\n";
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
    RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                              Visitor &visitor) {

        bool loading = false;
        _loading.compare_exchange_strong(loading, true);
        if (!loading) {
            if (_firstRead) {
                std::vector<std::thread> initvec;
                for (uint8_t i = 0; i < THREAD_NUM; ++i) {
                    initvec.emplace_back(std::thread(preRead, this, i));
                }

                for (auto& th:initvec) {
                    th.join();
                }
                _firstRead = false;
            }
            _loading = false;
        } else {
            while (_firstRead) {
                usleep(5);
            }
        }

//        std::cout << "range start " << lower.ToString() << " end: " << upper.ToString() << std::endl;
        int thread_id = 0;
        if ((thread_id = _container.fetch_add(1)) < THREAD_NUM - 1) {
            // 开启多线程读
            int record = 0;
            while (_waiting) {
                if (record++ > 10000) {
//                    exit(-1);
                    break;
                }
                usleep(2);
            }
        }
//        std::cout << thread_id << " " << _waiting << std::endl;
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
            if (count++ > 10000) {
                break;
            }
            usleep(2);
        }
//        std::cout << thread_id<<" "<<_range_count<<std::endl;
        return kSucc;
    }

    void EngineRace::prefetch(Visitor &visitor, int thread_id) {
        int i = 0;
        int _readone = -1;
        std::map<int, int> storeMap;
        std::map<int, int> visitorMap;

        while (i < BUCKET_NUM) {
            if (partition[i].shard_num == 0) {
                if (i + 2 < 1024 && partition[i + 2].read == false) {
                    partition[i + 2].read = true;
                    std::thread thread(PreReadWithThread, this, i + 2);
                    thread.detach();
                }
                i++;
                continue;
            }
            if (i == _readone) {
                usleep(2);
                continue;
            }
            int data_size = partition[i].metaLog.getSize();
            storeMap[i] = data_size;
            if (data_size == 0) {
                partition[i].shard_num.fetch_sub(1);
                _readone = i;
                continue;
            }
            Location *p_loc = partition[i].metaLog.findAll();
            char *p_val = partition[i].valueLog.findAll();
            int tmp_sum = 0;
            for (int j = 0; j < data_size - 1; j++) {
                if ((p_loc + j)->key != (p_loc + j + 1)->key) {
                    uint64_t tmpKey = bswap_64((p_loc + j)->key);
                    PolarString pkey((char *) &tmpKey, 8);
                    int pos = (p_loc + j)->addr;
                    PolarString pval(p_val + pos * 4096, 4096);
                    visitor.Visit(pkey, pval);
                    tmp_sum++;
                }
            }
            uint64_t tmpKey = bswap_64((p_loc + data_size-1)->key);
            PolarString pkey((char *) &tmpKey, 8);
            int pos = (p_loc + data_size - 1)->addr;
            PolarString pval(p_val + pos * 4096, 4096);
            visitor.Visit(pkey, pval);
            tmp_sum++;
            visitorMap[i] = tmp_sum;


            partition[i].shard_num.fetch_sub(1);
            _readone = i;
        }

        close(thread_id);
        _range_count.fetch_add(1);
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
