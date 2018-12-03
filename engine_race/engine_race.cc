// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"
#include "util.h"
#include <sys/stat.h>
#include <pthread.h>
#include <iostream>
#include <cstdio>
#include <map>

namespace polar_race {

//    uint64_t chang2Uint(const PolarString &key) {
//        union Str2Uint data;
//        size_t size = key.size();
//        for (size_t i = 0; i < 8; ++i)
//        {
//            if (i<size)
//            {
//                data.data[i] = key[i];
//            }else{
//                data.data[i] = 0;
//            }
//        }
//        return data.key;
//    }

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

    void* timeout(void *threadid) {
        std::cout << "time out thread start " << *((int*)threadid) <<std::endl;
        auto st = std::chrono::high_resolution_clock::now();
        double duration = 0;
        while (duration < 600*1000) {
            auto end = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration<double , std::milli>(end-st).count();
            sleep(1);
        }
        exit(-1);
    }

    RetCode Engine::Open(const std::string& name, Engine** eptr) {
        pthread_t pth;
        int thread_id = 0;
        pthread_create(&pth, NULL, timeout, (void*)&thread_id);
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
//        std::cout <<"init meta, thread id:" + std::to_string(info.id)  + "\n";
    }

    struct PreRange {
        EngineRace *engineRace;
        int shard_id;
    };

    void * PreRead(void *arg) {
        PreRange preRange = ((PreRange *)arg)[0];
        int shard_id = preRange.shard_id;
        std::string pre = "pre read value ";
        pre.append(std::to_string(shard_id));
//        std::cout << pre <<std::endl;
        preRange.engineRace->partition[shard_id].valueLog.findAll();
//        std::cout << pre <<std::endl;
        preRange.engineRace->partition[shard_id-2].valueLog.clear();
        std::cout << pre <<std::endl;
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

        std::cout << "open success" << std::endl;

        *eptr = engine_race;
        return kSucc;
    }

    // 2. Close engine
    EngineRace::~EngineRace() {
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
    RetCode EngineRace::Write(const PolarString& key, const PolarString& value) {
        Location location{};
        RetCode retCode;
        uint16_t index;

        // 1
        location.key = str2uint(key);
        index = getIndex(key);
        Partition & part = partition[index];

        // 2
        retCode = part.valueLog.append(value, location.addr);
        if (retCode != kSucc) {
            return retCode;
        }
//        std::cout << "write index:" << index << " addr:" << location.addr << std::endl;
//        if (location.addr % 10000 == 0) {
        std::string msg = "index:";
        msg.append(std::to_string(index)).append(" key:").append(std::to_string(location.key)).append(" addr").append(std::to_string(location.addr)).append("size ").append(std::to_string(key.size()));
        std::cout << msg <<std::endl;
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
    RetCode EngineRace::Read(const PolarString& key, std::string* value) {
        Location location{};
        RetCode retCode;
        uint16_t index;

        // 1
        location.key = str2uint(key);
        index = getIndex(key);
        Partition & part = partition[index];

        // 2
        retCode = part.metaLog.find(location);
        if (retCode != kSucc) {
            std::cout << " not found "<<location.key<<", index"<<index;
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
    RetCode EngineRace::Range(const PolarString& lower, const PolarString& upper,
        Visitor &visitor) {
        std::cout << "range start "<< lower.ToString() << " end: "<<upper.ToString() << std::endl;
        int thread_id = 0;
        if ((thread_id = _container.fetch_add(1)) < THREAD_NUM-1) {
            // 开启多线程读
            std::cout << thread_id <<std::endl;
            int record =0;
            while (_waiting) {
                if (record++ >100000) {
//                    exit(-1);
                    break;
                }
                usleep(2);
            }
        }
        _waiting = false;

        if(_container!=64) {
            for (int i = 0; i < BUCKET_NUM; ++i) {
                partition[i].shard_num = 1;
            }
        }
        // 2. 开始读
        prefetch(visitor, thread_id);

        int count = 0;
        while (_range_count <=THREAD_NUM-1 ) {
            if (count++>100000) {
                break;
            }
            usleep(2);
        }
        return kSucc;
    }

    void EngineRace::prefetch(Visitor &visitor, int thread_id) {
        PreRange info;
        info.engineRace = this;

        int i=0;
        int _readone = -1;
        std::map<int, int> storeMap;
        std::map<int, int> visitorMap;

        while (i<BUCKET_NUM) {
            if (partition[i].shard_num == 0) {
                if (i+2 <1024 && partition[i+2].read==false) {
                    partition[i+2].read = true;
                    info.shard_id = i+2;
                    std::string part = "part start: ";
                    part.append(std::to_string(thread_id)).append(" part: ").append(std::to_string(i));
                    std::cout << part <<std::endl;
                    pthread_create(&tids[thread_id], nullptr, PreRead, &info);
                }
                i++;
                continue;
            }
            if (i==_readone) {
                usleep(5);
                continue;
            }
            int data_size = partition[i].metaLog.getSize();
            storeMap[i] = data_size;
            if (data_size == 0) {
                partition[i].shard_num.fetch_sub(1);
                _readone = i;
                continue;
            }
//            std::cout << "get metalog "<<i <<" data size"<<data_size<<std::endl;
            Location *p_loc = partition[i].metaLog.findAll();
//            std::cout << "get valuelog "<<i <<std::endl;
            char *p_val = partition[i].valueLog.findAll();
            try {
                int tmp_sum = 0;
                for (int j=0; j<data_size-1;j++) {
//                    std::cout<< j << " key:"<<(p_loc+j)->key<<" loc:"<<(p_loc+j)->addr<<std::endl;
                    if ((p_loc+j)->key != (p_loc+j+1)->key) {
                        char *key_ch = uint2char((p_loc+j)->key);
                        PolarString pkey(key_ch,8);
                        int pos = (p_loc+j)->addr;
                        PolarString pval(p_val+pos*4096, 4096);
                        std::cout << j << " key:"<<(p_loc+j)->key<<" polar key:"<<pkey.ToString()<<" loc:"<<(p_loc+j)->addr<<std::endl;
                        visitor.Visit(pkey, pval);
                        tmp_sum++;
                    }
                }
                char *key_ch = uint2char((p_loc+data_size-1)->key);
                PolarString pkey(key_ch,8);
                int pos = (p_loc+data_size-1)->addr;
                PolarString pval(p_val+pos*4096, 4096);
                visitor.Visit(pkey, pval);
                tmp_sum++;
                visitorMap[i] = tmp_sum;
            } catch (std::exception e){
                std::cout << e.what() << std::endl;
            }

            partition[i].shard_num.fetch_sub(1);
            _readone = i;
            if (i==10) {
                break;
            }
        }
        int storeSum = 0;
        int visitorSum = 0;
        std::string result;
        for (int i=0;i<1024;i++) {
            storeSum+=storeMap[i];
            visitorSum+=visitorMap[i];
            if (storeMap[i] != visitorMap[i]) {
                result.append(std::to_string(i)).append(" ").append(std::to_string(storeMap[i])).append(" ")
                .append(std::to_string(visitorMap[i])).append("; ");
            }
        }
        result.append("\nstore key sum:").append(std::to_string(storeSum)).append(" visitor sum:").append(std::to_string(visitorSum));
        std::cout <<result <<std::endl;

        close(thread_id);
        if (thread_id == THREAD_NUM-1) {  // 保留最后一个分片数据
            partition[BUCKET_NUM-1].read = true;
            partition[BUCKET_NUM-1].shard_num = 1;
            partition[BUCKET_NUM-2].valueLog.clear();
        }
        _range_count.fetch_add(1);
    }


    RetCode EngineRace::close(int thread_id) {
        std::string clostr = "close by thread:";
        clostr.append(std::to_string(thread_id)).append("\n");
        std::cout << clostr;
        for (int i=(thread_id)*THREAD_CAP; i<(thread_id+1)*THREAD_CAP;i++) {
            partition[i].read = false;
            partition[i].shard_num = 64;
        }
        _container = 0;
        _waiting = true;
        return kSucc;
    }

}  // namespace polar_race
