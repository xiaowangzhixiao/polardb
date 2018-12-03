//
// Created by wangzhi on 18-11-17.
//
#include <assert.h>
#include <stdio.h>
#include <string>
#include "include/engine.h"

#include <thread>
#include <mutex>
#include <random>
#include <iostream>
#include <algorithm>
#include <set>
//#include <sys/stat.h>
//#include <sys/fcntl.h>
//#include <unistd.h>
//#include <engine_race/util.h>
//#include <stdlib.h>

static const char kEnginePath[] = "D:\\competition\\kvdb";
static const char kDumpPath[] = "/tmp/test_dump";

using namespace polar_race;

class DumpVisitor : public Visitor {
public:
    DumpVisitor(int *kcnt)
            : key_cnt_(kcnt) {}

    ~DumpVisitor() {}

    void Visit(const PolarString &key, const PolarString &value) {
        printf("Visit %s --> %s\n", key.data(), value.data());
        (*key_cnt_)++;
    }

private:
    int *key_cnt_;
};


template<typename T>
class threadsafe_vector : public std::vector<T> {
public:
    void add(const T &val) {
        std::lock_guard<std::mutex> lock(mMutex);
        this->push_back(val);
    }

    void add(T &&val) {
        std::lock_guard<std::mutex> lock(mMutex);
        this->emplace_back(val);
    }

private:
    mutable std::mutex mMutex;
};

class RandNum_generator {
private:
    RandNum_generator(const RandNum_generator &) = delete;

    RandNum_generator &operator=(const RandNum_generator &) = delete;

    std::uniform_int_distribution<unsigned> u;
    std::default_random_engine e;
    int mStart, mEnd;
public:
    // [start, end], inclusive, uniformally distributed
    RandNum_generator(int start, int end)
            : u(start, end), e(std::hash<std::thread::id>()(std::this_thread::get_id())
                               + std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::high_resolution_clock::now().time_since_epoch()).count()), mStart(start), mEnd(end) {}

    // [mStart, mEnd], inclusive
    unsigned nextNum() {
        return u(e);
    }

    // [0, max], inclusive
    unsigned nextNum(unsigned max) {
        return unsigned((u(e) - mStart) / float(mEnd - mStart) * max);
    }
};

std::string random_str(RandNum_generator &rng, std::size_t strLen) {
    std::string rs(strLen, ' ');
    for (auto &ch : rs) {
        ch = rng.nextNum();
    }
    return rs;
}

typedef unsigned long long hash64_t;

hash64_t fnv1_hash_64(const std::string &str) {
    static const hash64_t fnv_offset_basis = 14695981039346656037u;
    static const hash64_t fnv_prime = 1099511628211u;
    hash64_t hv = fnv_offset_basis;
    for (auto ch : str) {
        hv *= fnv_prime;
        hv ^= ch;
    }
    return hv;
}

std::string hash_to_str(hash64_t hash) {
    const int cnt = 8;
    char val[cnt];
    for (int i = 0; i < cnt; ++i) {
        val[cnt - i - 1] = hash % 256;
        hash /= 256;
    }
    return std::string(val, cnt);
}

std::string key_from_value(const std::string &val) {
    std::string key(8, ' ');

    key[0] = val[729];
    key[1] = val[839];
    key[2] = val[25];
    key[3] = val[202];
    key[4] = val[579];
    key[5] = val[1826];
    key[6] = val[369];
    key[7] = val[2903];

    return key;
}

void write(Engine *engine, threadsafe_vector<std::string> &keys, unsigned numWrite) {
    RandNum_generator rng(0, 255);
    for (unsigned i = 0; i < numWrite; ++i) {
        std::string val(random_str(rng, 4096));

        //std::string key = hash_to_str(fnv1_hash_64(val)); // strong hash, slow but barely any chance to duplicate
        std::string key(key_from_value(val)); // random positions, faster but tiny chance to duplicate

        engine->Write(key, val);
        keys.add(key);
    }
}

void randomRead(Engine *engine, const threadsafe_vector<std::string> &keys, unsigned numRead) {
    RandNum_generator rng(0, keys.size() - 1);
    for (unsigned i = 0; i < numRead; ++i) {
        auto &key = keys[rng.nextNum()];
        std::string val;
        engine->Read(key, &val);
        //if (key != hash_to_str(fnv1_hash_64(val))) {
        if (key != key_from_value(val)) {
            std::cout << "key and value not match:" << key << std::endl;
            exit(-1);
        }
    }
}

class MyVisitor : public Visitor {
public:
    MyVisitor(const threadsafe_vector<std::string> &keys, int &cnt)
            : mKeys(keys), mCnt(cnt) {}

    ~MyVisitor() {}

    void Visit(const PolarString &key, const PolarString &value) {
        if (key != key_from_value(value.ToString())) {
            std::cout << "Sequential Read error: key and value not match" << std::endl;
            exit(-1);
        }
        mCnt += 1;
    }

private:
    const threadsafe_vector<std::string> &mKeys;
    unsigned mStart;
    int &mCnt;
};

void sequentialRead(Engine *engine, const threadsafe_vector<std::string> &keys) {
    int keyCnt = 0;
    MyVisitor visitor(keys, keyCnt);
    engine->Range("", "", visitor);
    std::string range_over = "range over, the key sum: ";
    range_over.append(std::to_string(keyCnt));
    std::cout << range_over << std::endl;
}

union Str2Uint {
    char data[8];
    unsigned long key;
};

uint64_t str2long(const PolarString &key) {
    uint64_t num = 0;
    for (int i = 0; i < 8; ++i) {
        num <<= 8;
        num |= (key[i] & 0XFF);
    }
    return num;
}

char *long2char(const uint64_t num) {
    char *ch = static_cast<char *>(malloc(8));
    for (int i = 0; i < 8; i++) {
        int offset = 64 - (i + 1) * 8;
        int tmp = (num >> offset) & 0XFF;
        ch[i] = (char) (tmp);
    }
    return ch;
}

unsigned long chang2Uint(const PolarString &key) {
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

int getIndex(const PolarString &key) {
    if (key.size() > 1) {
        return ((key[0] & 0XFF) << 2) | (key[1] >> 6 & 0X3F);
    } else {
        return key[0] & 0xff;
    }
}

int main2() {
//    std::string path = "D:\\competition\\kvdb\\meta";
//    int _fd = open(path.c_str(), O_RDWR | O_CREAT | O_SYNC);
//
//    PolarString key = "a1234567";
//    uint64_t lk = str2long(key);
//    Location loc;
//    loc.key = lk;
//    loc.addr = 518;
//    pwrite(_fd, &loc, 16, 0);
//    usleep(1000);
//    Location *ploc = static_cast<Location *>(malloc(16));
//    pread(_fd, ploc, 16, 0);
//    std::cout << ploc->key << " " << ploc->addr <<std::endl;
//    char* ch = long2char(ploc->key);
//    PolarString  pkey1(ch,8);
//    std::cout << pkey1.ToString() <<std::endl;
//    PolarString pk((char*)&ploc->key,8);
//    std::cout << pk.ToString() <<std::endl;

//    unsigned long lala = 7003434091038455352;
//    std::string str((char*)&lala,8);
//    std::cout << str <<std::endl;
//    PolarString pk2((char*)&lala,8);
//    std::cout << pk2.ToString() <<std::endl;

    /*PolarString key = "a1234567";
    PolarString key2 = "a1234568";
    PolarString key3 = "a1234569";
    uint64_t lk = str2long(key);
    uint64_t lk2 = str2long(key2);
    uint64_t lk3 = str2long(key3);
    std::cout << lk <<std::endl;
    std::cout << lk2 <<std::endl;
    std::cout << lk3 <<std::endl;
    char* ch = long2char(lk);
    char* ch2 = long2char(lk2);
    char* ch3 = long2char(lk3);
    PolarString  pkey1(ch,8);
    PolarString  pkey2(ch2,8);
    PolarString  pkey3(ch3,8);
    std::cout << ch << pkey1.ToString() <<std::endl;
    std::cout << ch << pkey2.ToString() <<std::endl;
    std::cout << ch << pkey3.ToString() <<std::endl;*/


    /*uint64_t keyArr[] = {7236832701905371136, 7236832701955702784, 7236832702006034432, 7236832702089920512,
                         7236832702140252160, 7236832702190583808, 7236832702274469888, 7236832702358355968,
                         7236832702408687616, 7236832702459019264, 7236832702509350912, 7236832702542905344,
                         7236832702559682560, 7236832706200338432};
    uint64_t tmp = keyArr[0];
    for (int i=1;i<14;i++) {
        if (keyArr[i] < tmp) {
            std::cout << "no order:" << i <<std::endl;
        }
        tmp = keyArr[i];
    }

    uint64_t u_int1 = 7236832702559682560;
    uint64_t u_int2 = 7236832706200338432;
    PolarString  pkey1((char*)&(keyArr[12]),8);
    PolarString  pkey2((char*)&(keyArr[13]),8);
    std::cout << pkey1.compare(pkey2) <<std::endl;

    PolarString  tmpStr((char*)&(keyArr[0]),8);
    for (int i=1;i<14;i++) {
        PolarString pstr((char*)&(keyArr[i]),8);
        std::cout << tmpStr.compare(pstr) <<std::endl;
        tmpStr.clear();
        tmpStr = PolarString((char*)&(keyArr[i]),8);
    }*/


    /* std::string test1 = "a1234567";
     std::string test2 = "a1234568";
     std::string test3 = "a1234569";
     uint64_t u_int1 = chang2Uint(test1);
     uint64_t u_int2 = chang2Uint(test2);
     uint64_t u_int3 = chang2Uint(test3);
     PolarString pstr1((char*)&(u_int1),8);
     PolarString pstr2((char*)&(u_int2),8);
     PolarString pstr3((char*)&(u_int3),8);

     std::cout << u_int1 <<" "<<pstr1.ToString() << " " << getIndex(pstr1) <<std::endl;
     std::cout << u_int2 <<" "<<pstr2.ToString() << " " << getIndex(pstr2) <<std::endl;
     std::cout << u_int3 <<" "<<pstr3.ToString() << " " << getIndex(pstr3) <<std::endl;
     std::cout << test1.compare(test2) <<" "<<test2.compare(test3) <<std::endl;
     std::cout << (u_int1<u_int2)<< " "<< (u_int2<u_int3) <<std::endl;
     std::cout << pstr1.compare(pstr2) << " " << pstr2.compare(pstr3) <<std::endl;*/

    /*std::set<uint64_t> set;
    uint64_t key1 = 7236832701905371136;
    uint64_t key2 = 7236832701922148352;
    uint64_t key3 = 7236832701938925568;

    PolarString p1((char*)&(key1),8);
    PolarString p2((char*)&(key2),8);
    PolarString p3((char*)&(key3),8);

    uint64_t pkey1 = 7236832701905371136;
    uint64_t pkey2 = 7236832701905371137;
    uint64_t pkey3 = 7236832701905371138;

    PolarString pk1((char*)&(pkey1),8);
    PolarString pk2((char*)&(pkey2),8);
    PolarString pk3((char*)&(pkey3),8);
    std::cout << (pkey2>key2)<<std::endl;
    std::cout << pk2.compare(p2)<<std::endl;
    std::cout << sizeof(pkey2) << " "<< sizeof(key2)<<std::endl;
    uint64_t *pi2 = &key2;
    uint64_t *pik2 = &pkey2;
    for (int i=0; i< 8;i++) {
        if (pi2[i] < pik2[i]) {
            std::cout << "pik2"<<std::endl;
            break;
        } else if (pi2[i] > pik2[i]) {
            std::cout << "pi2"<<std::endl;
            break;
        }
    }*/

/*    std::cout << pk1.compare(pk2) <<std::endl;
    std::cout << pk1.compare(pk3) <<std::endl;
    std::cout << p1.compare(p2) <<std::endl;
    std::cout << p1.compare(p3) <<std::endl;
    std::cout << pk2.compare(pk2) <<std::endl;
    std::cout << pk2.compare(p2) <<std::endl;
    std::cout << p2.compare(pk3) <<std::endl;

    std::cout << "get index" <<std::endl;
    std::cout << getIndex(p1)<<std::endl;
    std::cout << getIndex(p2)<<std::endl;
    std::cout << getIndex(p3)<<std::endl;
    std::cout << getIndex(pk1)<<std::endl;
    std::cout << getIndex(pk2)<<std::endl;
    std::cout << getIndex(pk3)<<std::endl;

    set.insert(key1);
    set.insert(key2);
    set.insert(key3);
    set.insert(pkey1);
    set.insert(pkey2);
    set.insert(pkey3);

    std::set<uint64_t>::iterator pset = set.begin();
    while (pset!= set.end()) {
        std::cout << *pset <<std::endl;
        pset++;
    }*/

/*    PolarString testKey("a1234567");
    uint64_t test_uint = chang2Uint(testKey);
    PolarString pkey((char*)&(test_uint),8);
    std::cout <<testKey.ToString()<<" uint: "<<test_uint<<" pkey: "<< pkey.ToString() <<std::endl;*/

//    std::string path = "D:\\competition\\kvdb\\meta_538";
//    struct stat fileInfo{};
//    stat(path.c_str(), &fileInfo);
//    int size = fileInfo.st_size;
//    std::cout << "file size:" << fileInfo.st_size <<std::endl;
//    int _fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
//    Location *_table = static_cast<Location *>(malloc(size));
//    pread(_fd, _table, size, 0);
//    std::cout << "now merge" <<std::endl;
//
//    merge_sort(_table, size/16);
//
//    std::cout << "traversal merge array"<<std::endl;
//    uint64_t tmp = _table[0].key;
//    Location *p_loc = new Location[24];
//    for (int i=0;i<size/16;i++) {
//        std::cout << "index: " <<i<< " key: "<<_table[i].key << " loc: "<<_table[i].addr<<std::endl;
//        if (tmp > _table[i].key) {
//            std::cout << "traversal error" << std::endl;
//        }
//        tmp = _table[i].key;
//        p_loc[2*i].key = _table[i].key;
//        p_loc[2*i].addr = _table[i].addr;
//        p_loc[2*i+1].key = _table[i].key;
//        p_loc[2*i+1].addr = _table[i].addr;
//    }
//
//    std::cout << "traversal the double array"<<std::endl;
//    for (int i=0;i<24;i++) {
//        std::cout << "index: " <<i<< " key: "<<p_loc[i].key << " loc: "<<p_loc[i].addr<<std::endl;
//    }
//
//    std::cout << "\n\nduplicate"<<std::endl;
//    for (int j=0; j<24-1;j++) {
//        if ((p_loc+j)->key != (p_loc+j+1)->key) {
//            PolarString pkey((char*)&(p_loc+j)->key,8);
//            int pos = (p_loc+j)->addr;
//            std::cout << "key:"<<(p_loc+j)->key<<" polar key:"<<pkey.ToString()<<" loc:"<<(p_loc+j)->addr<<std::endl;
//        }
//    }
//    PolarString pkey((char*)&(p_loc+24-1)->key,8);
//    int pos = (p_loc+24-1)->addr;
//    std::cout << "key:"<<(p_loc+24-1)->key<<" polar key:"<<pkey.ToString()<<" loc:"<<(p_loc+24-1)->addr<<std::endl;

}

int main()
{
    auto numThreads = std::thread::hardware_concurrency();
    std::cout << numThreads << std::endl;

    Engine *engine = NULL;

    threadsafe_vector<std::string> keys;

    // Write
    unsigned numWrite = 10000, numKills = 4;
    double duration = 0;
    for (int nk = 0; nk < numKills; ++nk) {
        RetCode ret = Engine::Open(kEnginePath, &engine);
        assert (ret == kSucc);

        auto writeStart = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> writers;
//        for (int i = 0; i < numThreads; ++i) {
            writers.emplace_back(std::thread(write, engine, std::ref(keys), numWrite / numKills));
//        }
        for (auto& th : writers) {
            th.join();
        }
        writers.clear();

        auto writeEnd = std::chrono::high_resolution_clock::now();
        duration += std::chrono::duration<double, std::milli>(writeEnd - writeStart).count();

        delete engine;
    }

    std::cout << "Writing takes: "
              << duration
              << " milliseconds" << std::endl;



    RetCode ret = Engine::Open(kEnginePath, &engine);
    assert (ret == kSucc);

    std::cout << keys.size() << std::endl;
    std::sort(keys.begin(), keys.end());
    auto last = std::unique(keys.begin(), keys.end());
    keys.erase(last, keys.end());
    std::cout << keys.size() << std::endl;

    // Random Read
    auto rreadStart = std::chrono::high_resolution_clock::now();

    unsigned numRead = 10000;
    std::vector<std::thread> rreaders;
//    for (int i = 0; i < numThreads; ++i) {
        rreaders.emplace_back(std::thread(randomRead, engine, std::cref(keys), numRead));
//    }
    for (auto& th : rreaders) {
        th.join();
    }
    rreaders.clear();

    auto rreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Random read takes: "
              << std::chrono::duration<double, std::milli>(rreadEnd - rreadStart).count()
              << " milliseconds" << std::endl;


    // Sequential Read

//    RetCode ret = Engine::Open(kEnginePath, &engine);
    auto sreadStart = std::chrono::high_resolution_clock::now();

    std::vector<std::thread> sreaders;
    for (int i = 0; i < 64; ++i) {
        sreaders.emplace_back(std::thread(sequentialRead, engine, std::cref(keys)));
    }
    for (auto& th : sreaders) {
        th.join();
    }
    sreaders.clear();

    auto sreadEnd = std::chrono::high_resolution_clock::now();
    std::cout << "Sequential read takes: "
              << std::chrono::duration<double, std::milli>(sreadEnd - sreadStart).count()
              << " milliseconds" << std::endl;

    delete engine;

    return 0;
}
