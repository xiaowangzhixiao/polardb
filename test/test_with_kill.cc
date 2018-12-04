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
#include <algorithm>
#include <sstream>
#include <byteswap.h>

static const char kEnginePath[] = "/tmp/test_engine";
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
    uint64_t key;
};

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

std::string string_to_hex(const std::string& str) //transfer string to hex-string
{
    std::string result="0x";
    std::string tmp;
    std::stringstream ss;
    for(int i=0;i<str.size();i++)
    {
        ss<<std::hex<<int(str[i])<<std::endl;
        ss>>tmp;
        result+=tmp;
    }
    return result;
}

uint16_t getIndex(const PolarString &key) {
    if (key.size() > 1) {
        return ((((uint16_t)key[0]) << 2) | (((uint16_t)key[1] >> 6) & 0x3)) & 0x03ff;
    } else {
        return (((uint16_t)key[0]) << 2) & 0x03ff;
    }
}

int main2() {
//    std::string str("a1234567");
//    uint64_t u_int = chang2Uint(str);
//    std::cout << str << " "<< u_int <<std::endl;
//    uint64_t ch_int = bswap_64(u_int);
//    std::cout << u_int << " "<< ch_int <<std::endl;
//    PolarString pl((char*)&u_int, 8);
//    std::cout << pl.ToString() <<std::endl;

    /*uint64_t u_int = 6647396;
    bswap_64(u_int);
    std::cout << u_int <<std::endl;
    std::string pl((char*)&u_int, 8);
//    std::reverse(pl.begin(), pl.end());
    std::cout << pl <<std::endl;
    PolarString pstr(pl);
    std::cout << pstr.ToString() <<std::endl;


    std::cout << "测试 uint2char" <<std::endl;
    char ch[8];
    uint2char(u_int, ch);
    PolarString pkey(ch, 8);
    printf("%s", ch);
    std::cout << u_int << "#" << pkey.ToString() << std::endl;
    std::cout << chang2Uint(pkey) <<std::endl;

    std::cout << "测试 str2uint" <<std::endl;
    char ll[8] = {'\0', '\0', '\0', '\0', '\0', 'e', 'n', 'd'};
    printf("%s", ll);
    PolarString testll(ll, 8);
    std::cout << testll.ToString() <<" "<< testll.size() << std::endl;
    std::cout << str2uint(testll) <<std::endl;
    PolarString str("abcdfesg");
    std::cout << testll.compare(str) <<std::endl;*/


    /*std::string path = "D:\\competition\\kvdb\\meta_538";
    struct stat fileInfo{};
    stat(path.c_str(), &fileInfo);
    int size = fileInfo.st_size;
    std::cout << "file size:" << fileInfo.st_size <<std::endl;
    int _fd = open(path.c_str(), O_RDWR | O_CREAT, 0644);
    Location *_table = static_cast<Location *>(malloc(size));
    pread(_fd, _table, size, 0);
    std::cout << "now merge" <<std::endl;

    merge_sort(_table, size/16);

    std::cout << "traversal merge array"<<std::endl;
    uint64_t tmp = _table[0].key;
    Location *p_loc = new Location[24];
    for (int i=0;i<size/16;i++) {
        std::cout << "index: " <<i<< " key: "<<_table[i].key << " loc: "<<_table[i].addr<<std::endl;
        if (tmp > _table[i].key) {
            std::cout << "traversal error" << std::endl;
        }
        tmp = _table[i].key;
        p_loc[2*i].key = _table[i].key;
        p_loc[2*i].addr = _table[i].addr;
        p_loc[2*i+1].key = _table[i].key;
        p_loc[2*i+1].addr = _table[i].addr;
    }

    std::cout << "traversal the double array"<<std::endl;
    for (int i=0;i<24;i++) {
        std::cout << "index: " <<i<< " key: "<<p_loc[i].key << " loc: "<<p_loc[i].addr<<std::endl;
    }

    std::cout << "\n\nduplicate"<<std::endl;
    for (int j=0; j<24-1;j++) {
        if ((p_loc+j)->key != (p_loc+j+1)->key) {
            PolarString pkey((char*)&(p_loc+j)->key,8);
            int pos = (p_loc+j)->addr;
            std::cout << "key:"<<(p_loc+j)->key<<" polar key:"<<pkey.ToString()<<" loc:"<<(p_loc+j)->addr<<std::endl;
        }
    }
    PolarString pkey((char*)&(p_loc+24-1)->key,8);
    int pos = (p_loc+24-1)->addr;
    std::cout << "key:"<<(p_loc+24-1)->key<<" polar key:"<<pkey.ToString()<<" loc:"<<(p_loc+24-1)->addr<<std::endl;*/

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
//    for (int i = 0; i < 64; ++i) {
        sreaders.emplace_back(std::thread(sequentialRead, engine, std::cref(keys)));
//    }
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
