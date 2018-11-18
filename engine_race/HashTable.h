//
// Created by wangzhi on 18-11-18.
//

#ifndef ENGINE_HASHTABLE_H
#define ENGINE_HASHTABLE_H

#include "include/engine.h"
#include <atomic>
#include <cassert>

namespace polar_race {

    template <typename Key, typename Value>
    struct HashNode {
        Key key;
        Value value;
        HashNode<Key, Value> *next;
    };

    template <typename Key, typename Value>
    class HashTable {
        typedef HashNode<Key, Value> *pHashNode;
    public:
        explicit HashTable(int hint);
        ~HashTable();
        RetCode addOrUpdate(const Key &, const Value &);
        RetCode find(const Key &, Value &);

    private:
        pHashNode *_bucket;
        long _bucketSize;
        std::atomic_long _size;
    };

    template<typename Key, typename Value>
    HashTable<Key, Value>::HashTable(int hint) {
        static unsigned long primes[] = {509, 509, 1021, 2053, 4093, 8191, 16381, 32771, 65521, 98317};
        int i;
        assert(hint >= 0);
        for (i = 1; primes[i] < hint; ++i);

        _bucketSize = primes[i-1];
        _size = 0;
        _bucket = new pHashNode[_bucketSize];
        for (int j = 0; j < _bucketSize; ++j) {
            _bucket[j] = nullptr;
        }
    }

    template<typename Key, typename Value>
    HashTable<Key, Value>::~HashTable() {
        if (_size > 0) {
            pHashNode p,q;
            for (int i = 0; i < _size; ++i) {
                for (p = _bucket[i]; p; p = q) {
                    q = p->next;
                    delete(p);
                }
            }
        }
        delete[](_bucket);
    }

    template<typename Key, typename Value>
    RetCode HashTable<Key, Value>::addOrUpdate(const Key &key, const Value &value) {
        pHashNode p;
        unsigned long index;
        index = key % _bucketSize;
        for (p = _bucket[index]; p ; p = p->next) {
            if (key == p->key) {
                break;
            }
        }

        if (p == nullptr) {
            p = new HashNode<Key, Value>;
            p->key = key;
            p->next = _bucket[index];
            _bucket[index] = p;
            _size++;
        }

        p->value = value;

        return kSucc;
    }

    template<typename Key, typename Value>
    RetCode HashTable<Key, Value>::find(const Key &key, Value &value) {
        unsigned long index;
        pHashNode p;

        index = key%_bucketSize;
        for (p = _bucket[index]; p ; p = p->next) {
            if (key == p->key) {
                break;
            }
        }

        if (p == nullptr) {
            return kNotFound;
        }
        value = p->value;
        return kSucc;
    }


}



#endif //ENGINE_HASHTABLE_H
