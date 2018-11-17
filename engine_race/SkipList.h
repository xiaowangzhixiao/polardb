//
// Created by wangzhi on 18-11-14.
//

#ifndef POLARDB_SKIPLIST_H
#define POLARDB_SKIPLIST_H

#include <climits>
#include <iostream>
#include <random>
#include <cstdlib>
#include <atomic>
#include "include/engine.h"

#define MAX_LEVEL 9
namespace polar_race {

    template<typename Key, typename Value>
    struct SkipListNode
    {
        Key key;
        Value value;
        SkipListNode<Key, Value> *forward[1];
    };

    template <typename Key, typename Value>
    class SkipList
    {
        typedef SkipListNode<Key,Value>* pSkipListNode;
    public:
        SkipList();
        ~SkipList();
        RetCode addOrUpdate(const Key &, const Value &);
        RetCode find(const Key &, Value &);
        RetCode remove(const Key& k, Value &);
        void print();

    private:
        std::atomic_uint _size;
        std::atomic_uint _level;
        pSkipListNode head;
        pSkipListNode tail;

        pSkipListNode create(int level = 0)
        {
            size_t size = sizeof(SkipListNode<Key, Value>) + level * sizeof(pSkipListNode);
            return (pSkipListNode)malloc(size);
        }

        int randomLevel()
        {
            int lev = 0;
            std::random_device rd;
            while(rd() % 4 == 0 && lev < MAX_LEVEL - 1)
                ++lev;
            return lev;
        }
    };

    template<typename Key,typename Value>
    SkipList<Key,Value>::SkipList():_size(0),_level(0)
    {
        tail = create();
        // 设置尾部key为64位最大值
        tail->key = std::numeric_limits<u_int64_t>::max();
        head = create(MAX_LEVEL);
        for (int i = 0; i < MAX_LEVEL; ++i) {
            head->forward[i] = tail;
        }
    }

    template<typename Key,typename Value>
    SkipList<Key,Value>::~SkipList()
    {
        pSkipListNode p = head;
        pSkipListNode q;
        while(p != tail)
        {
            q = p->forward[0];
            free(p);
            p = q;
        }
        free(p);  //tail
    }

    template<typename Key,typename Value>
    RetCode SkipList<Key,Value>::addOrUpdate(const Key& k,const Value& t)
    {
        pSkipListNode update[MAX_LEVEL], p = head;
        for(int i = _level; i >= 0; i--)
        {
            while(p->forward[i] != tail && p->forward[i]->key < k)
                p = p->forward[i];
            update[i] = p;
        }

        p = p->forward[0];

        // update
        if(p != tail && p->key == k)
        {
            p->value = t;
            return kSucc;
        }

        int lev = randomLevel();

        if(lev > _level)
        {
            lev = ++_level;
            update[lev] = head;
        }

        pSkipListNode newNode = create(lev);
        newNode->key = k;
        newNode->value = t;

        // 从底层插入，保证一致性
        for(int i = lev; i >= 0; i--)
        {
            p = update[i];
            while (true) {
                newNode->forward[i] = p->forward[i];
                if (__sync_bool_compare_and_swap(&p->forward[i], newNode->forward[i], newNode)) {
                    break;
                }
            }
        }

        ++_size;
        return kSucc;
    }

    template<typename Key,typename Value>
    RetCode SkipList<Key,Value>::remove(const Key& k,Value& tout)
    {
        pSkipListNode update[MAX_LEVEL], p = head;
        for(int i = _level; i >= 0; i--)
        {
            while(p->forward[i] != tail && p->forward[i]->key < k)
                p = p->forward[i];

            update[i] = p;
        }

        p = p->forward[0];

        if(p != tail && p->key == k)
        {
            tout = p->value;

            for(int i = 0; i <= _level; i++)
            {
                if(update[i]->forward[i] != p)
                    break;
                update[i]->forward[i] = p->forward[i];
            }

            free(p);
            while(_level > 0 && head->forward[_level] == tail)
                _level--;

            _size--;
            return kSucc;
        }
        return kNotFound;
    }

    template<typename Key,typename Value>
    RetCode SkipList<Key,Value>::find(const Key& k, Value &value)
    {
        pSkipListNode p = head;
        for(int i = _level; i >= 0; i--)
        {
            while(p->forward[i] != tail && p->forward[i]->key < k)
                p = p->forward[i];
        }
        p = p->forward[0];
        if(p != tail && k == p->key)
        {
            value = p->value;
            return kSucc;
        } else {
            // TODO: 到序列化文件中查找
        }
        return kNotFound;
    }

    template<typename Key,typename Value>
    void SkipList<Key,Value>::print()
    {
        pSkipListNode p = head;
        while(p->forward[0] != tail)
        {
            std::cout << "Key:" << p->forward[0]->key << "Value:" << p->forward[0]->value << std::endl;
            p = p->forward[0];
        }
    }

}


#endif //POLARDB_SKIPLIST_H
