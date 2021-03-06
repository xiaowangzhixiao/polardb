//
// Created by wangzhi on 18-11-16.
//
#include <unistd.h>
#include "util.h"
#include <iostream>

namespace polar_race {
    bool FileExists(const std::string& path) {
        return access(path.c_str(), F_OK) == 0;
    }

    /**
    * 同一数组内归并排序
    * @param A
    * @param tmpA
    * @param l
    * @param r
    * @param r_end
    */
    void merge_tow_arr(Location A[], Location tmpA[], int l, int r, int r_end) {
        int l_end = r - 1, nums = r_end - l + 1, tmp = l;
        while (l <= l_end && r <= r_end) {
            if (A[l].key < A[r].key) {
                tmpA[tmp++] = A[l++];
            } else if (A[l].key > A[r].key) {
                tmpA[tmp++] = A[r++];
            } else {
                if (A[l].addr < A[r].addr) A[l].addr = A[r].addr;
                else if (A[l].addr > A[r].addr) A[r].addr = A[l].addr;
                tmpA[tmp++] = A[l++];
            }
        }
        while (l <= l_end) {
            tmpA[tmp++] = A[l++];
        }
        while (r <= r_end) {
            tmpA[tmp++] = A[r++];
        }
        for (int i = 0; i < nums; i++, r_end--) {
            A[r_end] = tmpA[r_end];
        }
    }

    // TODO 优化排序算法
    void merge_sort(Location A[], int N) {
        int length = 1;
        Location *tmpA = (Location *) malloc(N * sizeof(Location));

        if (tmpA != NULL) {
            while (length < N) {
                int i = 0;
                for (; i <= N - 2 * length; i += 2 * length) {
                    merge_tow_arr(A, tmpA, i, i + length, i + 2 * length - 1);
                }
                if (i + length < N) {
                    merge_tow_arr(A, tmpA, i, i + length, N - 1);
                }
                length *= 2;
            }
            free(tmpA);
        } else {
            std::cout << "memory" << std::endl;
        }

    }

    /**
     * 二分法查找 直接返回 addr
     * @param arr
     * @param size
     * @param key
     * @return
     */
//    int binary_search(Location* arr, int m, uint64_t key) {
//        while (m>1) {
//            int mid = m/2;
//            if (key < arr[mid].key) m = mid;
//            else {
//                arr+=mid;
//                m-=mid;
//            }
//        }
//        return key == arr[0].key? arr[0].addr:-1;
//    }

    int binary_search(Location arr[], int size, uint64_t key) {
        int l = 0, r = size - 1;
        while (l <= r) {
            int mid = (r - l) / 2 + l;
//            int mid = (int)(((double)(key - arr[l].key)) / (arr[r].key - arr[l].key))*(r -l)  + l;
            if (arr[mid].key < key) {
                l = mid + 1;
            } else if (arr[mid].key == key) {
                return arr[mid].addr;
            } else {
                r = mid - 1;
            }
        }
        return -1;
    }

    /**
     * 优化二分查找
     * @param arr
     * @param key
     * @param l
     * @param r
     * @return
     */
    int binary_search(Location arr[], uint64_t key, int l, int r) {
        while (l <= r) {
            int mid = (r - l) / 2 + l;
            if (arr[mid].key == key) {
                return arr[mid].addr;
            } else if (arr[mid].key < key) {
                l = mid + 1;
            } else {
                r = mid - 1;
            }
        }
        return -1;
    }

    uint64_t str2uint(const PolarString &key) {
        uint64_t num = 0;
        for (int i = 0; i < 8; ++i) {
            num <<=8;
            num |= (key[i]&0XFF);
        }
        return num;
    }

    void uint2char(const uint64_t num, char ch[]) {
        for (int i=0;i<8;i++) {
            int offset = 64-(i+1)*8;
            int tmp = (num>>offset) & 0XFF;
            ch[i] = (char) (tmp);
        }
    }

}

