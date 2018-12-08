//
// Created by wangzhi on 18-11-16.
//

#ifndef ENGINE_UTIL_H
#define ENGINE_UTIL_H

#include <string>
#include <byteswap.h>
#include "MetaLog.h"


namespace polar_race {

    bool FileExists(const std::string& path);

    void merge_tow_arr(Location A[], Location tmpA[], int l, int r, int r_end);

    void merge_sort(Location A[], int N);

    int binary_search(Location arr[], int size, uint64_t key);

    int binary_search(Location arr[], uint64_t key, int l, int r);

    uint64_t str2uint(const PolarString &key);

    void uint2char(const uint64_t num, char ch[] );

}



#endif //ENGINE_UTIL_H
