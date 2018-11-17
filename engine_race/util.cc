//
// Created by wangzhi on 18-11-16.
//
#include <unistd.h>
#include "util.h"

namespace polar_race {
    bool FileExists(const std::string& path) {
        return access(path.c_str(), F_OK) == 0;
    }
}

