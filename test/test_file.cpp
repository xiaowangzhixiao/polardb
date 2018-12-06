//
// Created by Idan on 2018/12/6.
//

#include <fstream>
#include <chrono>
#include <vector>
#include <cstdint>
#include <numeric>
#include <random>
#include <algorithm>
#include <iostream>
#include <cassert>

std::vector<uint64_t> GenerateData(std::size_t bytes)
{
    assert(bytes % sizeof(uint64_t) == 0);
    std::vector<uint64_t> data(bytes / sizeof(uint64_t));
    std::iota(data.begin(), data.end(), 0);
    std::shuffle(data.begin(), data.end(), std::mt19937{ std::random_device{}() });
    return data;
}

long long option_1(std::size_t bytes)
{
    std::vector<uint64_t> data = GenerateData(bytes);

    auto startTime = std::chrono::high_resolution_clock::now();
    auto myfile = std::fstream("C:\\kvdb\\fs_file.binary", std::ios::out | std::ios::binary);
    myfile.write((char*)&data[0], bytes);
    myfile.close();
    auto endTime = std::chrono::high_resolution_clock::now();

    return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
}

long long option_2(std::size_t bytes)
{
    std::vector<uint64_t> data = GenerateData(bytes);

    auto startTime = std::chrono::high_resolution_clock::now();
    FILE* file = fopen("C:\\kvdb\\fopen_file.binary", "wb");
    fwrite(&data[0], 1, bytes, file);
    fclose(file);
    auto endTime = std::chrono::high_resolution_clock::now();

    return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
}

long long option_3(std::size_t bytes)
{
    std::vector<uint64_t> data = GenerateData(bytes);

    std::ios_base::sync_with_stdio(false);
    auto startTime = std::chrono::high_resolution_clock::now();
    auto myfile = std::fstream("C:\\kvdb\\fs_nosync_file.binary", std::ios::out | std::ios::binary);
    myfile.write((char*)&data[0], bytes);
    myfile.close();
    auto endTime = std::chrono::high_resolution_clock::now();

    return std::chrono::duration_cast<std::chrono::milliseconds>(endTime - startTime).count();
}

int main()
{
    const std::size_t kB = 1024;
    const std::size_t MB = 1024 * kB;
    const std::size_t GB = 1024 * MB;

    for (std::size_t size = 1 * MB; size <= 2 * GB; size *= 2) std::cout << "option1, " << size / MB << "MB: " << option_1(size) << "ms" << std::endl;
    for (std::size_t size = 1 * MB; size <= 2 * GB; size *= 2) std::cout << "option2, " << size / MB << "MB: " << option_2(size) << "ms" << std::endl;
    for (std::size_t size = 1 * MB; size <= 2 * GB; size *= 2) std::cout << "option3, " << size / MB << "MB: " << option_3(size) << "ms" << std::endl;

    return 0;
}