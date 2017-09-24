#pragma once

#include "common.h"

class BloomFilter {
public:
    BloomFilter(void *data, size_t size, int hashFns) :filter(static_cast<uint8_t *>(data)), size(size) {}

    void Add(const bytes &itm) {
        for (auto i=0; i<hashFns; i++) {
            uint32_t h;
            MurmurHash3_x86_32(itm.data, itm.size, i+1, &h);
            h %= size*8;
            filter[h/8] |= (1 << h % 8);
        }
    }

    bool Test(const bytes &itm) {
        for (auto i=0; i<hashFns; i++) {
            uint32_t h;
            MurmurHash3_x86_32(itm.data, itm.size, i+1, &h);
            h %= size*8;
            if (!(filter[h/8] & (1 << h % 8))) {
                return false;
            }
        }

        return true;
    }

    void Reset() {
        for (auto i=0; i<size; i++) {
            filter[i] = 0;
        }
    }

private:

    uint8_t *filter;
    size_t size;
    int hashFns;
};
