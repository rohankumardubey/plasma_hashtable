#pragma once

#include "murmurhash3.h"
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <iostream>
#include <ostream>

using namespace std;

const int ALIGN_SIZE = 4096;

struct bytes {
    char *data;
    int size;

    bytes(char *p, int sz) {
        data = p;
        size = sz;
    }

    bytes() {
        size = 0;
        data = nullptr;
    }

    bytes & operator=(const bytes &s) {
        data = s.data;
        size = s.size;
        return *this;
    }

    bytes(const bytes &s) {
        data = s.data;
        size = s.size;
    }

    bool operator==(const bytes &other) const {
        return size == other.size && memcmp(data, other.data, size) == 0;
    }

    ~bytes() {
        data = nullptr;
        size = 0;
    }
};

bytes bytes_dup(const bytes &src);

void bytes_free(const bytes &s);

struct Buffer {
    int size;
    void *buf;

    Buffer() :size(0), buf(nullptr) {}

    bytes Alloc(int n) {
        if (n > size) {
            auto size2 = n*2;
            void *buf2;

            auto r = posix_memalign(&buf2, ALIGN_SIZE, ALIGN_SIZE * (size2/ALIGN_SIZE)+ ALIGN_SIZE);
            assert(r == 0);
            free(buf);
            size = size2;
            buf = buf2;
        }

        return bytes{reinterpret_cast<char*>(buf), n};
    }

    bytes Resize(int n) {
        if (n > size) {
            buf = realloc(buf, n);
            if (reinterpret_cast<uintptr_t>(buf) % ALIGN_SIZE != 0) {
                void *buf2;
                auto r = posix_memalign(&buf2, ALIGN_SIZE, ALIGN_SIZE * (n/ALIGN_SIZE)+ ALIGN_SIZE);
                assert(r == 0);
                memcpy(buf2, buf, size);
                free(buf);
                buf = buf2;
            }
            size = n;
        }
        return bytes{reinterpret_cast<char*>(buf), size};
    }

    ~Buffer() {
        free(buf);
    }
};


ostream & operator << (ostream &out, const bytes &s);

struct bytesHasher {
      size_t operator()(const bytes& k) const {
        uint32_t h;
        MurmurHash3_x86_32(k.data, k.size, 0, &h);
        return (size_t) h;
      }
};
