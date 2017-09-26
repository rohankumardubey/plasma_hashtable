#pragma once

#include <iostream>
#include <string>
#include <vector>
#include <atomic>
#include <unordered_map>
#include <string.h>
#include "common.h"
#include "log.h"
#include "murmurhash3.h"
#include "bloom.h"

#define USE_BLOOMFILTER
#define WRITE_BUFFER_SIZE 1024*1024

using namespace std;

struct HTBucketInfo {
    LogOffset offset;
    uint8_t segments;
    uint8_t version;
    uint8_t count;
    // 5 bytes bloom filter
    uint8_t bloom;
    uint32_t _bloom;

    HTBucketInfo() :offset(0), count(0), segments(0), bloom(0), _bloom(0) {}
};

struct HTData {
    uint32_t bucketID;
    uint8_t version;
    LogOffset nextOffset;
};

const int keyLenSize = 2;
const int valLenSize = 4;

struct kv {
    const bytes k, v;
};


const bytes deleteValue;

class HashTable {
public:

    HashTable(int nb, const string &filepath);

    void Delete(const bytes &key);

    void Set(const bytes &key, const bytes &value);

    bytes Get(const bytes &key, Buffer &b);

    float GetLogFragmentation();

    void writeHTData(int id, HTBucketInfo *bInfo, vector<kv> &kvs, int maxSegments);
    void compactLog(float fragThreshold, Buffer &b);

    //~HashTable();

    void Dump();
    void Stats();

private:
    uint32_t hash(const bytes &key) {
        uint32_t h {0};
        MurmurHash3_x86_32(key.data, key.size, 0, &h);
        return h;
    }

    int numBuckets;
    int maxSegments;
    int numHashes;
    HTBucketInfo *bucketDir;
    Log *log;

    vector<struct kv> kvs;
    Buffer wBuf;

    atomic<uint64_t> DataSize;
};

class KVCallback {
public:
    virtual bool Call(const bytes &k, const bytes &v) {
        return true;
    }
};

class PrintKVCallback: public KVCallback{
    bool Call(const bytes &k, const bytes &v) {
        cout<<"kv pair :"<<string(k.data, k.size)<<" "<<string(v.data, v.size)<<endl;
        return true;
    }
};

class LookupKVCallback: public KVCallback{
public:
    LookupKVCallback(const bytes &k) :lookup(k), Found(false) {}
    bool Call(const bytes &k, const bytes &v) {
        if (k == lookup) {
            Found = v.size != 0;
            Value = v;
            return false;
        }

        return true;
    }

    bytes Value;
    bool Found;
    bytes lookup;

};

class DedupKVCallback: public KVCallback {
public:

    bool Call(const bytes &k, const bytes &v) {
        if (Map.find(k) == Map.end()) {
            Map[bytes_dup(k)] = bytes_dup(v);
        }

        return true;
    }

    ~DedupKVCallback() {
        for (auto x: Map) {
            bytes_free(x.first);
            bytes_free(x.second);
        }
    }

    unordered_map<bytes, bytes, bytesHasher> Map;
};

int VisitBucketKVs(Log *log, Buffer &b, HTBucketInfo *info, KVCallback *callb);
