#include "hashtable.h"
#include <vector>

HashTable::HashTable(int nb, const string &filepath) :DataSize(0) {
    numBuckets = nb;
    maxSegments = 10000;
    numHashes = 3; // 5 bytes filter, 15 % false positives
    bucketDir = new HTBucketInfo[nb];
    log = new Log();
    auto space = log->ReserveSpace(4096);
    log->FinalizeWrite(space);
}

bytes HashTable::Get(const bytes &key, Buffer &b) {
    auto h = hash(key);
    auto bInfo = &bucketDir[h % numBuckets];

#ifdef USE_BLOOMFILTER
    BloomFilter bloom(static_cast<void *>(&bInfo->bloom), 5, numHashes);

    if (!bloom.Test(key)) {
        return bytes();
    }
#endif

    LookupKVCallback cb(key);

    VisitBucketKVs(log, b, bInfo, &cb);
    if (cb.Found) {
        return cb.Value;
    }

    return bytes();
}

int copyKV(char *buf, int offset, const bytes &k, const bytes &v) {
    uint16_t kl = (uint16_t) k.size;
    uint32_t vl = (uint32_t) v.size;

    memcpy(buf+offset, &kl, keyLenSize);
    offset += keyLenSize;

    memcpy(buf+offset, k.data, k.size);
    offset += kl;

    memcpy(buf+offset, &vl,valLenSize);
    offset += valLenSize;

    memcpy(buf+offset, v.data, v.size);
    offset += vl;
    return offset;
}

void HashTable::Delete(const bytes &key) {
    Set(key, deleteValue);
}

void HashTable::Set(const bytes &key, const bytes &value){
    auto h = hash(key);
    auto id = h % numBuckets;
    auto bInfo = &bucketDir[id];

    compactLog(30, wBuf);
    kvs.clear();
    kvs.push_back(kv{key,value});
    writeHTData(id, bInfo, kvs, maxSegments);
}


void HashTable::writeHTData(int id, HTBucketInfo *bInfo, vector<kv> &kvs, int maxSegments) {
    DedupKVCallback cb;
    HTBucketInfo head = *bInfo;

#ifdef USE_BLOOMFILTER
    BloomFilter bloom(static_cast<void *>(&bInfo->bloom), 4, numHashes);
#endif

    if (bInfo->segments > maxSegments) {
#ifdef USE_BLOOMFILTER
        bloom.Reset();
#endif
        head = HTBucketInfo();
        head.offset = 0;
        head.version = bInfo->version+1;

        DataSize -= VisitBucketKVs(log, wBuf, bInfo, &cb);
        for (auto x: cb.Map) {
            if (x.second.size > 0) {
               kvs.push_back(kv{x.first,x.second});
            }
        }
    }

    HTData header {(uint32_t)id, head.version, head.offset};
    auto headerSize = sizeof(header);
    auto size = headerSize;

    for (auto x: kvs) {
        size += keyLenSize +valLenSize;
        size += x.k.size+ x.v.size;
    }

    auto space = log->ReserveSpace(size);

    auto offset = 0;
    memcpy(space.Buffer+offset, &header, headerSize);
    offset += headerSize;

    for (auto x: kvs) {
        offset = copyKV(space.Buffer, offset, x.k, x.v);
#ifdef USE_BLOOMFILTER
        bloom.Add(x.k);
#endif
    }

    log->FinalizeWrite(space);
    DataSize += logBlockSize(size);

    bInfo->offset = space.Offset;
    bInfo->segments = head.segments+1;
    bInfo->count = head.count + kvs.size();
    bInfo->version = head.version;
}



int VisitBucketKVs(Log *log, Buffer &b, HTBucketInfo *info, KVCallback *callb) {
    int readBytes = 0;

    LogOffset logOff = info->offset;
    while (logOff) {
        auto block = log->Read(logOff, b);
        readBytes += logBlockSize(block.size);
        logOff = (*(HTData*)(block.data)).nextOffset;
        for (auto off = sizeof(HTData); off<block.size; ) {
            uint16_t kl = *(uint16_t*)(block.data+off);
            off += keyLenSize;

            auto k = bytes(block.data+off, kl);
            off += kl;

            uint32_t vl = *(uint32_t*)(block.data+off);
            off += valLenSize;

            auto v = bytes(block.data+off, vl);
            off += vl;

            if (!callb->Call(k,v)) {
                return readBytes;
            }
        }
    }

    return readBytes;
}

void HashTable::Dump() {
    PrintKVCallback cb;
    Buffer b;
    VisitBucketKVs(log, b, &bucketDir[0], &cb);
}

void HashTable::Stats() {
    cout<<"Fragmentation: "<<GetLogFragmentation()<<endl;
    /*
    for (auto i=0;i <numBuckets; i++) {
        cout<<"Bucket "<<i<<"-"<<int(bucketDir[i].count)<<endl;
    }
    */
}

float HashTable::GetLogFragmentation() {
    auto logSize = log->TailOffset() - log->HeadOffset();
    //cout<<"logSize :"<<logSize<<" dataSize :"<<DataSize<<endl;
    auto wasted = logSize-DataSize;
    return float(wasted*100)/float(logSize);
}

void HashTable::compactLog(float fragThreshold, Buffer &b) {
    int n;
    auto offset = log->HeadOffset();
    while (GetLogFragmentation() > fragThreshold) {
        auto block = log->Read(offset, sizeof(HTData), b, n);
        HTData *header = (HTData*)(block.data);
        if (!header->nextOffset) {
            auto bInfo = &bucketDir[header->bucketID];
            if (bInfo->version == header->version) {
                kvs.clear();
                writeHTData(header->bucketID, bInfo, kvs, -1);
            }
        }

        offset += logBlockSize(n);
        log->TrimLog(offset);
    }
}
