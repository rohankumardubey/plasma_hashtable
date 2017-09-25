#pragma once

#include <iostream>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <string.h>
#include "common.h"

using namespace std;

typedef uint64_t LogOffset;
const uint64_t LOG_MAXSIZE = static_cast<uint64_t>(1024)*1024*1024*100;
const uint64_t LOG_RECLAIM_SIZE = static_cast<uint64_t>(1024)*1024*64;
const uint64_t LOG_BEGIN_OFFSET = 4096;

const int logBlockHeaderSize = 4;

int logBlockSize(int size);

struct LogSpace{
    LogOffset Offset;
    char *Buffer;
};

class Log {
public:
    virtual ~Log() {}

    virtual LogSpace ReserveSpace(int size) = 0;

    virtual void FinalizeWrite(LogSpace &s) = 0;

    virtual bytes Read(LogOffset off, Buffer &b) = 0;

    virtual bytes Read(LogOffset off, int n, Buffer &b, int &blockLen) = 0;

    virtual void TrimLog(LogOffset off) = 0;

    virtual LogOffset HeadOffset() = 0;

    virtual LogOffset TailOffset() = 0;
};

class InMemoryLog: public Log {
public:
    InMemoryLog();

    ~InMemoryLog();

    LogSpace ReserveSpace(int size);

    void FinalizeWrite(LogSpace &s);

    bytes Read(LogOffset off, Buffer &b);

    bytes Read(LogOffset off, int n, Buffer &b, int &blockLen);

    void TrimLog(LogOffset off);

    LogOffset HeadOffset();

    LogOffset TailOffset();
private:
    char *logBuf;
    uint64_t head, tail;
    uint64_t phyHead;
};

class PersistentLog: public Log {
public:
    PersistentLog(string filepath, int wbsize);

    ~PersistentLog();

    LogSpace ReserveSpace(int size);

    void FinalizeWrite(LogSpace &s);

    bytes Read(LogOffset off, Buffer &b);

    bytes Read(LogOffset off, int n, Buffer &b, int &blkSz);

    void TrimLog(LogOffset off);

    LogOffset HeadOffset();

    LogOffset TailOffset();

private:
    void writeBuf();

    int fd;
    char *buf;
    int bufSize;
    uint64_t bufOffset;

    atomic<uint64_t> head, tail;
    atomic<uint64_t> phyHead, phyTail;

    mutex m;
    condition_variable cond;
    bool bufClosed;
    int rc;
};
