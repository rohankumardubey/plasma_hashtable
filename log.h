#pragma once

#include <iostream>
#include <string.h>
#include "common.h"

using namespace std;

typedef uint64_t LogOffset;
const uint64_t LOG_MAXSIZE = static_cast<uint64_t>(1024)*1024*1024*100;
const uint64_t LOG_RECLAIM_SIZE = static_cast<uint64_t>(1024)*1024*64;

const int logBlockHeaderSize = 4;

int logBlockSize(int size);

struct LogSpace{
    LogOffset Offset;
    char *Buffer;
};

class Log {
public:
    Log();

    LogSpace ReserveSpace(int size);

    void FinalizeWrite(LogSpace &s);

    bytes Read(LogOffset off, Buffer &b);

    bytes Read(LogOffset off, int n, Buffer &b, int &blkSz);

    void TrimLog(LogOffset off);

    LogOffset HeadOffset();
    LogOffset TailOffset();

private:
    char *logBuf;
    uint64_t head, tail;
    uint64_t phyHead;
};
