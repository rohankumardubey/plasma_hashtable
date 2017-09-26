#include "log.h"
#include <assert.h>
#include <atomic>
#include <sys/mman.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>

InMemoryLog::InMemoryLog() :head(LOG_BEGIN_OFFSET), tail(LOG_BEGIN_OFFSET), phyHead(LOG_BEGIN_OFFSET) {
    logBuf = static_cast<char *>(mmap(0, LOG_MAXSIZE, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0));
}

LogSpace InMemoryLog::ReserveSpace(int size) {
    auto r = LogSpace{tail, logBuf+tail+logBlockHeaderSize};
    uint32_t *blockLen = reinterpret_cast<uint32_t*>(logBuf+tail);
    *blockLen = static_cast<uint32_t>(size);
    tail += size + logBlockHeaderSize;
    return r;
}

void InMemoryLog::FinalizeWrite(LogSpace &s) {
}

bytes InMemoryLog::Read(LogOffset off, Buffer &b) {
    int n = static_cast<int>(*reinterpret_cast<uint32_t*>(logBuf + off));
    auto buf = b.Alloc(n);
    memcpy(buf.data, logBuf + off + logBlockHeaderSize, n);
    return buf;
}

bytes InMemoryLog::Read(LogOffset off, int n, Buffer &b, int &blkSz) {
    blkSz = static_cast<int>(*reinterpret_cast<uint32_t*>(logBuf + off));
    auto buf = b.Alloc(n);
    memcpy(buf.data, logBuf + off + logBlockHeaderSize, n);
    return buf;
}

LogOffset InMemoryLog::HeadOffset() {
    return head;
}

LogOffset InMemoryLog::TailOffset() {
    return tail;
}

void InMemoryLog::TrimLog(LogOffset off) {
    head = off;
    auto diff = (head - phyHead)/LOG_RECLAIM_SIZE;
    if (diff) {
        auto n = diff*LOG_RECLAIM_SIZE;
        auto r = madvise(logBuf+phyHead, n, MADV_DONTNEED);
        assert(r == 0);

        phyHead += n;
    }
}

InMemoryLog::~InMemoryLog() {
    munmap(logBuf, LOG_MAXSIZE);
}

int logBlockSize(int size) {
    return size+logBlockHeaderSize;
}

PersistentLog::PersistentLog(string filepath, int wbsize) {
    head = LOG_BEGIN_OFFSET;
    tail = LOG_BEGIN_OFFSET;
    phyHead = LOG_BEGIN_OFFSET;
    phyTail = LOG_BEGIN_OFFSET;

    bufSize = wbsize;
    bufOffset = 0;
    bufClosed = false;
    rc = 0;
    int flags = O_RDWR | O_CREAT | O_SYNC;

#ifdef __linux__
    flags |= O_DIRECT;
#endif

    fd = open(filepath.c_str(), flags, 0755);
    assert(fd > 0);
    auto r = posix_memalign(reinterpret_cast<void **>(&buf), ALIGN_SIZE, bufSize);
    assert(r == 0);
}

LogSpace PersistentLog::ReserveSpace(int size) {
    unique_lock<std::mutex> lock(m);

    tail += logBlockHeaderSize + size;
    while (true) {
        auto woffset = bufOffset + size + logBlockHeaderSize;
        // Buffer has space, allocate
        if (woffset <= bufSize-logBlockHeaderSize ||  woffset == bufSize) {
            auto allocOffset = bufOffset;
            bufOffset += size + logBlockHeaderSize;
            rc++;
            int32_t *blockLen = reinterpret_cast<int32_t*>(buf+allocOffset);
            *blockLen = static_cast<int32_t>(size);
            return LogSpace{phyTail+allocOffset, buf+allocOffset+logBlockHeaderSize};
        // Write out the buffer if it has no users
        } else if (rc == 0) {
            writeBuf();
        // Wait for the writer to release the buffer
        } else {
                bufClosed = true;
                cond.wait(lock);
        }
    }
}

void PersistentLog::writeBuf() {
    if (bufOffset < bufSize) {
            int32_t *blockLen = reinterpret_cast<int32_t*>(buf+bufOffset);
            *blockLen = -static_cast<int32_t>(bufSize-bufOffset-logBlockHeaderSize);
            bufOffset += bufSize-bufOffset;
    }

    auto r = pwrite(fd, static_cast<void*>(buf), bufSize, phyTail);
    assert(r > 0);
    phyTail += bufOffset;
    bufOffset = 0;
    bufClosed = false;
}

void PersistentLog::FinalizeWrite(LogSpace &s) {
    unique_lock<std::mutex> lock(m);

    rc--;
    if (rc==0 && bufClosed) {
        writeBuf();
    }
}

bytes PersistentLog::Read(LogOffset off, int n, Buffer &b, int &blockLen) {
    // Data is in the memory buffer
    // Perform optimistic read
    do {
        uint64_t phyOff = phyTail;
        if (off >= phyOff) {
            auto bs = b.Alloc(logBlockHeaderSize);
            memcpy(bs.data, buf+off-phyOff, bs.size);
            blockLen = static_cast<int>(*reinterpret_cast<int32_t*>(bs.data));

            if (!n) {
                n = blockLen;
            }

            // Block len could be invalid as we do not use any synchronization
            // Verify it before proceeding
            if (off + n <= phyOff + bufSize) {
                bs = b.Alloc(n);
                memcpy(bs.data, buf+off-phyOff+logBlockHeaderSize, n);
                if (phyOff == phyTail) {
                    return bs;
                }
           }
        }
    } while (off > phyTail);

    // Data is in the persistent log
    auto alignOff = (off / 4096)*4096;
    auto rdSize = 4096;
    while (alignOff+rdSize < off+logBlockHeaderSize+n) {
        rdSize += 4096;
    }

    auto buf = b.Alloc(rdSize);
    auto r = pread(fd, buf.data, buf.size, alignOff);
    assert(r >= 0);

    blockLen = static_cast<int>(*reinterpret_cast<int32_t*>(buf.data + off%4096));

    // Padding block, ignore it
    if (blockLen < 0) {
        return bytes{nullptr, 0};
    }

    if (!n) {
        n = blockLen;
    }

    auto remaining = off+logBlockHeaderSize+n - alignOff+rdSize;
    if (remaining > 0) {
        if (remaining % 4096) {
            remaining = 4096*(remaining/4096) + 4096;
        }
        buf = b.Resize(buf.size + remaining);
        auto r = pread(fd, buf.data+rdSize, remaining, alignOff+rdSize);
        assert(r >= 0);
    }

    return bytes{buf.data+off%4096+logBlockHeaderSize, n};
}

bytes PersistentLog::Read(LogOffset off, Buffer &b) {
    int _;
    return Read(off, 0, b, _);
}

void PersistentLog::TrimLog(LogOffset off) {
    head = off;
    auto diff = (head - phyHead)/LOG_RECLAIM_SIZE;
    if (diff) {
        auto n = diff*LOG_RECLAIM_SIZE;
#ifdef __linux__
        auto r = fallocate(fd, FALLOC_FL_PUNCH_HOLE|FALLOC_FL_KEEP_SIZE, phyHead, n);
        assert(r == 0);
#endif

        phyHead += n;
    }
}

LogOffset PersistentLog::HeadOffset(){
    return head;

}

LogOffset PersistentLog::TailOffset() {
    return tail;
}

PersistentLog::~PersistentLog() {
    free(buf);
    close(fd);
}
