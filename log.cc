#include "log.h"
#include <assert.h>
#include <atomic>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <fcntl.h>

InMemoryLog::InMemoryLog() :head(0), tail(0), phyHead(0) {
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
        assert(r > 0);

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
    head = 0;
    tail = 0;
    phyHead = 0;
    phyTail = 0;

    bufSize = wbsize;
    bufOffset = 0;
    bufClosed = false;
    rc = 0;

    fd = open(filepath.c_str(), O_RDWR | O_CREAT | O_SYNC, 0755);
    assert(fd > 0);
    buf = new char[wbsize];
}

LogSpace PersistentLog::ReserveSpace(int size) {
    unique_lock<std::mutex> lock(m);

    while (true) {
        // Buffer has space, allocate
        if (bufOffset + size + logBlockHeaderSize < bufSize) {
            auto allocOffset = bufOffset;
            bufOffset += size + logBlockHeaderSize;
            rc++;
            uint32_t *blockLen = reinterpret_cast<uint32_t*>(buf+allocOffset);
            *blockLen = static_cast<uint32_t>(size);
            tail = phyTail + bufOffset;
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
    auto r = pwrite(fd, static_cast<void*>(buf), bufOffset, phyTail);
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

bytes PersistentLog::Read(LogOffset off, Buffer &b) {
    // Data is in the memory buffer
    // Perform optimistic read
    do {
        uint64_t phyOff = phyTail;
        if (off >= phyOff) {
            auto bs = b.Alloc(logBlockHeaderSize);
            memcpy(bs.data, buf+off-phyOff, bs.size);
            uint32_t blockLen = *reinterpret_cast<uint32_t*>(bs.data);

            // Block len could be invalid as we do not use any synchronization
            // Verify it before proceeding
            if (off + blockLen <= phyOff + bufSize) {
		    bs = b.Alloc(blockLen);
		    memcpy(bs.data, buf+off-phyOff+logBlockHeaderSize, blockLen);
		    if (phyOff == phyTail) {
			return bs;
		    }
           }
        }
    } while (off > phyTail);

    // Data is in the persistent log
    auto alignOff = (off / 4096)*4096;
    auto rdSize = 4096;
    if (alignOff+rdSize < off+logBlockHeaderSize) {
        rdSize += 4096;
    }

    auto buf = b.Alloc(rdSize);
    auto r = pread(fd, buf.data, buf.size, alignOff);
    assert(r >= 0);

    uint32_t *blockLen = reinterpret_cast<uint32_t*>(buf.data + off%4096);
    auto n = static_cast<int>(*blockLen);
    auto remaining = off+logBlockHeaderSize+n - alignOff+rdSize;
    if (remaining > 0) {
        buf = b.Resize(buf.size + remaining);
        auto r = pread(fd, buf.data+rdSize, remaining, alignOff+rdSize);
        assert(r >= 0);
    }

    return bytes{buf.data+off%4096+logBlockHeaderSize, n};
}


bytes PersistentLog::Read(LogOffset off, int n, Buffer &b, int &blkSz) {
    auto alignOff = (off / 4096)*4096;
    auto rdSize = 4096;
    if (alignOff+rdSize < off+logBlockHeaderSize+n) {
        rdSize += 4096;
    }

    auto buf = b.Alloc(rdSize);
    auto r = pread(fd, buf.data, buf.size, alignOff);
    assert(r >= 0);

    uint32_t *blockLen = reinterpret_cast<uint32_t*>(buf.data + off%4096);
    blkSz = static_cast<int>(*blockLen);
    return bytes{buf.data+off%4096+logBlockHeaderSize, n};
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
    return phyTail;
}

PersistentLog::~PersistentLog() {
    delete buf;
    close(fd);
}
