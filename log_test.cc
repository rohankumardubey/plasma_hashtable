#include <iostream>
#include <vector>
#include <assert.h>
#include "log.h"

using namespace std;

void test_log_write_read(bool inmemory){
    Log *log;

    if (inmemory) {
        log = new InMemoryLog();
    } else {
        log = new PersistentLog("test.data", 1024);
    }

    Buffer b;
    char buf[1000];
    vector<LogOffset> off;
    auto numItems = 100000;

    for (auto i=0; i< numItems; i++) {
        auto n = sprintf(buf, "%d", i);
        auto space = log->ReserveSpace(n);
        memcpy(space.Buffer, buf, n);
        log->FinalizeWrite(space);
        off.push_back(space.Offset);

        auto expected = bytes(buf, n);
        auto got = log->Read(off[i], b);
        assert(expected == got);
        if (!(expected == got)) {
            cout<<"expected: "<<expected<<" "<<"got: "<<got<<endl;
        }
    }

    for (auto i=0; i< numItems; i++) {
        auto n = sprintf(buf, "%d", i);
        auto expected = bytes(buf, n);
        auto got = log->Read(off[i], b);
        if (!(expected == got)) {
            cout<<"expected: "<<expected<<" "<<"got: "<<got<<endl;
        }
    }

    delete log;
}

int main() {
    test_log_write_read(true);
    test_log_write_read(false);

    return 0;
}
