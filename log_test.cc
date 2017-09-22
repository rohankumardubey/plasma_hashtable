#include <iostream>
#include <vector>
#include "log.h"

using namespace std;

void test_log_write_read(){
    Log log;
    Buffer b;
    char buf[100];
    vector<LogOffset> off;

    for (auto i=0; i< 1000; i++) {
        auto n = sprintf(buf, "%d", i);
        auto space = log.ReserveSpace(n);
        memcpy(space.Buffer, buf, n);
        log.FinalizeWrite(space);
        off.push_back(space.Offset);
    }

    for (auto i=0; i< 1000; i++) {
        auto n = sprintf(buf, "%d", i);
        auto expected = bytes(buf, n);
        auto got = log.Read(off[i], b);
        if (!(expected == got)) {
            cout<<"expected: "<<expected<<" "<<"got: "<<got<<endl;
        }
    }
}

int main() {
    test_log_write_read();

    return 0;
}
