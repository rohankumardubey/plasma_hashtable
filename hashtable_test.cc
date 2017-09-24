#include <iostream>
#include <chrono>
#include "hashtable.h"


using namespace std;

void test_set_get(Buffer &b) {
    char kbuf[100], vbuf[100];
    auto n = 10000;
    HashTable ht(10, "test");
    for (auto i=0; i<n; i++) {
        auto nk = sprintf(kbuf, "key-%d", i);
        auto nv = sprintf(vbuf, "val-%d", i);
        bytes kbs(kbuf, nk);
        bytes vbs(vbuf, nv);
        ht.Set(kbs, vbs);
    }

    for (auto i=0; i<n; i++) {
        auto nk = sprintf(kbuf, "key-%d", i);
        auto nv = sprintf(vbuf, "val-%d", i);
        bytes kbs(kbuf, nk);
        bytes vbs(vbuf, nv);
        auto out = ht.Get(kbs, b);
        if (!(out == vbs)) {
            cout<<vbs<<" != "<<out<<endl;
        }
    }
}

int main() {
    Buffer b;
    test_set_get(b);

    return 0;
}
