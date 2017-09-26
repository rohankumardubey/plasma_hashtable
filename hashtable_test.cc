#include <iostream>
#include <chrono>
#include "hashtable.h"


using namespace std;

char alnum[] = "0123456789abcdefghijklmnopqrstuvwxyz";

void randKey(char *buf, int len) {
    for (auto i=0; i< len-1; i++) {
        buf[i] = alnum[rand()%36];
    }
    buf[len-1] = '\0';
}

void testbench_hashtable() {
    auto n = 100000000;
    Buffer b;
    HashTable ht(100000, "test.data");

    std::chrono::time_point<std::chrono::system_clock> start, end;
    char *buf = new char[64];
    srand(0);

    start = std::chrono::system_clock::now();
    auto t0 = std::chrono::system_clock::now();
    for (auto i=0; i<n;i++) {
        randKey(buf, 64);
        auto key = bytes(buf, 64);
        //ht.Get(key,b);
        ht.Set(key, key);
        if (i%1000000==0) {
            ht.Stats();
            auto now = std::chrono::system_clock::now();
            std::chrono::duration<double> dur = (now-t0);
            t0 = now;
            cout<<"write throughput: "<<double(1000000)/double(dur.count())<<endl;
        }
    }

    end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed_seconds = end-start;
    cout<<"=================================================="<<endl;
    cout<<"Execution took "<<elapsed_seconds.count()<<"seconds"<<endl;
    cout<<"Avg write throughput: "<<float(n)/float(elapsed_seconds.count())<<endl;

    srand(0);
    start = std::chrono::system_clock::now();
    t0 = std::chrono::system_clock::now();
    for (auto i=0; i<n;i++) {
        randKey(buf, 64);
        auto key = bytes(buf, 64);
        auto out = ht.Get(key, b);
        if (!(out == key)) {
            cout<<"Mismatch: "<<key<<" "<<out<<endl;
        }

        if (i%1000000==0) {
            ht.Stats();
            auto now = std::chrono::system_clock::now();
            std::chrono::duration<double> dur = (now-t0);
            t0 = now;
            cout<<"read throughput: "<<double(1000000)/double(dur.count())<<endl;
        }
    }
    end = std::chrono::system_clock::now();
    elapsed_seconds = end-start;
    cout<<"=================================================="<<endl;
    cout<<"Execution took "<<elapsed_seconds.count()<<"seconds"<<endl;
    cout<<"Avg read thhroughput: "<<float(n)/float(elapsed_seconds.count())<<endl;
}

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

    testbench_hashtable();

    return 0;
}
