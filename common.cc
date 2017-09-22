#include "common.h"
#include <iostream>

using namespace std;

ostream & operator << (ostream &out, const bytes &s){
    out << string(s.data, s.size);
    return out;
}

bytes bytes_dup(const bytes &src) {
    bytes dst{new char[src.size], src.size};
    memcpy(dst.data, src.data, src.size);
    return dst;
}

void bytes_free(const bytes &s) {
    delete [] s.data;
}

