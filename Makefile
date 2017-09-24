CC = g++ -std=c++11 -O2 -g

all: hashtable_test log_test

log_test:
	 $(CC) -o $@ log.cc log_test.cc common.cc

hashtable_test:
	 $(CC) -o $@ hashtable_test.cc hashtable.cc log.cc common.cc murmurhash3.cc

clean:
	rm -f log_test hashtable_test
