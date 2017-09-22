CC = g++ -std=c++11 -O2 -g

log_test:
	 $(CC) -o $@ log.cc log_test.cc common.cc

clean:
	rm -f log_test
