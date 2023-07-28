PREFIX=/usr/local

all:
		cd vendor/ && ${MAKE}
		#gcc -g -O2 -pthread -Ivendor/lua/src -Ivendor/liburing/src/include -Wall -pedantic -o mcshredder mcshredder.c pcg-basic.c itoa_ljust.c $(LDFLAGS)
		# sometimes helpful to add -fno-omit-frame-pointer for perf tracing
		gcc -g -O2 -pthread -Ivendor/lua/src -Ivendor/liburing/src/include -Wall -pedantic -o mcshredder mcshredder.c itoa_ljust.c vendor/mcmc/mcmc.o vendor/lua/src/liblua.a vendor/liburing/src/liburing.a -lm $(LDFLAGS) -ldl
