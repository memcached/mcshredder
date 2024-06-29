PREFIX=/usr/local

all:
		cd vendor/ && ${MAKE}
		#gcc -g -O2 -pthread -Ivendor/lua/src -Ivendor/liburing/src/include -Wall -pedantic -o mcshredder mcshredder.c pcg-basic.c itoa_ljust.c $(LDFLAGS)
		# sometimes helpful to add -fno-omit-frame-pointer for perf tracing
		gcc -g -O2 -pthread -rdynamic -Ivendor/lua/src -Ivendor/liburing/src/include -Wall -pedantic -o mcshredder mcshredder.c itoa_ljust.c vendor/mcmc/mcmc.o vendor/lua/src/liblua.a vendor/liburing/src/liburing.a -lm $(LDFLAGS) -ldl

tls:
		cd vendor/ && ${MAKE}
		# sometimes helpful to add -fno-omit-frame-pointer for perf tracing
		gcc -g -O2 -pthread -rdynamic -Ivendor/lua/src -Ivendor/liburing/src/include -Wall -pedantic -o mcshredder mcshredder.c tls.c itoa_ljust.c vendor/mcmc/mcmc.o vendor/lua/src/liblua.a vendor/liburing/src/liburing.a -lcrypto -lssl -lm $(LDFLAGS) -ldl -DUSE_TLS=1

