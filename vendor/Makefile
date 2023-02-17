all:
	cd lua && $(MAKE) all CFLAGS="-g -O2" && cd ..
	cd mcmc && $(MAKE) all && cd ..
	cd liburing && $(MAKE) && cd ..

clean:
	cd lua && $(MAKE) clean && cd ..
	cd mcmc && $(MAKE) clean && cd ..
	cd liburing && $(MAKE) clean && cd ..

install: ;

dist: clean
distdir: clean
