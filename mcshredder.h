#ifndef MCS_H
#define MCS_H

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <errno.h>
#include <pthread.h>
#include <unistd.h>
#include <getopt.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <stdbool.h>

#define USE_TLS 1

#ifdef USE_TLS
#include "tls.h"
#endif

// Should be able to accommodate the rbuf/wbuf used in clients.
// Not replacing them as of this writing. Should be separate change.
struct mcs_buf {
    int used;
    int sent;
    int toconsume;
    size_t size;
    char *buf;
    pthread_mutex_t lock;
};

struct mcs_conn {
    // host info
    char host[NI_MAXHOST];
    char port_num[NI_MAXSERV];
    // event detail
    int fd;
};

// client object for custom funcs
struct mcs_func_client {
    struct mcs_conn conn;
#ifdef USE_TLS
    struct mcs_tls_client tls;
#endif
    void *mcmc; // mcmc client object
    char *wbuf;
    size_t wbuf_size; // total size of buffer
    int wbuf_used;
    int wbuf_sent;
    char *rbuf;
    size_t rbuf_size;
    int rbuf_used;
    int rbuf_toconsume; // how far to skip rbuf on next read.
    bool connected;
};

#endif // MCS_H
