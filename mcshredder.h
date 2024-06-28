#ifndef MCS_H
#define MCS_H

struct mcs_buf {
    int used; // total valid data filled
    int offset; // data sent/read
    int toconsume; // marker for consuming parsed data
    size_t size; // length of *data
    char *data;
};

struct mcs_lock_buf {
    pthread_mutex_t lock;
    struct mcs_buf buf;
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
    struct mcs_buf wb;
    struct mcs_buf rb;
    bool connected;
};

#endif // MCS_H
