#ifndef MCS_H
#define MCS_H

#include <stdbool.h>

// incomplete type for passing through to _evset functions
struct mcs_func;
struct mcs_func_client;

#include "tls.h"

// TODO: remove?
typedef bool (*mcs_cfunc)(struct mcs_func *f, struct mcs_func_client *c);

enum mcs_func_state {
    mcs_fstate_disconn = 0,
    mcs_fstate_connecting,
    mcs_fstate_postconnect,
    mcs_fstate_retry,
    mcs_fstate_postretry,
    mcs_fstate_run,
    mcs_fstate_flush,
    mcs_fstate_postflush,
    mcs_fstate_read,
    mcs_fstate_postread,
    mcs_fstate_rerun,
    mcs_fstate_restart,
    mcs_fstate_syserr,
    mcs_fstate_outwait,
    mcs_fstate_sleep,
    mcs_fstate_stop,
    mcs_fstate_tls_hs,
    mcs_fstate_tls_hs_postread,
    mcs_fstate_tls_hs_postwrite,
    mcs_fstate_tls_read,
    mcs_fstate_tls_postread, // decrypt data then -> fstate_postread
    mcs_fstate_tls_flush,
    mcs_fstate_tls_postflush,
};

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
    struct mcs_tls_client *tls;
    struct mcs_func *parent; // function that owns this client.
    void *mcmc; // mcmc client object
    struct mcs_buf wb;
    struct mcs_buf rb;
    enum mcs_func_state s_read;
    enum mcs_func_state s_flush;
    bool connected;
};

void mcs_buf_init(struct mcs_buf *b, size_t size);

// TODO: wrong, but trying to save some time.
// need to end up with a libevent-like thing that wraps syscalls and gives you
// callbacks, but I don't have epoll implemented to compare with and need to
// refactor all of the uring code to get a proper abstract interface.
//
// In the meantime we do it halfway: let the TLS module swap around callbacks
// and call into our uring code a little bit.
void _evset_wrflush_data(struct mcs_func *f, struct mcs_func_client *c, char *data, long len);
void _evset_read_data(struct mcs_func *f, struct mcs_func_client *c, char *data, long len);

// return -1 if failure to get sqe
typedef int (*queue_cb)(void *udata);
int mcs_func_cqe_res(struct mcs_func *f);
void mcs_func_set_cqe_res(struct mcs_func *f, int res);
struct mcs_func_client *mcs_func_get_client(struct mcs_func *f);
void mcs_func_set_state(struct mcs_func *f, enum mcs_func_state s);
bool mcs_cm_postconnect(struct mcs_func *f, struct mcs_func_client *c);

#endif // MCS_H
