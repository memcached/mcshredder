/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  mc-shredder - burn-in load test client
 *
 *       https://github.com/memcached/mcshredder
 *
 *  Copyright 2023 Cache Forge LLC.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      dormando <dormando@rydia.net>
 */

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

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <liburing.h>
#include <poll.h> // POLLOUT for liburing.

#include "vendor/mcmc/mcmc.h"
#include "queue.h"
#include "itoa_ljust.h"

#define PRING_QUEUE_SQ_ENTRIES 1024
#define PRING_QUEUE_CQ_ENTRIES 4096
// avoiding some hacks for finding member size.
#define SOCK_MAX 100

#define WBUF_INITIAL_SIZE 16384
#define RBUF_INITIAL_SIZE 65536

#define KEY_MAX_LENGTH 250
#define REQ_MAX_LENGTH KEY_MAX_LENGTH * 2

#define NSEC_PER_SEC 1000000000

char sock_path_default[SOCK_MAX];

// TODO: This is a global timeout just to get code started.
// Note that timeouts must be stable until the sqe is submitted, so any
// timeouts have to exist on the func structure.
struct __kernel_timespec timeout_default = { .tv_sec = 0, .tv_nsec = 500000000 };
// time to wait before attempting to reconnect after an error.
struct __kernel_timespec timeout_retry = { .tv_sec = 0, .tv_nsec = 500000000 };
struct mcs_thread;
struct mcs_func;
struct mcs_ctx;

static void register_lua_libs(lua_State *L);
static void mcs_queue_cb(void *udata, struct io_uring_cqe *cqe);
static int mcs_func_lua(struct mcs_func *f);
static void mcs_start_limiter(struct mcs_func *f);

typedef void (*event_cb)(void *udata, struct io_uring_cqe *cqe);
// return -1 if failure to get sqe
typedef int (*queue_cb)(void *udata);
struct mcs_event {
    void *udata;
    event_cb cb;
    queue_cb qcb;
};

struct mcs_conn {
    // host info
    char host[NI_MAXHOST];
    char port_num[NI_MAXSERV];
    // event detail
    int fd;
};

struct mcs_f_rate {
    int rate;
    uint64_t period; // stored in nanoseconds.
    struct __kernel_timespec delta; // period / rate in timespec format
    struct __kernel_timespec next; // next absolute time to schedule the alarm for
};

// governs when to naturally reconnect
struct mcs_f_reconn {
    unsigned int every; // how often to reconnect
    unsigned int after; // counter until next reconnect
};

// TODO: func or macro for state changes so can be printed.
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
    mcs_fstate_stop,
};

enum mcs_lua_yield {
    mcs_luayield_write = 0,
    mcs_luayield_flush,
    mcs_luayield_read,
};

struct mcs_func_req {
    struct timespec start;
    int len;
    int vlen;
    char data[];
};

// points into func's rbuf
struct mcs_func_resp {
    int status;
    char *buf; // start of response buffer
    mcmc_resp_t resp;
};

struct mcs_func {
    lua_State *L; // lua coroutine local to this function
    int self_ref; // avoid garbage collection
    int self_ref_coro; // reference for the coroutine thread
    STAILQ_ENTRY(mcs_func) next; // coroutine stack
    struct mcs_thread *parent; // pointer back to owner thread
    char *fname; // name of function to call
    bool linked;
    enum mcs_func_state state;
    struct mcs_f_rate rate; // rate limiter
    struct mcs_f_reconn reconn; // tcp reconnector
    struct mcs_event ev;
    struct mcs_conn conn;
    int limit; // stop running after N loops
    void *mcmc; // mcmc client object
    int cqe_res; // result of most recent cqe
    int lua_nargs; // number of args to pass back to lua
    char *wbuf;
    size_t wbuf_size; // total size of buffer
    int wbuf_used;
    int wbuf_sent;
    char *rbuf;
    size_t rbuf_size;
    int rbuf_used;
    int rbuf_toconsume; // how far to skip rbuf on next read.
    int reserr; // probably -ERRNO via uring.
};

typedef STAILQ_HEAD(func_head_s, mcs_func) func_head_t;
struct mcs_thread {
    struct mcs_ctx *ctx;
    lua_State *L; // lua VM local to this thread
    STAILQ_ENTRY(mcs_thread) next; // thread stack
    func_head_t funcs; // coroutine stack
    int active_funcs; // stop if no active functions
    struct io_uring ring;
    pthread_t tid;
    bool stop;
};

typedef STAILQ_HEAD(thread_head_s, mcs_thread) thread_head_t;
struct mcs_ctx {
    lua_State *L;
    thread_head_t threads; // stack of threads
    pthread_cond_t wait_cond; // thread completion signal
    pthread_mutex_t wait_lock;
    int active_threads; // return from shredder() if threads stopped
    const char *conffile;
    struct mcs_conn conn; // connection details.
};

// adds ts2 to ts1
static void timespec_add(struct __kernel_timespec *ts1,
        struct __kernel_timespec *ts2) {
    ts1->tv_sec += ts2->tv_sec;
    ts1->tv_nsec += ts2->tv_nsec;
    if (ts1->tv_nsec >= NSEC_PER_SEC) {
        ts1->tv_sec++;
        ts1->tv_nsec -= NSEC_PER_SEC;
    }
}

// Common lua debug command.
__attribute__((unused)) void dump_stack(lua_State *L) {
    int top = lua_gettop(L);
    int i = 1;
    fprintf(stderr, "--TOP OF STACK [%d]\n", top);
    for (; i < top + 1; i++) {
        int type = lua_type(L, i);
        // lets find the metatable of this userdata to identify it.
        if (lua_getmetatable(L, i) != 0) {
            lua_pushstring(L, "__name");
            if (lua_rawget(L, -2) != LUA_TNIL) {
                fprintf(stderr, "--|%d| [%s] (%s)\n", i, lua_typename(L, type), lua_tostring(L, -1));
                lua_pop(L, 2);
                continue;
            }
            lua_pop(L, 2);
        }
        if (type == LUA_TSTRING) {
            fprintf(stderr, "--|%d| [%s] | %s\n", i, lua_typename(L, type), lua_tostring(L, i));
        } else {
            fprintf(stderr, "--|%d| [%s]\n", i, lua_typename(L, type));
        }
    }
    fprintf(stderr, "-----------------\n");
}

// *** IO_URING ***

static void init_thread_uring(struct mcs_thread *t) {
    struct io_uring_params p = {0};
    p.flags = IORING_SETUP_CQSIZE;
    p.cq_entries = PRING_QUEUE_CQ_ENTRIES;
    int ret = io_uring_queue_init_params(PRING_QUEUE_SQ_ENTRIES, &t->ring, &p);
    if (ret) {
        perror("io_uring_queue_init_params");
        exit(1);
    }
    if (!(p.features & IORING_FEAT_NODROP)) {
        fprintf(stderr, "uring: kernel missing IORING_FEAT_NODROP\n");
        exit(EXIT_FAILURE);
    }
    if (!(p.features & IORING_FEAT_SINGLE_MMAP)) {
        fprintf(stderr, "uring: kernel missing IORING_FEAT_SINGLE_MMAP\n");
        exit(EXIT_FAILURE);
    }
    if (!(p.features & IORING_FEAT_FAST_POLL)) {
        fprintf(stderr, "uring: kernel missing IORING_FEAT_FAST_POLL\n");
        exit(EXIT_FAILURE);
    }
}

// NOTE: Don't believe we need handlers on timeouts, as the linked SQE will
// return with an abort failure.
// TODO: timeout override.
static int _evset_link_timeout(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    if (sqe == NULL) {
        return -1;
    }
    io_uring_prep_link_timeout(sqe, &timeout_default, 0);
    io_uring_sqe_set_data(sqe, NULL);

    return 0;
}

static int _evset_abs_timeout(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    if (sqe == NULL) {
        return -1;
    }
    io_uring_prep_timeout(sqe, &f->rate.next, 0, IORING_TIMEOUT_ABS);
    io_uring_sqe_set_data(sqe, &f->ev);

    return 0;
}

static int _evset_retry_timeout(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    if (sqe == NULL) {
        return -1;
    }
    io_uring_prep_timeout(sqe, &timeout_retry, 0, 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    return 0;
}

static int _evset_wrpoll(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    if (sqe == NULL) {
        return -1;
    }

    io_uring_prep_poll_add(sqe, mcmc_fd(f->mcmc), POLLOUT);
    io_uring_sqe_set_data(sqe, &f->ev);

    sqe->flags |= IOSQE_IO_LINK;

    // couldn't link our timeout, need to give up on this sqe.
    // should be an extremely rare event.
    if (_evset_link_timeout(f) != 0) {
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, NULL);
        sqe->flags = 0;

        return -1;
    }

    return 0;
}

static int _evset_nop(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    if (sqe == NULL) {
        return -1;
    }
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, &f->ev);

    return 0;
}

static int _evset_wrflush(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    if (sqe == NULL) {
        return -1;
    }

    io_uring_prep_write(sqe, mcmc_fd(f->mcmc), f->wbuf + f->wbuf_sent, f->wbuf_used - f->wbuf_sent, 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    if (_evset_link_timeout(f) != 0) {
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, NULL);
        sqe->flags = 0;

        return -1;
    }

    return 0;
}

static int _evset_read(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    if (sqe == NULL) {
        return -1;
    }

    io_uring_prep_recv(sqe, mcmc_fd(f->mcmc), f->rbuf + f->rbuf_used, f->rbuf_size - f->rbuf_used, 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    if (_evset_link_timeout(f) != 0) {
        io_uring_prep_nop(sqe);
        io_uring_sqe_set_data(sqe, NULL);
        sqe->flags = 0;

        return -1;
    }

    return 0;
}

// *** CORE ***

static void mcs_expand_rbuf(struct mcs_func *f) {
    if (f->rbuf_used == f->rbuf_size) {
        f->rbuf_size *= 2;
        char *nrb = realloc(f->rbuf, f->rbuf_size);
        if (nrb == NULL) {
            fprintf(stderr, "Failed to realloc read buffer\n");
            abort();
        }
        f->rbuf = nrb;
    }
}

// yes this should be a "buf" abstraction
static void mcs_expand_wbuf(struct mcs_func *f, size_t len) {
    while (f->wbuf_used + len > f->wbuf_size) {
        f->wbuf_size *= 2;
    }
    char *nwb = realloc(f->wbuf, f->wbuf_size);
    if (nwb == NULL) {
        fprintf(stderr, "Failed to realloc write buffer\n");
        abort();
    }
    f->wbuf = nwb;
}

// The connect routine isn't very "io_uring-y", as it calls
// socket()/connect() from here, but considering we're calling connect in
// nonblock mode I'm not sure if there's any real difference in pushing it
// over uring.
static int mcs_connect(struct mcs_func *f) {
    int status = mcmc_connect(f->mcmc, f->conn.host, f->conn.port_num, MCMC_OPTION_NONBLOCK);
    if (status == MCMC_CONNECTED) {
        // NOTE: find when this is possible?
        abort();
    } else if (status == MCMC_CONNECTING) {
        // need to wait for a writeable event.
        f->state = mcs_fstate_connecting;
        return 0;
    } else {
        // FIXME: use real error flow once it exists
        fprintf(stderr, "failed to connect: %s:%s\n", f->conn.host, f->conn.port_num);
        return -1;
    }
    return 0;
}

void mcs_postflush(struct mcs_func *f) {
    int res = f->cqe_res;

    if (res > 0) {
        f->wbuf_sent += res;
        if (f->wbuf_sent < f->wbuf_used) {
            // need to continue flushing write buffer.
            f->state = mcs_fstate_flush;
        } else {
            f->wbuf_sent = 0;
            f->wbuf_used = 0;
            f->state = mcs_fstate_run;
        }
    } else if (res < 0) {
        if (res == -EAGAIN || res == -EWOULDBLOCK) {
            // TODO: -> wrpoll -> flush
            // is this even possible with uring?
            abort();
        } else {
            f->reserr = res;
            f->state = mcs_fstate_syserr;
        }
    } else if (res == 0) {
        // disconnected, but probably gracefully
        f->reserr = 0;
        f->state = mcs_fstate_syserr;
    }
}

// Note: this function throws away the response object if it needs to read
// more data from the socket, re-parsing after another read attempt.
// This should be an extremely rare case, and parsing is fast enough that I
// don't want to add more logic around this right now.
static int mcs_read_buf(struct mcs_func *f) {
    // optimistically allocate a response to minimize data copying.
    struct mcs_func_resp *r = lua_newuserdatauv(f->L, sizeof(struct mcs_func_resp), 0);
    memset(r, 0, sizeof(*r));
    r->status = mcmc_parse_buf(f->mcmc, f->rbuf, f->rbuf_used, &r->resp);
    if (r->status == MCMC_OK) {
        if (r->resp.vlen != r->resp.vlen_read) {
            lua_pop(f->L, 1); // throw away the resp object, try re-parsing later
            mcs_expand_rbuf(f);
            f->state = mcs_fstate_read;
        } else {
            r->buf = f->rbuf;
            f->state = mcs_fstate_run;
            f->lua_nargs = 1;
            f->rbuf_toconsume = r->resp.reslen + r->resp.vlen_read;
        }
    } else if (r->resp.code == MCMC_WANT_READ) {
        lua_pop(f->L, 1);
        mcs_expand_rbuf(f);
        f->state = mcs_fstate_read;
    } else {
        // TODO: real error.
        fprintf(stderr, "Buffer read failed with bad response, exiting: %s\n", f->rbuf);
        abort();
    }

    return 0;
}

static void mcs_postread(struct mcs_func *f) {
    int res = f->cqe_res;

    if (res > 0) {
        f->rbuf_used += res;
        mcs_read_buf(f);
    } else if (res < 0) {
        if (res == -EAGAIN || res == -EWOULDBLOCK) {
            // TODO: I think we should never get here, as uring is supposed to
            // only wake us up with data filled.
            abort();
        } else {
            f->reserr = 0;
            f->state = mcs_fstate_syserr;
        }
    } else if (res == 0) {
        // disconnected, but probably gracefully
        f->reserr = 0;
        f->state = mcs_fstate_syserr;
    }
}

static int mcs_reschedule(struct mcs_func *f) {
    if (f->rate.rate != 0) {
        if (_evset_abs_timeout(f) == 0) {
            // schedule the next wakeup time.
            timespec_add(&f->rate.next, &f->rate.delta);
        } else {
            return -1;
        }
    } else {
        return _evset_nop(f);
    }
    return 0;
}

static void mcs_syserror(struct mcs_func *f) {
    mcmc_disconnect(f->mcmc);
    if (f->reserr == 0) {
        fprintf(stderr, "%s: conn gracefully disconnected\n", f->fname);
    } else {
        // TODO: strerror
        fprintf(stderr, "%s: system error, reconnecting: %d\n", f->fname, f->reserr);
    }

    // we need to reset the coroutine.
    int res = lua_resetthread(f->L);
    if (res != LUA_OK) {
        // TODO: read lua code to find potential errors.
        abort();
    }

    f->state = mcs_fstate_retry;
}

static void mcs_restart(struct mcs_func *f) {
    f->state = mcs_fstate_run;
    if (f->limit != 0) {
        f->limit--;
        if (f->limit == 0) {
            mcmc_disconnect(f->mcmc);
            f->state = mcs_fstate_stop;
            return;
        }
    }
    if (f->reconn.every != 0) {
        f->reconn.after--;
        if (f->reconn.after == 0) {
            mcmc_disconnect(f->mcmc);
            f->reconn.after = f->reconn.every;
            f->state = mcs_fstate_disconn;
            return;
        }
    }
    if (f->parent->stop) {
        mcmc_disconnect(f->mcmc);
        f->state = mcs_fstate_stop;
        return;
    }
}

// run the function state machine.
// called _outside_ of the cqe reception loop
// must return -1 if we tried to allocate an SQE for some reason and couldn't.
static int mcs_func_run(void *udata) {
    struct mcs_func *f = udata;

    bool stop = false;
    int err = 0;
    while (!stop) {
    switch (f->state) {
        case mcs_fstate_disconn:
            if (mcs_connect(f) != 0) {
                mcmc_disconnect(f->mcmc);
                f->state = mcs_fstate_retry;
            }
            break;
        case mcs_fstate_connecting:
            if (_evset_wrpoll(f) != 0) {
                return -1;
            }
            f->state = mcs_fstate_postconnect;
            stop = true;
            break;
        case mcs_fstate_postconnect:
            if (mcmc_check_nonblock_connect(f->mcmc, &err) != MCMC_OK) {
                mcmc_disconnect(f->mcmc);
                f->state = mcs_fstate_retry;
            } else {
                mcs_start_limiter(f);
                f->state = mcs_fstate_run;
            }
            break;
        case mcs_fstate_run:
            mcs_func_lua(f);
            break;
        case mcs_fstate_restart:
            mcs_restart(f);
            break;
        case mcs_fstate_rerun:
            if (mcs_reschedule(f) == 0) {
                f->state = mcs_fstate_restart;
                stop = true;
            } else {
                return -1;
            }
            break;
        case mcs_fstate_flush:
            if (_evset_wrflush(f) == 0) {
                f->state = mcs_fstate_postflush;
                stop = true;
            } else {
                return -1;
            }
            break;
        case mcs_fstate_postflush:
            mcs_postflush(f);
            break;
        case mcs_fstate_read:
            if (_evset_read(f) == 0) {
                f->state = mcs_fstate_postread;
                stop = true;
            } else {
                return -1;
            }
            break;
        case mcs_fstate_postread:
            mcs_postread(f);
            break;
        case mcs_fstate_retry:
            if (_evset_retry_timeout(f) == 0) {
                f->state = mcs_fstate_postretry;
                stop = true;
            } else {
                return -1;
            }
            break;
        case mcs_fstate_postretry:
            // FIXME: go directly to disconn from retry?
            f->state = mcs_fstate_disconn;
            break;
        case mcs_fstate_syserr:
            mcs_syserror(f);
            break;
        case mcs_fstate_stop:
            f->parent->active_funcs--;
            stop = true;
            break;
        default:
            abort();
    }
    }

    return 0;
}

// writes the passed argument to the client buffer
static int mcslib_write_c(struct mcs_func *f) {
    int type = lua_type(f->L, -1);
    size_t len = 0;
    int vlen = 0;
    const char *rline = NULL;

    if (type == LUA_TUSERDATA) {
        struct mcs_func_req *req = lua_touserdata(f->L, -1);
        len = req->len;
        rline = req->data;
        vlen = req->vlen;
        clock_gettime(CLOCK_MONOTONIC, &req->start);
    } else if (type == LUA_TSTRING) {
        rline = luaL_tolstring(f->L, -1, &len);
    }

    mcs_expand_wbuf(f, len + vlen + 2);

    memcpy(f->wbuf + f->wbuf_used, rline, len);
    f->wbuf_used += len;

    lua_pop(f->L, 1);

    if (vlen != 0) {
        // TODO: write a specific pattern into the buffer.
        memset(f->wbuf + f->wbuf_used, 35, vlen);
        f->wbuf_used += vlen;

        memcpy(f->wbuf + f->wbuf_used, "\r\n", 2);
        f->wbuf_used += 2;
    }
    return 0;
}

// TODO: the read routine can offset via rbuf_toconsume until we hit
// "MCMC_WANT_READ" to avoid memmove's in most/many cases.
// Avoiding this optimization for now for stability.
static int mcslib_read_c(struct mcs_func *f) {
    // first we need to see how far to move the rbuf, based on the previous
    // successful read.
    if (f->rbuf_toconsume != 0) {
        f->rbuf_used -= f->rbuf_toconsume;
        if (f->rbuf_used > 0) {
            memmove(f->rbuf, f->rbuf+f->rbuf_toconsume, f->rbuf_used);
        }
        f->rbuf_toconsume = 0;
    }

    if (f->rbuf_used == 0) {
        f->state = mcs_fstate_read;
    } else {
        mcs_read_buf(f);
    }
    return 0;
}

// functions that yield should do minimal work then kick back here for
// handling.
// this allows us to avoid passing objects/context around in lua when we want
// to write things to clients.
// NOTE: The to-yield functions can instead push cfunctions that we call
// directly, which would require updating code in fewer places.
// I'm keeping a switch statement here because that's a little easier to debug
static int mcs_func_lua_yield(struct mcs_func *f, int nresults) {
    int yield_type = lua_tointeger(f->L, -1);
    lua_pop(f->L, 1);
    int res = 0;
    switch (yield_type) {
        case mcs_luayield_write:
            res = mcslib_write_c(f);
            break;
        case mcs_luayield_flush:
            f->wbuf_sent = 0;
            f->state = mcs_fstate_flush;
            break;
        case mcs_luayield_read:
            res = mcslib_read_c(f);
            break;
        default:
            abort();
    }
    return res;
}

static int mcs_func_lua(struct mcs_func *f) {
    int status = lua_status(f->L);
    int nresults = 0;
    switch (status) {
        case LUA_OK:
            // Kick off the function from the top.
            lua_getglobal(f->L, f->fname);
            if (lua_isnil(f->L, -1)) {
                fprintf(stderr, "Configuration missing '%s' function\n", f->fname);
                exit(EXIT_FAILURE);
            }
            f->lua_nargs = 0;
            // fall through to YIELD case.
        case LUA_YIELD:
            status = lua_resume(f->L, NULL, f->lua_nargs, &nresults);
            if (status == LUA_OK) {
                f->state = mcs_fstate_rerun;
            } else if (status == LUA_YIELD) {
                // We're paused for some reason.
                return mcs_func_lua_yield(f, nresults);
            } else {
                fprintf(stderr, "%s: Failed to run coroutine: %s\n", __func__, lua_tostring(f->L, -1));
                abort();
            }
            break;
        default:
            // if not OK or YIELD it's some kind of error state.
            abort();
            break;
    }

    return 0;
}

// *** THREAD RUNNER ***

static void mcs_start_limiter(struct mcs_func *f) {
    struct timespec ts = {0};

    int res = clock_gettime(CLOCK_MONOTONIC, &ts);
    if (res != 0) {
        abort();
    }

    // timespec and io_uring timespecs aren't the same (maybe).
    f->rate.next.tv_sec = ts.tv_sec;
    f->rate.next.tv_nsec = ts.tv_nsec;

    // add time delta to schedule the next run.
    timespec_add(&f->rate.next, &f->rate.delta);
}

// Only exists to escape us from the cqe loop.
// TODO: likely able to refactor this away once I've seen more of the
// structure.
static void mcs_queue_cb(void *udata, struct io_uring_cqe *cqe) {
    struct mcs_func *f = udata;
    f->cqe_res = cqe->res;
    STAILQ_INSERT_TAIL(&f->parent->funcs, f, next);
    f->linked = true;
}

static void *shredder_thread(void *arg) {
    struct mcs_thread *t = arg;
    t->stop = false;

    // uring core loop
    // TODO: check stop flag in loop.
    while (1) {
        struct io_uring_cqe *cqe;

        uint32_t head = 0;
        uint32_t count = 0;
        io_uring_for_each_cqe(&t->ring, head, cqe) {
            struct mcs_event *ev = io_uring_cqe_get_data(cqe);
            if (ev != NULL) {
                ev->cb(ev->udata, cqe);
            }
            count++;
        }

        io_uring_cq_advance(&t->ring, count);

        // call queue any queue callbacks
        while (!STAILQ_EMPTY(&t->funcs)) {
            struct mcs_func *f = STAILQ_FIRST(&t->funcs);
            STAILQ_REMOVE_HEAD(&t->funcs, next);
            f->linked = false;
            int res = f->ev.qcb(f);
            if (res == -1) {
                // failed to get SQE's, need to continue the list later.
                STAILQ_INSERT_HEAD(&t->funcs, f, next);
                break;
            }
        }

        if (t->active_funcs == 0) {
            break;
        }

        io_uring_submit_and_wait(&t->ring, 1);
    }

    pthread_mutex_lock(&t->ctx->wait_lock);
    pthread_cond_signal(&t->ctx->wait_cond);
    pthread_mutex_unlock(&t->ctx->wait_lock);

    return NULL;
}

// *** LUA ***

static int mcslib_thread(lua_State *L) {
    struct mcs_ctx *ctx = *(struct mcs_ctx **)lua_getextraspace(L);

    struct mcs_thread *t = lua_newuserdatauv(ctx->L, sizeof(struct mcs_thread), 0);
    STAILQ_INIT(&t->funcs);

    t->ctx = ctx;
    t->active_funcs = 0;
    t->L = luaL_newstate();
    // TODO: what to stuff into the extraspace? ctx or thread?
    luaL_openlibs(t->L);
    register_lua_libs(t->L);

    int res = luaL_dofile(t->L, ctx->conffile);
    if (res != LUA_OK) {
        fprintf(stderr, "Failed to load config file: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }

    init_thread_uring(t);

    return 1;
}

// takes mcsthread, table
// configures thread.
// arguments:
// conns, rate_limit, rate_period, reconn_every, reconn_random, ramp_period,
// start_delay
static int mcslib_run(lua_State *L) {
    struct mcs_ctx *ctx = *(struct mcs_ctx **)lua_getextraspace(L);
    luaL_checktype(L, 1, LUA_TUSERDATA);
    luaL_checktype(L, 2, LUA_TTABLE);

    struct mcs_thread *t = lua_touserdata(L, 1);
    int conns = 1;
    int limit = 0;
    struct mcs_f_rate frate = {0};
    struct mcs_f_reconn freconn = {0};

    if (lua_getfield(L, 2, "conns") != LUA_TNIL) {
        conns = lua_tointeger(L, -1);
    }
    lua_pop(L, 1);

    // seed the "rate" to be one per second per connection.
    frate.rate = conns;

    if (lua_getfield(L, 2, "rate_limit") != LUA_TNIL) {
        frate.rate = lua_tointeger(L, -1);
        frate.period = NSEC_PER_SEC; // default is rate per second.
    }
    lua_pop(L, 1);

    if (lua_getfield(L, 2, "rate_period") != LUA_TNIL) {
        frate.period = lua_tointeger(L, -1);
        frate.period *= NSEC_PER_SEC / 1000; // ms to ns
    }
    lua_pop(L, 1);

    if (lua_getfield(L, 2, "reconn_every") != LUA_TNIL) {
        freconn.every = lua_tointeger(L, -1);
        freconn.after = freconn.every;
    }
    lua_pop(L, 1);

    if (lua_getfield(L, 2, "limit") != LUA_TNIL) {
        limit = lua_tointeger(L, -1);
    }
    lua_pop(L, 1);

    // request rate is specified as total across all connections
    // divide it down to per-connection here.
    if (frate.rate != 0) {
        frate.rate /= conns;
        uint64_t rate_div = frate.period / frate.rate;
        frate.delta.tv_sec = rate_div / NSEC_PER_SEC;
        frate.delta.tv_nsec = rate_div - frate.delta.tv_sec * NSEC_PER_SEC;
    }

    for (int x = 0; x < conns; x++) {
        // create coroutine using thread VM.
        struct mcs_func *f = lua_newuserdatauv(t->L, sizeof(struct mcs_func), 0);
        memset(f, 0, sizeof(struct mcs_func));
        STAILQ_INSERT_TAIL(&t->funcs, f, next);
        t->active_funcs++;

        f->parent = t;

        // prepare the function's coroutine
        lua_newthread(t->L);
        lua_State *Lc = lua_tothread(t->L, -1);
        f->L = Lc;
        // pops thread
        f->self_ref_coro = luaL_ref(t->L, LUA_REGISTRYINDEX);
        // pop the func
        f->self_ref = luaL_ref(t->L, LUA_REGISTRYINDEX);

        // pull data from table into *f
        if (lua_getfield(L, 2, "func") != LUA_TNIL) {
            const char *fname = lua_tostring(L, -1);
            f->fname = strdup(fname);
        }
        lua_pop(L, 1);

        f->rate = frate;
        f->reconn = freconn;
        f->limit = limit;

        memcpy(&f->conn, &ctx->conn, sizeof(f->conn));

        // allocate mcmc client
        f->mcmc = calloc(1, mcmc_size(MCMC_OPTION_BLANK));

        // kick off the state machine.
        f->ev.qcb = mcs_func_run;
        f->ev.cb = mcs_queue_cb;
        f->ev.udata = f;
        f->state = mcs_fstate_disconn;

        f->wbuf = malloc(WBUF_INITIAL_SIZE);
        f->wbuf_size = WBUF_INITIAL_SIZE;

        f->rbuf = malloc(RBUF_INITIAL_SIZE);
        f->rbuf_size = RBUF_INITIAL_SIZE;
    }

    return 0;
}

static void _mcs_cleanup_thread(struct mcs_thread *t) {
    struct mcs_func *f = NULL;

    STAILQ_FOREACH(f, &t->funcs, next) {
        free(f->rbuf);
        free(f->wbuf);
        free(f->mcmc);
        free(f->fname);
        luaL_unref(t->L, LUA_REGISTRYINDEX, f->self_ref);
        luaL_unref(t->L, LUA_REGISTRYINDEX, f->self_ref_coro);
        // do not free the function: it's owned by the lua state
    }
    STAILQ_INIT(&t->funcs);
    // NOTE: attempting to make threads re-usable, so we leave the VM open.
    // lua_close(t->L);
    // io_uring_queue_exit(&t->ring);
    // do not free the thread object, it is owned by the context VM.
}

// main VM: start threads, run threads
static int mcslib_shredder(lua_State *L) {
    struct mcs_ctx *ctx = *(struct mcs_ctx **)lua_getextraspace(L);

    ctx->active_threads = 0;
    STAILQ_INIT(&ctx->threads);
    luaL_checktype(L, 1, LUA_TTABLE);
    int n = luaL_len(L, 1);
    for (int x = 1; x <= n; x++) {
        lua_geti(L, 1, x);
        struct mcs_thread *t = lua_touserdata(L, -1);
        lua_pop(L, 1);
        STAILQ_INSERT_TAIL(&ctx->threads, t, next);
        ctx->active_threads++;
    }

    pthread_mutex_lock(&ctx->wait_lock);
    struct mcs_thread *t;
    STAILQ_FOREACH(t, &ctx->threads, next) {
        int ret;
        ret = pthread_create(&t->tid, NULL, shredder_thread, t);
        if (ret != 0) {
            fprintf(stderr, "Failed to start shredder thread: %s\n",
                    strerror(ret));
            // FIXME: throw error and exit.
        }
    }

    int type = lua_type(L, -1);
    struct timespec wait;
    bool use_wait = false;;

    if (type == LUA_TNUMBER) {
        int tosleep = lua_tointeger(L, -1);
        clock_gettime(CLOCK_REALTIME, &wait);
        wait.tv_nsec = 0;
        wait.tv_sec += tosleep;
        use_wait = true;
    }

    while (ctx->active_threads) {
        if (use_wait) {
            int res = pthread_cond_timedwait(&ctx->wait_cond, &ctx->wait_lock, &wait);
            if (res == ETIMEDOUT) {
                // loosely signal threads to stop
                // note lack of locking making this imperfect
                STAILQ_FOREACH(t, &ctx->threads, next) {
                    t->stop = true;
                }
                use_wait = false;
                continue; // retry the loop.
            }
        } else {
            pthread_cond_wait(&ctx->wait_cond, &ctx->wait_lock);
        }
        ctx->active_threads--;
        if (ctx->active_threads == 0) {
            pthread_mutex_unlock(&ctx->wait_lock);
            break;
        }
    }

    // cleanup loop
    STAILQ_FOREACH(t, &ctx->threads, next) {
        // FIXME: assuming success.
        pthread_join(t->tid, NULL);
        _mcs_cleanup_thread(t);
    }

    return 0;
}

// TODO: minimal argument validation?
// since this is a benchmark tool we should attempt to minmax, and argument
// checking does take measurable time.
static int mcslib_write(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_write);
    return lua_yield(L, 2);
}

static int mcslib_flush(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_flush);
    return lua_yield(L, 1);
}

static int mcslib_read(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_read);
    return lua_yield(L, 1);
}

static int mcslib_resline(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -1);
    lua_pushlstring(L, r->buf, r->resp.reslen);
    return 1;
}

// get(prefix, number)
static int mcslib_get(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "get ", 4);
    p += 4;
    memcpy(p, pfx, len);
    p += len;

    int num = lua_tointeger(L, 2);

    p = itoa_32(num, p);

    memcpy(p, "\r\n", 2);
    p += 2;

    req->len = p - req->data;
    req->vlen = 0;

    return 1;
}

// TODO: xxhash the full key and use that for the value pattern.
// TODO: noreply support.
static int mcslib_set(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "set ", 4);
    p += 4;
    memcpy(p, pfx, len);
    p += len;

    int num = lua_tointeger(L, 2);

    p = itoa_32(num, p);
    *p = ' ';
    p++;

    int cflags = lua_tointeger(L, 3);
    p = itoa_32(cflags, p);
    *p = ' ';
    p++;

    int ttl = lua_tointeger(L, 4);
    p = itoa_32(ttl, p);
    *p = ' ';
    p++;

    req->vlen = lua_tointeger(L, 5);
    p = itoa_32(req->vlen, p);

    memcpy(p, "\r\n", 2);
    p += 2;

    req->len = p - req->data;

    return 1;
}

static int mcslib_ms(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "ms ", 3);
    p += 3;
    memcpy(p, pfx, len);
    p += len;

    int num = lua_tointeger(L, 2);

    p = itoa_32(num, p);
    *p = ' ';
    p++;

    req->vlen = lua_tointeger(L, 4);
    p = itoa_32(req->vlen, p);

    const char *flags = lua_tolstring(L, 3, &len);
    if (len) {
        *p = ' ';
        p++;
        memcpy(p, flags, len);
        p += len;
    }

    memcpy(p, "\r\n", 2);
    p += 2;

    req->len = p - req->data;

    return 1;
}

// TODO: automatically create and append an Opaque token for matching.
static int _mcslib_basic(lua_State *L, char cmd) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "m  ", 3);
    p[1] = cmd;
    p += 3;
    memcpy(p, pfx, len);
    p += len;

    int num = lua_tointeger(L, 2);

    p = itoa_32(num, p);

    const char *flags = lua_tolstring(L, 3, &len);
    if (len) {
        *p = ' ';
        p++;
        memcpy(p, flags, len);
        p += len;
    }

    memcpy(p, "\r\n", 2);
    p += 2;

    req->len = p - req->data;
    req->vlen = 0;

    return 1;
}

static int mcslib_mg(lua_State *L) {
    return _mcslib_basic(L, 'g');
}

static int mcslib_md(lua_State *L) {
    return _mcslib_basic(L, 'd');
}

static int mcslib_ma(lua_State *L) {
    return _mcslib_basic(L, 'a');
}

// TODO: use a differnt lib for main VM vs thread VM's?
// it should still be fine to use the same source file.
static void register_lua_libs(lua_State *L) {
    const struct luaL_Reg mcs_f [] = {
        {"thread", mcslib_thread},
        {"run", mcslib_run},
        {"shredder", mcslib_shredder},
        // func functions.
        {"write", mcslib_write},
        {"flush", mcslib_flush},
        {"read", mcslib_read},
        // object functions.
        {"resline", mcslib_resline},
        // request functions.
        {"get", mcslib_get},
        {"set", mcslib_set},
        {"mg", mcslib_mg},
        {"ms", mcslib_ms},
        {"md", mcslib_md},
        {"ma", mcslib_ma},
        {NULL, NULL}
    };

    luaL_newlibtable(L, mcs_f);
    luaL_setfuncs(L, mcs_f, 0);
    lua_setglobal(L, "mcs"); // set lib table to global
}

int main(int argc, char **argv) {
    const char *conffile = NULL;
    struct mcs_conn conn = {.host = "127.0.0.1", .port_num = "11211"};
    const struct option longopts[] = {
        {"ip", required_argument, 0, 'i'},
        {"port", required_argument, 0, 'p'},
        // connect to unix socket instead
        {"sock", required_argument, 0, 's'},
        {"conf", required_argument, 0, 'c'},
        // end
        {0, 0, 0, 0}
    };
    int optindex;
    int c;
    while (-1 != (c = getopt_long(argc, argv, "", longopts, &optindex))) {
        switch (c) {
        case 'i':
            strncpy(conn.host, optarg, NI_MAXHOST);
            break;
        case 'p':
            strncpy(conn.port_num, optarg, NI_MAXSERV);
            break;
        case 's':
            fprintf(stderr, "unix socket not yet implemented\n");
            return EXIT_FAILURE;
            break;
        case 'c':
            conffile = strdup(optarg);
            break;
        default:
            fprintf(stderr, "Unknown option\n");
            return EXIT_FAILURE;
        }
    }

    struct mcs_ctx *ctx = calloc(1, sizeof(struct mcs_ctx));
    memcpy(&ctx->conn, &conn, sizeof(conn));
    pthread_mutex_init(&ctx->wait_lock, NULL);
    pthread_cond_init(&ctx->wait_cond, NULL);

    // - create main VM
    lua_State *L = luaL_newstate();
    ctx->L = L;
    ctx->conffile = conffile;
    struct mcs_ctx **extra = lua_getextraspace(L);
    *extra = ctx;
    luaL_openlibs(L);
    register_lua_libs(L);

    if (conffile == NULL) {
        fprintf(stderr, "Must provide a config file: --conf etc.lua\n");
        exit(EXIT_FAILURE);
    }

    // - load config file
    int res = luaL_dofile(L, conffile);
    if (res != LUA_OK) {
        fprintf(stderr, "Failed to load config file: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }
    // - call "config" global cmd
    lua_getglobal(L, "config");

    if (lua_isnil(L, -1)) {
        fprintf(stderr, "Configuration missing 'config' function\n");
        exit(EXIT_FAILURE);
    }
    if (lua_pcall(L, 0, 0, 0) != LUA_OK) {
        fprintf(stderr, "Failed to execute config function: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }

    return EXIT_SUCCESS;
}
