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

#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>

#include <sys/eventfd.h>
#include <liburing.h>
#include <poll.h> // POLLOUT for liburing.

#include "mcshredder.h"
#include "vendor/mcmc/mcmc.h"
#include "queue.h"
#include "itoa_ljust.h"
#define XXH_INLINE_ALL
#include "xxhash.h"

#define PRING_QUEUE_SQ_ENTRIES 1024
#define PRING_QUEUE_CQ_ENTRIES 4096
// min available SQE's before we run a callback.
// means this is the max number of SQE's a callback can safely fetch.
#define PRING_MIN_SQE 4
// avoiding some hacks for finding member size.
#define SOCK_MAX 100

#define BUF_INITIAL_SIZE 1<<15
#define WBUF_INITIAL_SIZE 16384
#define RBUF_INITIAL_SIZE 65536

#define KEY_MAX_LENGTH 250
#define REQ_MAX_LENGTH KEY_MAX_LENGTH * 2

#define NSEC_PER_SEC 1000000000

#define PARSER_MAX_TOKENS 24
#define PARSER_MAXLEN USHRT_MAX-1

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
static int mcs_func_run(void *udata);
static void mcs_start_limiter(struct mcs_func *f);
static void _mcs_cleanup_thread(struct mcs_thread *t);
static bool mcs_postread(struct mcs_func *f, struct mcs_func_client *c);

typedef void (*event_cb)(void *udata, struct io_uring_cqe *cqe);
struct mcs_event {
    void *udata;
    event_cb cb;
    queue_cb qcb;
};

struct mcs_f_rate {
    int rate;
    uint64_t period; // stored in nanoseconds.
    struct __kernel_timespec delta; // period / rate in timespec format
    struct __kernel_timespec next; // next absolute time to schedule the alarm for
    struct __kernel_timespec start; // post-connect start offset
};

// governs when to naturally reconnect
struct mcs_f_reconn {
    unsigned int every; // how often to reconnect
    unsigned int after; // counter until next reconnect
};

enum mcs_lua_yield {
    mcs_luayield_write = 0,
    mcs_luayield_write_factory,
    mcs_luayield_flush,
    mcs_luayield_read,
    mcs_luayield_sleep,
    mcs_luayield_outwait,
    mcs_luayield_c_conn,
    mcs_luayield_c_read,
    mcs_luayield_c_readline,
    mcs_luayield_c_write,
    mcs_luayield_c_write_factory,
    mcs_luayield_c_flush,
};

enum mcs_req_type {
    mcs_req_type_flat = 0,
    mcs_req_type_factory = 1,
};

// factories can have at most one outstanding request at a time if you want to
// use response matching.
// if pipelining requests, pre-make N factories.
#define FACTORY_LENGTH KEY_MAX_LENGTH + REQ_MAX_LENGTH + 2
struct mcs_func_req {
    enum mcs_req_type type;
    struct timespec start;
    uint64_t hash;
    union {
        struct {
            int len;
            int vlen;
        } flat;
        struct {
            int64_t numeric;
            char cmd;
            int prefix_len;
            int postfix_len;
            // prefix/postfix are in data[]
        } fac;
    };
    char data[];
};

// points into func's rbuf
struct mcs_func_resp {
    int status;
    int ntokens; // zero if not tokenized
    struct timespec received; // time response was read from socket
    char *buf; // start of response buffer
    mcmc_resp_t resp;
    uint16_t tokens[PARSER_MAX_TOKENS]; // offsets for start of each token
};

struct mcs_func {
    lua_State *L; // lua coroutine local to this function
    int self_ref; // avoid garbage collection
    int self_ref_coro; // reference for the coroutine thread
    int arg_ref; // reference for function argument
    int func_ref; // reference to the function to call
    STAILQ_ENTRY(mcs_func) next_run; // coroutine run stack.
    STAILQ_ENTRY(mcs_func) next_func; // total live list.
    struct mcs_thread *parent; // pointer back to owner thread
    char *fname; // name of function to call
    bool linked;
    bool active;
    enum mcs_func_state state;
    struct mcs_f_rate rate; // rate limiter
    struct mcs_f_reconn reconn; // tcp reconnector
    struct mcs_event ev;
    struct mcs_func_client c;
    struct __kernel_timespec tosleep; // need a stable location for timespecs.
    int limit; // stop running after N loops
    int cqe_res; // result of most recent cqe
    int lua_nargs; // number of args to pass back to lua
    int reserr; // probably -ERRNO via uring.
    int buf_readline; // skip parsing on next line response.
};

typedef STAILQ_HEAD(func_head_s, mcs_func) func_head_t;
struct mcs_thread {
    struct mcs_ctx *ctx;
    lua_State *L; // lua VM local to this thread
    STAILQ_ENTRY(mcs_thread) next; // thread stack
    func_head_t func_runlist; // queued runlist
    func_head_t func_list; // coroutine stack
    struct io_uring ring;
    int active_funcs; // stop if no active functions
    pthread_t tid;
    bool stop;
};

enum mcs_memprofile_types {
    mcs_memp_free = 0,
    mcs_memp_string,
    mcs_memp_table,
    mcs_memp_func,
    mcs_memp_userdata,
    mcs_memp_thread,
    mcs_memp_default,
    mcs_memp_realloc,
};

struct mcs_memprofile {
    struct timespec last_status; // for per-second prints on status
    int id;
    uint64_t allocs[8];
    uint64_t alloc_bytes[8];
};

typedef STAILQ_HEAD(thread_head_s, mcs_thread) thread_head_t;
struct mcs_ctx {
    lua_State *L;
    thread_head_t threads; // stack of threads
    pthread_cond_t wait_cond; // thread completion signal
    pthread_mutex_t wait_lock;
#ifdef USE_TLS
    void *tls_ctx; // global SSL CTX object
#endif
    struct mcs_lock_buf out_buf; // out/listener stream buffer.
    eventfd_t out_fd;
    uint64_t out_readvar; // let io_uring put eventfd res somewhere.
    bool memprofile; // runs threads with a memory profiler.
    bool out_listener; // short circuit if there's no listener.
    bool stop; // some thread told us to cut out of the shredder.
    int active_threads; // return from shredder() if threads stopped
    int arg_ref; // commandline argument table
    int thread_counter; // to give ID's for the memory profiler.
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

static void *profile_alloc(void *ud, void *ptr, size_t osize,
                                            size_t nsize) {
    struct mcs_memprofile *prof = ud;
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    enum mcs_memprofile_types t = mcs_memp_free;
    if (ptr == NULL) {
        switch (osize) {
            case LUA_TSTRING:
                t = mcs_memp_string;
                //fprintf(stderr, "alloc string: %ld\n", nsize);
                break;
            case LUA_TTABLE:
                t = mcs_memp_table;
                //fprintf(stderr, "alloc table: %ld\n", nsize);
                break;
            case LUA_TFUNCTION:
                t = mcs_memp_func;
                //fprintf(stderr, "alloc func: %ld\n", nsize);
                break;
            case LUA_TUSERDATA:
                t = mcs_memp_userdata;
                //fprintf(stderr, "alloc userdata: %ld\n", nsize);
                break;
            case LUA_TTHREAD:
                t = mcs_memp_thread;
                //fprintf(stderr, "alloc thread: %ld\n", nsize);
                break;
            default:
                t = mcs_memp_default;
                //fprintf(stderr, "alloc osize: %ld nsize: %ld\n", osize, nsize);
        }
        prof->allocs[t]++;
        prof->alloc_bytes[t] += nsize;
    } else {
        if (nsize != 0) {
            prof->allocs[mcs_memp_realloc]++;
            prof->alloc_bytes[mcs_memp_realloc] += nsize;
        } else {
            prof->allocs[mcs_memp_free]++;
            prof->alloc_bytes[mcs_memp_free] += osize;
        }
        //fprintf(stderr, "realloc: osize: %ld nsize: %ld\n", osize, nsize);
    }

    if (now.tv_sec != prof->last_status.tv_sec) {
        prof->last_status.tv_sec = now.tv_sec;
        fprintf(stderr, "MEMPROF[%d]:\tstring[%llu][%llu] table[%llu][%llu] func[%llu][%llu] udata[%llu][%llu] thr[%llu][%llu] def[%llu][%llu] realloc[%llu][%llu] free[%llu][%llu]\n",
                prof->id,
                (unsigned long long)prof->allocs[1],
                (unsigned long long)prof->alloc_bytes[1],
                (unsigned long long)prof->allocs[2],
                (unsigned long long)prof->alloc_bytes[2],
                (unsigned long long)prof->allocs[3],
                (unsigned long long)prof->alloc_bytes[3],
                (unsigned long long)prof->allocs[4],
                (unsigned long long)prof->alloc_bytes[4],
                (unsigned long long)prof->allocs[5],
                (unsigned long long)prof->alloc_bytes[5],
                (unsigned long long)prof->allocs[6],
                (unsigned long long)prof->alloc_bytes[6],
                (unsigned long long)prof->allocs[7],
                (unsigned long long)prof->alloc_bytes[7],
                (unsigned long long)prof->allocs[0],
                (unsigned long long)prof->alloc_bytes[0]);
        for (int x = 0; x < 8; x++) {
            prof->allocs[x] = 0;
            prof->alloc_bytes[x] = 0;
        }
    }

    if (nsize == 0) {
        free(ptr);
        return NULL;
    } else {
        return realloc(ptr, nsize);
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
static void _evset_link_timeout(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    io_uring_prep_link_timeout(sqe, &timeout_default, 0);
    io_uring_sqe_set_data(sqe, NULL);
}

static void _evset_abs_timeout(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    io_uring_prep_timeout(sqe, &f->rate.next, 0, IORING_TIMEOUT_ABS);
    io_uring_sqe_set_data(sqe, &f->ev);
}

static void _evset_retry_timeout(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    io_uring_prep_timeout(sqe, &timeout_retry, 0, 0);
    io_uring_sqe_set_data(sqe, &f->ev);
}

static void _evset_sleep(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    io_uring_prep_timeout(sqe, &f->tosleep, 0, 0);
    io_uring_sqe_set_data(sqe, &f->ev);
}

static void _evset_wrpoll(struct mcs_func *f, struct mcs_func_client *c) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);

    io_uring_prep_poll_add(sqe, mcmc_fd(c->mcmc), POLLOUT);
    io_uring_sqe_set_data(sqe, &f->ev);

    sqe->flags |= IOSQE_IO_LINK;

    _evset_link_timeout(f);
}

static void _evset_nop(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);
    io_uring_prep_nop(sqe);
    io_uring_sqe_set_data(sqe, &f->ev);
}

static void _evset_wrflush(struct mcs_func *f, struct mcs_func_client *c) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);

    io_uring_prep_write(sqe, mcmc_fd(c->mcmc), c->wb.data + c->wb.offset, c->wb.used - c->wb.offset, 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    _evset_link_timeout(f);
}

void _evset_wrflush_data(struct mcs_func *f, struct mcs_func_client *c, char *data, long len) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);

    io_uring_prep_write(sqe, mcmc_fd(c->mcmc), data, len, 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    _evset_link_timeout(f);
}

void _evset_read_data(struct mcs_func *f, struct mcs_func_client *c, char *data, long len) {
    struct io_uring_sqe *sqe = io_uring_get_sqe(&f->parent->ring);

    io_uring_prep_recv(sqe, mcmc_fd(c->mcmc), data, len, 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    _evset_link_timeout(f);
}

static void _evset_read(struct mcs_func *f, struct mcs_func_client *c) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);

    io_uring_prep_recv(sqe, mcmc_fd(c->mcmc), c->rb.data + c->rb.used, c->rb.size - c->rb.used, 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    _evset_link_timeout(f);
}

static void _evset_read_eventfd(struct mcs_func *f) {
    struct io_uring_sqe *sqe;
    struct mcs_ctx *ctx = f->parent->ctx;

    sqe = io_uring_get_sqe(&f->parent->ring);

    io_uring_prep_read(sqe, ctx->out_fd, &ctx->out_readvar, sizeof(uint64_t), 0);
    io_uring_sqe_set_data(sqe, &f->ev);

    // no timeout; block forever.
}

static void _evset_cancel(struct mcs_func *f) {
    struct io_uring_sqe *sqe;

    sqe = io_uring_get_sqe(&f->parent->ring);

    io_uring_prep_cancel(sqe, &f->ev, 0);
    io_uring_sqe_set_data(sqe, &f->ev);
}

// *** CORE ***

int mcs_func_cqe_res(struct mcs_func *f) {
    return f->cqe_res;
}

void mcs_func_set_cqe_res(struct mcs_func *f, int res) {
    f->cqe_res = res;
}

struct mcs_func_client *mcs_func_get_client(struct mcs_func *f) {
    struct mcs_func_client *c = NULL;
    if (f->c.mcmc) {
        return c = &f->c;
    } else {
        // FIXME: gross. This requires the caller to assume that if it wants a
        // client, one is currently sitting in the stack.
        // Which should be generally true: however fixing the callback system
        // so we can also override the callback data will remove the need for
        // this function.
        c = lua_touserdata(f->L, 1);
    }
    return c;
}

// TODO: ifdef a STATE_DEBUG with a print.
void mcs_func_set_state(struct mcs_func *f, enum mcs_func_state s) {
    f->state = s;
}

static void mcs_client_init(struct mcs_func_client *c) {
    c->s_read = mcs_fstate_read;
    c->s_flush = mcs_fstate_flush;
}

static int mcs_lock_buf_used(struct mcs_lock_buf *b) {
    int used = 0;
    pthread_mutex_lock(&b->lock);
    used = b->buf.used;
    pthread_mutex_unlock(&b->lock);
    return used;
}

void mcs_buf_init(struct mcs_buf *b, size_t size) {
    memset(b, 0, sizeof(*b));
    b->data = malloc(size);
    b->size = size;
}

// For this codebase these buffers tend to settle into a certain size,
// and we don't shrink them. Increasing by chunks of the original size should
// help avoid memory bloat.
static void mcs_check_buf(struct mcs_buf *b, size_t len) {
    if (b->used + len <= b->size) {
        return;
    }

    while (b->used + len > b->size) {
        // FIXME: remember and increase by a multiple of the initial size?
        b->size += BUF_INITIAL_SIZE;
    }
    char *nb = realloc(b->data, b->size);
    if (nb == NULL) {
        fprintf(stderr, "Failed to realloc buffer buffer\n");
        abort();
    }
    b->data = nb;
}

static void mcs_reset_buf(struct mcs_buf *b) {
    char *data = b->data;
    size_t size = b->size;
    memset(b, 0, sizeof(struct mcs_buf));
    b->size = size;
    b->data = data;
}

static void mcs_consume_buf(struct mcs_buf *b) {
    if (b->toconsume != 0) {
        b->used -= b->toconsume;
        if (b->used > 0) {
            memmove(b->data, b->data+b->toconsume, b->used);
        }
        b->toconsume = 0;
    }
}

// The connect routine isn't very "io_uring-y", as it calls
// socket()/connect() from here, but considering we're calling connect in
// nonblock mode I'm not sure if there's any real difference in pushing it
// over uring.
static int mcs_connect(struct mcs_func_client *c) {
    int status = mcmc_connect(c->mcmc, c->conn.host, c->conn.port_num, MCMC_OPTION_NONBLOCK);
    mcs_reset_buf(&c->wb);
    mcs_reset_buf(&c->rb);
    if (status == MCMC_CONNECTED) {
        // NOTE: find when this is possible?
        fprintf(stderr, "Client connected unexpectedly, please report this\n");
        abort();
    } else if (status == MCMC_CONNECTING) {
        // need to wait for a writeable event.
        return 0;
    } else {
        // FIXME: use real error flow once it exists
        fprintf(stderr, "failed to connect: %s:%s\n", c->conn.host, c->conn.port_num);
        return -1;
    }
    return 0;
}

void mcs_postflush(struct mcs_func *f, struct mcs_func_client *c) {
    int res = f->cqe_res;
    struct mcs_buf *b = &c->wb;

    if (res > 0) {
        b->offset += res;
        if (b->offset < b->used) {
            // need to continue flushing write buffer.
            f->state = c->s_flush;
        } else {
            b->offset = 0;
            b->used = 0;
            f->state = mcs_fstate_run;
        }
    } else if (res < 0) {
        if (res == -EAGAIN || res == -EWOULDBLOCK) {
            // TODO: -> wrpoll -> flush
            // is this even possible with uring?
            fprintf(stderr, "Unexpectedly could not write to socket: please report this: %d\n", res);
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
static int mcs_read_buf(struct mcs_func *f, struct mcs_func_client *c) {
    int ret = 0; // RUN
    struct mcs_buf *b = &c->rb;
    char *rbuf_offset = b->data + b->toconsume;
    int rbuf_remain = b->used - b->toconsume;
    if (f->buf_readline == 0) {
        struct mcs_func_resp *r = lua_touserdata(f->L, -1);
        memset(r, 0, sizeof(*r));
        r->status = mcmc_parse_buf(rbuf_offset, rbuf_remain, &r->resp);
        if (r->status == MCMC_OK) {
            if (r->resp.vlen != r->resp.vlen_read) {
                if (b->toconsume != 0) {
                    // vlen didn't fit, but we are read partway into the
                    // buffer.
                    // memmove the buffer and read out.
                    mcs_consume_buf(&c->rb);
                } else {
                    // ... else the read buffer simply wasn't large enough.
                    mcs_check_buf(&c->rb, RBUF_INITIAL_SIZE);
                }
                ret = 1; // WANT_READ
            } else {
                r->buf = rbuf_offset;
                f->lua_nargs = 0;
                b->toconsume += r->resp.reslen + r->resp.vlen_read;

                clock_gettime(CLOCK_MONOTONIC, &r->received);
            }
        } else if (r->resp.code == MCMC_WANT_READ) {
            mcs_check_buf(&c->rb, RBUF_INITIAL_SIZE);
            ret = 1;
        } else {
            switch (r->resp.type) {
                case MCMC_RESP_ERRMSG:
                    if (r->resp.code != MCMC_CODE_SERVER_ERROR) {
                        fprintf(stderr, "Protocol error, reconnecting: %.*s\n", rbuf_remain, rbuf_offset);
                        ret = -1;
                    } else {
                        // SERVER_ERROR can be handled upstream
                        r->buf = rbuf_offset;
                        f->lua_nargs = 0;
                        b->toconsume += r->resp.reslen;
                        clock_gettime(CLOCK_MONOTONIC, &r->received);
                    }
                    break;
                case MCMC_RESP_FAIL:
                    fprintf(stderr, "Read failed, reconnecting: %.*s\n", rbuf_remain, rbuf_offset);
                    ret = -1;
                    break;
                default:
                    fprintf(stderr, "Read found garbage, reconnecting: %.*s\n", rbuf_remain, rbuf_offset);
                    ret = -1;
            }
        }
    } else {
        // looking for a nonstandard or expanded protocol response line.
        char *end = memchr(rbuf_offset, '\n', rbuf_remain);
        if (end != NULL) {
            f->buf_readline = 0;
            // FIXME: making an assumption the minimum read buffer size is
            // always big enough for one line. Could probably add the
            // detection code anyway once I'm sure this works?
            size_t len = end - rbuf_offset + 1;
            b->toconsume += len;
            if (len < 2) {
                fprintf(stderr, "Protocol error, short response: %d\n", (int)len);
                ret = -1;
            } else {
                if (*(end-1) == '\r') {
                    len -= 2;
                } else {
                    len--;
                }
                lua_pushlstring(f->L, rbuf_offset, len);
                f->lua_nargs = 1;
            }
        } else {
            // in case we were reading off the end of the data buffer.
            mcs_consume_buf(&c->rb);
            ret = 1; // WANT_READ
        }
    }

    return ret;
}

static bool mcs_postread(struct mcs_func *f, struct mcs_func_client *c) {
    int res = f->cqe_res;

    if (res > 0) {
        c->rb.used += res;
        int ret = mcs_read_buf(f, c);
        if (ret == 0) {
            f->state = mcs_fstate_run;
        } else if (ret == 1) {
            f->state = c->s_read;
        } else if (ret < 0) {
            f->reserr = ret;
            f->state = mcs_fstate_syserr;
        }
    } else if (res < 0) {
        if (res == -EAGAIN || res == -EWOULDBLOCK) {
            // TODO: I think we should never get here, as uring is supposed to
            // only wake us up with data filled.
            fprintf(stderr, "Unexpectedly could not read from socket, please report this\n");
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

    return true; // nonsense from the state machine.
}

static void mcs_reschedule(struct mcs_func *f) {
    if (f->rate.rate != 0) {
        _evset_abs_timeout(f);
        // schedule the next wakeup time.
        timespec_add(&f->rate.next, &f->rate.delta);
    } else {
        _evset_nop(f);
    }
}

static void mcs_syserror(struct mcs_func *f) {
    mcmc_disconnect(f->c.mcmc);
    f->c.connected = false;
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
        fprintf(stderr, "Lua thread failed to reset, aborting\n");
        abort();
    }

    f->state = mcs_fstate_retry;
}

static void mcs_restart(struct mcs_func *f) {
    f->state = mcs_fstate_run;
    if (f->limit != 0) {
        f->limit--;
        if (f->limit == 0) {
            mcmc_disconnect(f->c.mcmc);
            f->state = mcs_fstate_stop;
            return;
        }
    }
    if (f->reconn.every != 0) {
        f->reconn.after--;
        if (f->reconn.after == 0) {
            mcmc_disconnect(f->c.mcmc);
            f->c.connected = false;
            f->reconn.after = f->reconn.every;
            f->state = mcs_fstate_disconn;
            return;
        }
    }
    if (f->parent->stop) {
        mcmc_disconnect(f->c.mcmc);
        f->c.connected = false;
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
            if (mcs_connect(&f->c) == 0) {
                _evset_wrpoll(f, &f->c);
                f->state = mcs_fstate_connecting;
                stop = true;
            } else {
                mcmc_disconnect(f->c.mcmc);
                f->c.connected = false;
                f->state = mcs_fstate_retry;
            }
            break;
        case mcs_fstate_connecting:
            if (mcmc_check_nonblock_connect(f->c.mcmc, &err) != MCMC_OK) {
                mcmc_disconnect(f->c.mcmc);
                f->c.connected = false;
                f->state = mcs_fstate_retry;
            } else {
                if (f->c.tls) {
                    stop = mcs_tls_postconnect(f, &f->c);
                } else {
                    f->state = mcs_fstate_postconnect;
                }
            }
            break;
        case mcs_fstate_tls_hs_postread:
            stop = mcs_tls_hs_postread(f);
            break;
        case mcs_fstate_tls_hs_postwrite:
            stop = mcs_tls_hs_postwrite(f);
            break;
        case mcs_fstate_postconnect:
            f->c.connected = true;
            mcs_start_limiter(f);
            f->state = mcs_fstate_rerun;
            break;
        case mcs_fstate_run:
            if (mcs_func_lua(f)) {
                f->state = mcs_fstate_rerun;
            }
            break;
        case mcs_fstate_restart:
            mcs_restart(f);
            break;
        case mcs_fstate_rerun:
            mcs_reschedule(f);
            f->state = mcs_fstate_restart;
            stop = true;
            break;
        case mcs_fstate_flush:
            _evset_wrflush(f, &f->c);
            f->state = mcs_fstate_postflush;
            stop = true;
            break;
        case mcs_fstate_postflush:
            mcs_postflush(f, &f->c);
            break;
        case mcs_fstate_read:
            _evset_read(f, &f->c);
            mcs_func_set_state(f, mcs_fstate_postread);
            stop = true;
            break;
        case mcs_fstate_postread:
            mcs_postread(f, &f->c);
            break;
        case mcs_fstate_tls_read:
            stop = mcs_tls_read(f, &f->c);
            break;
        case mcs_fstate_tls_postread:
            stop = mcs_tls_postread(f, &f->c);
            break;
        case mcs_fstate_tls_flush:
            stop = mcs_tls_flush(f, &f->c);
            break;
        case mcs_fstate_tls_postflush:
            stop = mcs_tls_postflush(f, &f->c);
            break;
        case mcs_fstate_retry:
            _evset_retry_timeout(f);
            f->state = mcs_fstate_postretry;
            stop = true;
            break;
        case mcs_fstate_postretry:
            // FIXME: go directly to disconn from retry?
            f->state = mcs_fstate_disconn;
            break;
        case mcs_fstate_syserr:
            mcs_syserror(f);
            break;
        case mcs_fstate_sleep:
            _evset_sleep(f);
            f->state = mcs_fstate_run;
            stop = true;
            break;
        case mcs_fstate_stop:
            f->parent->active_funcs--;
            f->active = false;
            stop = true;
            break;
        default:
            fprintf(stderr, "Unhandled function state, aborting\n");
            abort();
    }
    }

    return 0;
}

static int mcs_cfunc_run(void *udata) {
    struct mcs_func *f = udata;
    struct mcs_func_client *c = NULL;

    bool stop = false;
    int err = 0;
    while (!stop) {
    switch (f->state) {
        case mcs_fstate_connecting:
            c = lua_touserdata(f->L, 1);
            if (mcmc_check_nonblock_connect(c->mcmc, &err) != MCMC_OK) {
                mcmc_disconnect(c->mcmc);
                c->connected = false;
                lua_pushboolean(f->L, 0);
                f->lua_nargs = 1;
            } else {
                if (c->tls) {
                    stop = mcs_tls_postconnect(f, c);
                } else {
                    f->state = mcs_fstate_postconnect;
                }
            }
            break;
        case mcs_fstate_tls_hs_postread:
            stop = mcs_tls_hs_postread(f);
            break;
        case mcs_fstate_tls_hs_postwrite:
            stop = mcs_tls_hs_postwrite(f);
            break;
        case mcs_fstate_postconnect:
            c = lua_touserdata(f->L, 1);
            c->connected = true;
            lua_pushboolean(f->L, 1);
            f->lua_nargs = 1;
            f->state = mcs_fstate_run;
            break;
        case mcs_fstate_read:
            c = lua_touserdata(f->L, 1);
            if (!c->connected) {
                lua_pushnil(f->L);
                // FIXME: get a real error in here.
                lua_pushinteger(f->L, -1);
                f->lua_nargs = 2;
                f->state = mcs_fstate_run;
            } else {
                _evset_read(f, c);
                f->state = mcs_fstate_postread;
                stop = true;
            }
            break;
        case mcs_fstate_postread:
            c = lua_touserdata(f->L, 1);
            mcs_postread(f, c);
            break;
        case mcs_fstate_flush:
            c = lua_touserdata(f->L, 1);
            if (!c->connected) {
                lua_pushboolean(f->L, 0);
                lua_pushinteger(f->L, 0);
                f->lua_nargs = 2;
                f->state = mcs_fstate_run;
            } else {
                _evset_wrflush(f, c);
                f->state = mcs_fstate_postflush;
                stop = true;
            }
            break;
        case mcs_fstate_postflush:
            c = lua_touserdata(f->L, 1);
            mcs_postflush(f, c);
            break;
        case mcs_fstate_tls_read:
            // FIXME: need to do the !connected check like in fstate_read?
            // do the same with all the below funcs.
            c = lua_touserdata(f->L, 1);
            stop = mcs_tls_read(f, c);
            break;
        case mcs_fstate_tls_postread:
            c = lua_touserdata(f->L, 1);
            stop = mcs_tls_postread(f, c);
            break;
        case mcs_fstate_tls_flush:
            c = lua_touserdata(f->L, 1);
            stop = mcs_tls_flush(f, c);
            break;
        case mcs_fstate_tls_postflush:
            c = lua_touserdata(f->L, 1);
            stop = mcs_tls_postflush(f, c);
            break;
        case mcs_fstate_syserr:
            c = lua_touserdata(f->L, 1);
            mcmc_disconnect(c->mcmc);
            c->connected = false;
            lua_pushinteger(f->L, f->reserr);
            f->lua_nargs = 2;
            f->state = mcs_fstate_run;
            break;
        case mcs_fstate_outwait:
            // NOTE: I suspect there's still a race condition where we can end
            // up sleeping forever. this won't happen in any normal loads as
            // things will keep putting data on the out buffer.
            // might need to write the total bytes into the eventfd and then
            // read that much per wake event.
            if (mcs_lock_buf_used(&f->parent->ctx->out_buf) != 0) {
                f->state = mcs_fstate_run;
            } else {
                _evset_read_eventfd(f);
                stop = true;
            }
            break;
        case mcs_fstate_run:
            switch (mcs_func_lua(f)) {
                case 0:
                    break; // do nothing
                case 1:
                    f->state = mcs_fstate_stop; // complete
                    break;
                case 2:
                    stop = true; // resume later.
                    break;
            }
            break;
        case mcs_fstate_sleep:
            _evset_sleep(f);
            f->state = mcs_fstate_run;
            stop = true;
            break;
        case mcs_fstate_stop:
            f->parent->active_funcs--;
            f->active = false;
            stop = true;
            break;
        default:
            fprintf(stderr, "Unhandled custom function state, aborting\n");
            abort();
    }
    }

    return 0;
}

// Turn the 8 byte hash into a pattern-fill for the value. Just filling with
// the same value can miss a few classes of bugs.
static char *_mcs_write_value(uint64_t hash, int vlen, char *p) {
     for (int x = 0; x < vlen / sizeof(hash); x++) {
        memcpy(p, &hash, sizeof(hash));
        hash++;
        p += sizeof(hash);
    }

    int remain = vlen % sizeof(hash);
    for (int x = 0; x < remain; x++) {
        *p = '#';
        p++;
    }

    memcpy(p, "\r\n", 2);
    p += 2;
    return p;
}

// should be doing cast-comparisons since that should be a bit faster, but not
// sure what the compiler's going to do with this to be honest.
static int _mcs_check_value(struct mcs_func_req *req, struct mcs_func_resp *res) {
    uint64_t hash = req->hash;
    // remove \r\n from value for comparison.
    for (int x = 0; x < (res->resp.vlen-2) / sizeof(hash); x++) {
        if (memcmp(res->resp.value + x * sizeof(hash), &hash, sizeof(hash)) != 0) {
            return -1;
        }
        hash++;
    }
    // TODO: bother checking the remain? it should be fine since we can't get
    // here without the \r\n being valid _and_ all of the key specific stuff
    // checked.
    //int remain = res->resp.vlen % sizeof(hash);

    return 0;
}

// writes the passed argument to the client buffer
static int mcslib_write_c(struct mcs_func *f, struct mcs_func_client *c) {
    int type = lua_type(f->L, -1);
    size_t len = 0;
    int vlen = 0;
    const char *rline = NULL;
    struct mcs_func_req *req = NULL;
    struct mcs_buf *b = &c->wb;

    if (type == LUA_TUSERDATA) {
        req = lua_touserdata(f->L, -1);
        len = req->flat.len;
        rline = req->data;
        vlen = req->flat.vlen;
        clock_gettime(CLOCK_MONOTONIC, &req->start);
    } else if (type == LUA_TSTRING) {
        rline = luaL_tolstring(f->L, -1, &len);
    }

    mcs_check_buf(&c->wb, len + vlen + 2);

    memcpy(b->data + b->used, rline, len);
    b->used += len;

    lua_pop(f->L, 1);

    if (vlen != 0) {
        char *p = _mcs_write_value(req->hash, vlen, (char *)b->data + b->used);
        b->used += p - (b->data + b->used);
    }
    return 0;
}

// uses the passed factory and numeric to write request into the client
// write buffer.
// for set requests, also takes the value size as a final argument.
static int mcslib_write_c_factory(struct mcs_func *f, struct mcs_func_client *c, int offset) {
    struct mcs_func_req *req = lua_touserdata(f->L, offset + 1);
    int32_t vlen = 0;

    mcs_check_buf(&c->wb, REQ_MAX_LENGTH);

    char *p = c->wb.data + c->wb.used;
    char *start = p;

    memcpy(p, "m  ", 3);
    p[1] = req->fac.cmd;
    p += 3;
    char *key = p;

    memcpy(p, req->data, req->fac.prefix_len);
    p += req->fac.prefix_len;

    req->fac.numeric = lua_tointeger(f->L, offset + 2);
    if (req->fac.numeric > -1) {
        p = itoa_64(req->fac.numeric, p);
    }
    int klen = p - key;
    req->hash = XXH3_64bits(key, klen);

    if (req->fac.cmd == 's') {
        vlen = lua_tointeger(f->L, offset + 3);
        *p = ' ';
        p++;
        p = itoa_32(vlen, p);
    }

    // should include the \r\n terminator.
    memcpy(p, req->data + req->fac.prefix_len, req->fac.postfix_len);
    p += req->fac.postfix_len;

    if (req->fac.cmd == 's') {
        p = _mcs_write_value(req->hash, vlen, p);
    }

    clock_gettime(CLOCK_MONOTONIC, &req->start);
    c->wb.used += p - start;

    return 0;
}

// TODO: the read routine can offset via rbuf_toconsume until we hit
// "MCMC_WANT_READ" to avoid memmove's in most/many cases.
// Avoiding this optimization for now for stability.
static int mcslib_read_c(struct mcs_func_client *c) {
    struct mcs_buf *b = &c->rb;
    if (b->toconsume == b->used) {
        b->used = 0;
        b->toconsume = 0;
    }

    if (b->used == 0) {
        return 0;
    } else {
        return 1;
    }
}

static void mcs_set_sleep(struct mcs_func *f) {
    lua_State *L = f->L;
    lua_Integer t = lua_tointeger(L, 1);
    f->tosleep.tv_sec = t / 1000;
    f->tosleep.tv_nsec = (t - (f->tosleep.tv_sec * 1000)) * 1000000;
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
    struct mcs_func_client *c = NULL;
    lua_pop(f->L, 1);
    int res = 0;
    switch (yield_type) {
        case mcs_luayield_write:
            res = mcslib_write_c(f, &f->c);
            break;
        case mcs_luayield_write_factory:
            res = mcslib_write_c_factory(f, &f->c, 0);
            break;
        case mcs_luayield_flush:
            f->c.wb.offset = 0;
            f->state = f->c.s_flush;
            break;
        case mcs_luayield_read:
            if (mcslib_read_c(&f->c)) {
                int ret = mcs_read_buf(f, &f->c);
                if (ret == 0) {
                    f->state = mcs_fstate_run;
                } else if (ret == 1) {
                    f->state = f->c.s_read;
                } else if (ret < 0) {
                    f->reserr = 1;
                    f->state = mcs_fstate_syserr;
                }
            } else {
                f->state = f->c.s_read;
            }
            break;
        case mcs_luayield_sleep:
            mcs_set_sleep(f);
            f->state = mcs_fstate_sleep;
            break;
        case mcs_luayield_outwait:
            if (mcs_lock_buf_used(&f->parent->ctx->out_buf) != 0) {
                f->state = mcs_fstate_run;
            } else {
                f->state = mcs_fstate_outwait;
            }
            break;
        case mcs_luayield_c_conn:
            c = lua_touserdata(f->L, 1);
            _evset_wrpoll(f, c);
            f->state = mcs_fstate_connecting;
            res = 2; // stop caller state machine.
            break;
        case mcs_luayield_c_readline:
            f->buf_readline = 1;
            // explicit fall through.
        case mcs_luayield_c_read:
            c = lua_touserdata(f->L, 1);
            if (mcslib_read_c(c)) {
                int ret = mcs_read_buf(f, c);
                if (ret == 0) {
                    f->state = mcs_fstate_run;
                } else if (ret == 1) {
                    f->state = c->s_read;
                } else if (ret < 0) {
                    f->reserr = ret;
                    f->state = mcs_fstate_syserr;
                }
            } else {
                f->state = c->s_read;
            }
            break;
        case mcs_luayield_c_write:
            c = lua_touserdata(f->L, 1);
            res = mcslib_write_c(f, c);
            break;
        case mcs_luayield_c_write_factory:
            c = lua_touserdata(f->L, 1);
            res = mcslib_write_c_factory(f, c, 1);
            break;
        case mcs_luayield_c_flush:
            c = lua_touserdata(f->L, 1);
            c->wb.offset = 0;
            f->state = c->s_flush;
            break;
        default:
            fprintf(stderr, "Unhandled yield state, aborting\n");
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
            lua_rawgeti(f->L, LUA_REGISTRYINDEX, f->func_ref);
            f->lua_nargs = 0;
            if (f->arg_ref) {
                lua_rawgeti(f->L, LUA_REGISTRYINDEX, f->arg_ref);
                f->lua_nargs = 1;
            }
            // fall through to YIELD case.
        case LUA_YIELD:
            status = lua_resume(f->L, NULL, f->lua_nargs, &nresults);
            f->lua_nargs = 0;
            if (status == LUA_OK) {
                return 1;
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
            fprintf(stderr, "Lua thread yielded an unexpected error, aborting\n");
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
        fprintf(stderr, "Failed to get monotonic clock, aborting\n");
        abort();
    }

    // timespec and io_uring timespecs aren't the same (maybe).
    f->rate.next.tv_sec = ts.tv_sec;
    f->rate.next.tv_nsec = ts.tv_nsec;

    // add time delta to schedule the first run.
    timespec_add(&f->rate.next, &f->rate.start);
}

// Only exists to escape us from the cqe loop.
// TODO: likely able to refactor this away once I've seen more of the
// structure.
static void mcs_queue_cb(void *udata, struct io_uring_cqe *cqe) {
    struct mcs_func *f = udata;
    f->cqe_res = cqe->res;
    STAILQ_INSERT_TAIL(&f->parent->func_runlist, f, next_run);
    f->linked = true;
}

// alternative cooldown function for cancelling and stopping active funcs with
// outstanding waits (either hung, timers, or non-exiting loops).
// there's some structure here for the types of return codes I observed in
// case I feel like filling them in or handling this differently.
// Currently we send cancellations until ENOENT comes back, but if I have more
// faith in my understanding of io_uring we could stop after the matching
// ECANCELED is found.
// NOTE: This loop is imperfect: if we want to immediately cancel we may need
// an extra bool on "cancellation sent" so it won't double up in a loop? I see
// what looks like data corruption when I avoid waiting for the io_uring
// timeout to stop. It doesn't happen if we wait so I'm leaving this note for
// the next time I feel like investigating io_uring esoterics.
static void mcs_close_cb(void *udata, struct io_uring_cqe *cqe) {
    struct mcs_func *f = udata;
    if (cqe->res == -ENOENT) {
        if (f->active) {
            f->parent->active_funcs--;
            f->active = false;
        }
    } else if (cqe->res == -EALREADY) {
        // original request is already on its way back.
    } else if (cqe->res == -ECANCELED) {
        // this was the cqe for the original request.
        // there should only be one; our cancellation request resulted in this
        // cqe, so far as I can tell.
    } else {
        // some unknown error.
    }
}

static void *shredder_thread(void *arg) {
    struct __kernel_timespec timeout_loop = { .tv_sec = 0, .tv_nsec = 500000000 };
    struct mcs_thread *t = arg;
    t->stop = false;
    func_head_t fhead; // temporary func stack.
    STAILQ_INIT(&fhead);

    // uring core loop
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
        STAILQ_CONCAT(&fhead, &t->func_runlist);

        // call queue any queue callbacks
        while (!STAILQ_EMPTY(&fhead)
                && io_uring_sq_space_left(&t->ring) > PRING_MIN_SQE) {
            struct mcs_func *f = STAILQ_FIRST(&fhead);
            STAILQ_REMOVE_HEAD(&fhead, next_run);
            f->linked = false;
            int res = f->ev.qcb(f);
            if (res == -1) {
                // temporary failure, need to continue the list later.
                STAILQ_INSERT_HEAD(&t->func_runlist, f, next_run);
                break;
            }
        }

        if (t->active_funcs == 0) {
            break;
        }

        // TODO: this returns number of cqe and populates cqe array so we can
        // restructure the above loop too?
        int res = io_uring_submit_and_wait_timeout(&t->ring, &cqe, 1, &timeout_loop, NULL);
        if (res < 0 && t->stop) {
            struct mcs_func *f = NULL;
            // parent told us to stop.
            // insert cancellation requests so we can close the funcs.
            STAILQ_FOREACH(f, &t->func_list, next_func) {
                if (f->active) {
                    f->ev.cb = mcs_close_cb;
                    // have to try again later.
                    if (io_uring_sq_space_left(&t->ring) < PRING_MIN_SQE) {
                        break;
                    }
                    _evset_cancel(f);
                }
            }
        }
    }

    pthread_mutex_lock(&t->ctx->wait_lock);
    t->ctx->active_threads--;
    pthread_cond_signal(&t->ctx->wait_cond);
    pthread_mutex_unlock(&t->ctx->wait_lock);

    _mcs_cleanup_thread(t);
    return NULL;
}

// *** LUA ***

static int mcslib_thread(lua_State *L) {
    struct mcs_ctx *ctx = *(struct mcs_ctx **)lua_getextraspace(L);

    struct mcs_thread *t = lua_newuserdatauv(ctx->L, sizeof(struct mcs_thread), 0);
    STAILQ_INIT(&t->func_runlist);
    STAILQ_INIT(&t->func_list);

    t->ctx = ctx;
    t->active_funcs = 0;
    if (ctx->memprofile) {
        struct mcs_memprofile *prof = calloc(1, sizeof(struct mcs_memprofile));
        prof->id = ctx->thread_counter++;
        t->L = lua_newstate(profile_alloc, prof);
    } else {
        t->L = luaL_newstate();
    }
    // allow in-thread functions to access both the parent thread and ctx
    struct mcs_thread **extra = lua_getextraspace(t->L);
    *extra = t;
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

static struct mcs_func *mcs_add_func(struct mcs_thread *t) {
    // create coroutine using thread VM.
    struct mcs_func *f = lua_newuserdatauv(t->L, sizeof(struct mcs_func), 0);
    memset(f, 0, sizeof(struct mcs_func));
    STAILQ_INSERT_TAIL(&t->func_runlist, f, next_run);
    STAILQ_INSERT_TAIL(&t->func_list, f, next_func);
    t->active_funcs++;
    f->active = true;

    f->parent = t;

    // prepare the function's coroutine
    lua_newthread(t->L);
    lua_State *Lc = lua_tothread(t->L, -1);
    f->L = Lc;
    // pops thread
    f->self_ref_coro = luaL_ref(t->L, LUA_REGISTRYINDEX);
    // pop the func
    f->self_ref = luaL_ref(t->L, LUA_REGISTRYINDEX);

    // allocate mcmc client
    f->c.mcmc = calloc(1, mcmc_size(MCMC_OPTION_BLANK));
    // set our back reference for callback handling later.
    f->c.parent = f;

    // kick off the state machine.
    f->ev.qcb = mcs_func_run;
    f->ev.cb = mcs_queue_cb;
    f->ev.udata = f;
    f->state = mcs_fstate_disconn;

    mcs_buf_init(&f->c.wb, WBUF_INITIAL_SIZE);
    mcs_buf_init(&f->c.rb, RBUF_INITIAL_SIZE);
    mcs_client_init(&f->c);
#ifdef USE_TLS
    if (t->ctx->tls_ctx) {
        mcs_tls_client_init(&f->c, t->ctx->tls_ctx);
    }
#endif

    return f;
}

static void _mcs_copy_table(lua_State *from, lua_State *to);
static void _mcs_copy_table(lua_State *from, lua_State *to) {
    int type = lua_type(from, -1);
    switch (type) {
        case LUA_TNIL:
            lua_pushnil(to);
            break;
        case LUA_TUSERDATA:
            // FIXME: error.
            break;
        case LUA_TNUMBER:
            if (lua_isinteger(from, -1)) {
                lua_pushinteger(to, lua_tointeger(from, -1));
            } else {
                lua_pushnumber(to, lua_tonumber(from, -1));
            }
            break;
        case LUA_TBOOLEAN:
            lua_pushboolean(to, lua_toboolean(from, -1));
            break;
        case LUA_TSTRING:
            lua_pushlstring(to, lua_tostring(from, -1), lua_rawlen(from, -1));
            break;
        case LUA_TTABLE:
            lua_newtable(to);
            int t = lua_absindex(from, -1); // static index of from table.
            int nt = lua_absindex(to, -1); // static index of new table.
            lua_pushnil(from); // start iterator for main
            while (lua_next(from, t) != 0) {
                int keytype = lua_type(from, -2);
                switch (keytype) {
                    case LUA_TNUMBER:
                        if (lua_isinteger(from, -2)) {
                            lua_pushinteger(to, lua_tointeger(from, -2));
                        } else {
                            lua_pushnumber(to, lua_tonumber(from, -2));
                        }
                        break;
                    case LUA_TSTRING:
                        lua_pushlstring(to, lua_tostring(from, -2), lua_rawlen(from, -2));
                        break;
                    default:
                        // TODO: error
                        fprintf(stderr, "UNHANDLED KEY TYPE: %s\n", lua_typename(from, keytype));
                        break;
                }
                _mcs_copy_table(from, to); // recurse.
                lua_settable(to, nt);
                lua_pop(from, 1); // drop value, keep key.
            }
            break;
        default:
            fprintf(stderr, "UNHANDLED TYPE: %s\n", lua_typename(from, type));
    }
}

// takes mcsthread, table, [passthru argument value]
// or table of mcsthread, table
// configures thread.
// arguments:
// clients, rate_limit, rate_period, reconn_every, limit
static int mcslib_add(lua_State *L) {
    struct mcs_ctx *ctx = *(struct mcs_ctx **)lua_getextraspace(L);
    struct mcs_thread **threads = NULL;
    int threadcount = 0;
    int type = lua_type(L, 1);
    int arg_type = lua_type(L, 3);
    if (type == LUA_TUSERDATA) {
        threadcount = 1;
        threads = calloc(threadcount, sizeof(struct mcs_thread *));
        threads[0] = (struct mcs_thread *) lua_touserdata(L, 1);
    } else if (type == LUA_TTABLE) {
        lua_pushnil(L); // initialize table iteration.
        while (lua_next(L, 1) != 0) {
            luaL_checktype(L, -1, LUA_TUSERDATA);
            threadcount++;
            lua_pop(L, 1); // remove value, keep key.
        }
        threads = calloc(threadcount, sizeof(struct mcs_thread *));
        // loop again to get the threads
        lua_pushnil(L); // initialize table iteration.
        int n = 0;
        while (lua_next(L, 1) != 0) {
            threads[n] = (struct mcs_thread *) lua_touserdata(L, -1);
            n++;
            lua_pop(L, 1); // remove value, keep key.
        }

    }
    luaL_checktype(L, 2, LUA_TTABLE);

    int clients = 1;
    int limit = 0;
    bool use_init = false;
    struct mcs_f_rate frate = {0};
    struct mcs_f_reconn freconn = {0};

    if (lua_getfield(L, 2, "clients") != LUA_TNIL) {
        clients = lua_tointeger(L, -1) / threadcount;
        if (clients < 1) {
            clients = 1;
        }
    }
    lua_pop(L, 1);

    // seed the "rate" to be one per second per connection.
    frate.rate = clients;
    frate.period = NSEC_PER_SEC; // default is rate per second.

    if (lua_getfield(L, 2, "rate_limit") != LUA_TNIL) {
        frate.rate = lua_tointeger(L, -1);
        if (frate.rate != 0 && frate.rate <= threadcount) {
            frate.rate = 1;
        } else {
            frate.rate /= threadcount;
        }
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
        limit = lua_tointeger(L, -1) / threadcount;
        limit++; // first run counts against the limiter.
    }
    lua_pop(L, 1);

    if (lua_getfield(L, 2, "init") != LUA_TNIL) {
        use_init = lua_toboolean(L, -1);
    }
    lua_pop(L, 1);

    // request rate is specified as total across all connections
    // divide it down to per-connection here.
    int start_rate = 0;
    if (frate.rate != 0) {
        frate.rate /= clients;
        uint64_t rate_div = frate.period / frate.rate;
        frate.delta.tv_sec = rate_div / NSEC_PER_SEC;
        frate.delta.tv_nsec = rate_div - frate.delta.tv_sec * NSEC_PER_SEC;

        // we also need an initial offset for each client post-connect or a
        // large number of clients will rush all of their requests at once.
        start_rate = frate.period / clients;
    }

    const char *fname = NULL;
    if (lua_getfield(L, 2, "func") != LUA_TNIL) {
        fname = lua_tostring(L, -1);
    }
    lua_pop(L, 1);
    if (fname == NULL) {
        luaL_error(L, "mcs.add call missing 'func' argument");
    }

    for (int i = 0; i < threadcount; i++) {
        struct mcs_thread *t = threads[i];
        int arg = 0;
        if (arg_type != LUA_TNONE) {
            // expects argument table to be in slot -1.
            _mcs_copy_table(L, t->L);
            arg = lua_absindex(t->L, -1);
        }
        for (int x = 0; x < clients; x++) {
            struct mcs_func *f = mcs_add_func(t);

            if (arg) {
                lua_pushvalue(t->L, arg);
                f->arg_ref = luaL_ref(t->L, LUA_REGISTRYINDEX);
            }

            // TODO: move this to a helper func.
            lua_getglobal(t->L, fname);
            if (lua_isnil(t->L, -1)) {
                luaL_error(t->L, "mcs.add configuration missing '%s' function", fname);
            }
            if (use_init) {
                int acount = 0;
                // We immediately call the function and use the result
                // function for what we actually call.
                if (arg) {
                    lua_pushvalue(t->L, arg);
                    acount = 1;
                }
                if (lua_pcall(t->L, acount, 1, 0) != LUA_OK) {
                    luaL_error(t->L, "mcs.add failed to call '%s' function for init: %s", fname, lua_tostring(L, -1));
                }
                if (lua_type(t->L, -1) != LUA_TFUNCTION) {
                    luaL_error(t->L, "mcs.add init function '%s' must return a function", fname);
                }
                // function and arguments are removed from stack, leaving the
                // final function for us to reference below.
            }
            f->func_ref = luaL_ref(t->L, LUA_REGISTRYINDEX);

            // pull data from table into *f
            f->fname = strdup(fname);

            f->rate = frate;
            if (start_rate != 0) {
                uint64_t start_offset = start_rate * x;
                f->rate.start.tv_sec = start_offset / NSEC_PER_SEC;
                if (start_offset > NSEC_PER_SEC) {
                    f->rate.start.tv_nsec = start_offset - (f->rate.start.tv_sec * NSEC_PER_SEC);
                } else {
                    f->rate.start.tv_nsec = start_offset;
                }
            }
            f->reconn = freconn;
            // first run counts against the limiter
            f->limit = limit;

            memcpy(&f->c.conn, &ctx->conn, sizeof(f->c.conn));
        }
        if (arg) {
            lua_pop(t->L, 1);
        }
    }

    return 0;
}

static struct mcs_func *mcs_add_custom_func(struct mcs_thread *t) {
    // create coroutine using thread VM.
    struct mcs_func *f = lua_newuserdatauv(t->L, sizeof(struct mcs_func), 0);
    memset(f, 0, sizeof(struct mcs_func));
    STAILQ_INSERT_TAIL(&t->func_runlist, f, next_run);
    STAILQ_INSERT_TAIL(&t->func_list, f, next_func);
    t->active_funcs++;

    f->active = true;
    f->parent = t;

    // prepare the function's coroutine
    lua_newthread(t->L);
    lua_State *Lc = lua_tothread(t->L, -1);
    f->L = Lc;
    // pops thread
    f->self_ref_coro = luaL_ref(t->L, LUA_REGISTRYINDEX);
    // pop the func
    f->self_ref = luaL_ref(t->L, LUA_REGISTRYINDEX);

    // kick off the state machine.
    f->ev.qcb = mcs_cfunc_run;
    f->ev.cb = mcs_queue_cb;
    f->ev.udata = f;
    f->state = mcs_fstate_run;

    return f;
}

static int mcslib_add_custom(lua_State *L) {
    struct mcs_ctx *ctx = *(struct mcs_ctx **)lua_getextraspace(L);
    luaL_checktype(L, 1, LUA_TUSERDATA);
    struct mcs_thread *t = lua_touserdata(L, 1);
    luaL_checktype(L, 2, LUA_TTABLE);
    int arg_type = lua_type(L, 3);

    const char *fname = NULL;
    if (lua_getfield(L, 2, "func") != LUA_TNIL) {
        fname = lua_tostring(L, -1);
    }
    lua_pop(L, 1);
    if (fname == NULL) {
        luaL_error(L, "mcs.add_custom call missing 'func' argument");
    }

    int arg_offset = 0;
    if (arg_type != LUA_TNONE) {
        // expects argument table to be in slot -1
        _mcs_copy_table(L, t->L);
        arg_offset = lua_absindex(t->L, -1);
    }

    struct mcs_func *f = mcs_add_custom_func(t);

    lua_getglobal(t->L, fname);
    if (lua_isnil(t->L, -1)) {
        luaL_error(L, "mcs.add_custom configuration missing '%s' function", fname);
    }
    f->func_ref = luaL_ref(t->L, LUA_REGISTRYINDEX);

    f->fname = strdup(fname);
    memcpy(&f->c.conn, &ctx->conn, sizeof(f->c.conn));

    if (arg_offset) {
        lua_pushvalue(t->L, arg_offset);
        f->arg_ref = luaL_ref(t->L, LUA_REGISTRYINDEX);
    }
    // ensure we don't leak the argument table (or anything else)
    lua_settop(t->L, 0);

    return 0;
}

static void _mcs_cleanup_thread(struct mcs_thread *t) {
    struct mcs_func *f = NULL;

    STAILQ_FOREACH(f, &t->func_list, next_func) {
        if (f->c.mcmc) {
            mcmc_disconnect(f->c.mcmc);
        }
        free(f->c.rb.data);
        free(f->c.wb.data);
        free(f->c.mcmc);
        free(f->fname);
        // TODO: if we upgrade to 5.4.6 use closethread instead. resetthread
        // is deprecated.
        lua_resetthread(f->L);
        luaL_unref(t->L, LUA_REGISTRYINDEX, f->self_ref);
        luaL_unref(t->L, LUA_REGISTRYINDEX, f->self_ref_coro);
        luaL_unref(t->L, LUA_REGISTRYINDEX, f->func_ref);
        luaL_unref(t->L, LUA_REGISTRYINDEX, f->arg_ref);
        // do not free the function: it's owned by the lua state
    }
    STAILQ_INIT(&t->func_list);
    STAILQ_INIT(&t->func_runlist);
    // run the lua VM GC a couple times to kick free any pending
    // objects.
    lua_gc(t->L, LUA_GCCOLLECT);
    lua_gc(t->L, LUA_GCCOLLECT);
    // "have you turned it off on and again?" "uhhhuh." "three times?"
    lua_gc(t->L, LUA_GCCOLLECT);

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

    int type = lua_type(L, 2);
    struct timespec wait;
    bool use_wait = false;;

    if (type == LUA_TNUMBER) {
        int tosleep = lua_tointeger(L, 2);
        if (tosleep != 0) {
            clock_gettime(CLOCK_REALTIME, &wait);
            wait.tv_nsec = 0;
            wait.tv_sec += tosleep;
            use_wait = true;
        }
    } else if (type != LUA_TNONE && type != LUA_TNIL) {
        return luaL_error(L, "second argument to mcs.shredder must be numeric");
    }

    bool stop_sent = false;
    while (ctx->active_threads) {
        if (use_wait) {
            int res = pthread_cond_timedwait(&ctx->wait_cond, &ctx->wait_lock, &wait);
            if (res == ETIMEDOUT || ctx->stop) {
                // loosely signal threads to stop
                // note lack of locking making this imperfect
                STAILQ_FOREACH(t, &ctx->threads, next) {
                    t->stop = true;
                }
                use_wait = false;
                stop_sent = true;
                continue; // retry the loop.
            }
        } else {
            pthread_cond_wait(&ctx->wait_cond, &ctx->wait_lock);
            if (ctx->stop && !stop_sent) {
                STAILQ_FOREACH(t, &ctx->threads, next) {
                    t->stop = true;
                }
                stop_sent = true;
            }
        }
        if (ctx->active_threads == 0) {
            pthread_mutex_unlock(&ctx->wait_lock);
            break;
        }
    }

    // cleanup loop
    STAILQ_FOREACH(t, &ctx->threads, next) {
        // FIXME: assuming success.
        pthread_join(t->tid, NULL);
    }

    return 0;
}

static int _tokenize(struct mcs_func_resp *r) {
    const char *s = r->buf;
    int len = r->resp.reslen - 2;
    int max = PARSER_MAX_TOKENS;

    if (len > PARSER_MAXLEN) {
        len = PARSER_MAXLEN;
    }

    const char *end = s + len;
    int curtoken = 0; // token 0 always starts at 0.

    int state = 0;
    while (s != end) {
        switch (state) {
            case 0:
                // scanning for first non-space to find a token.
                if (*s != ' ') {
                    r->tokens[curtoken] = s - r->buf;
                    if (++curtoken == max) {
                        s++;
                        state = 2;
                        break;
                    }
                    state = 1;
                }
                s++;
                break;
            case 1:
                // advance over a token
                if (*s != ' ') {
                    s++;
                } else {
                    state = 0;
                }
                break;
            case 2:
                // hit max tokens before end of the line.
                // keep advancing so we can place endcap token.
                if (*s == ' ') {
                    goto endloop;
                }
                s++;
                break;
        }
    }
endloop:

    // endcap token so we can quickly find the length of any token by looking
    // at the next one.
    r->tokens[curtoken] = s - r->buf;
    r->ntokens = curtoken;

    return 0;

}

static int _token_len(struct mcs_func_resp *r, int token) {
    const char *s = r->buf + r->tokens[token];
    const char *e = r->buf + r->tokens[token+1];
    // start of next token is after any space delimiters, so back those out.
    while (*(e-1) == ' ') {
        e--;
    }
    return e - s;
}

// TODO: metatable with gc routine for freeing conn states.
// TODO: if 1 is TNIL use defaults.
static int mcslib_client_new(lua_State *L) {
    struct mcs_thread *t = *(struct mcs_thread **)lua_getextraspace(L);
    luaL_checktype(L, 1, LUA_TTABLE);
    struct mcs_func_client *c = lua_newuserdatauv(L, sizeof(struct mcs_func_client), 0);
    memset(c, 0, sizeof(*c));
    luaL_getmetatable(L, "mcs.client");
    lua_setmetatable(L, -2);

    // seed the socket parameters and allow overrides.
    memcpy(&c->conn, &t->ctx->conn, sizeof(t->ctx->conn));

    // TODO: throw error if strings too long.
    if (lua_getfield(L, 1, "host") != LUA_TNIL) {
        size_t len = 0;
        const char *host = lua_tolstring(L, -1, &len);
        strncpy(c->conn.host, host, NI_MAXHOST);
    }
    lua_pop(L, 1);

    if (lua_getfield(L, 1, "port") != LUA_TNIL) {
        size_t len = 0;
        const char *port = lua_tolstring(L, -1, &len);
        strncpy(c->conn.port_num, port, NI_MAXSERV);
    }
    lua_pop(L, 1);

    // TODO: buffer presizing? limits?
    // allocate mcmc client
    c->mcmc = calloc(1, mcmc_size(MCMC_OPTION_BLANK));

    mcs_buf_init(&c->wb, WBUF_INITIAL_SIZE);
    mcs_buf_init(&c->rb, RBUF_INITIAL_SIZE);
    mcs_client_init(c);
#ifdef USE_TLS
    if (t->ctx->tls_ctx) {
        mcs_tls_client_init(c, t->ctx->tls_ctx);
    }
#endif

    // return client object.
    return 1;
}

static int mcslib_client_connect(lua_State *L) {
    struct mcs_func_client *c = lua_touserdata(L, 1);
    int ret = mcs_connect(c);
    if (ret == 0) {
        // TODO: pushvalue to copy the userdata or we good here?
        lua_pushinteger(L, mcs_luayield_c_conn);
        return lua_yield(L, 2);
    } else {
        // Failed to connect.
        lua_pushboolean(L, 0);
        return 1;
    }

    return 0;
}

// yield new state for calling mcs_read_buf but against an external
// client object.
static int mcslib_client_read(lua_State *L) {
    luaL_checktype(L, 1, LUA_TUSERDATA);
    luaL_checktype(L, 2, LUA_TUSERDATA);
    lua_pushinteger(L, mcs_luayield_c_read);
    return lua_yield(L, 3);
}

// yield and read with a special handler that just looks for \r\n.
// chomp newlines and return string to lua.
// TODO: also add a readline_split() to auto-split the line?
static int mcslib_client_readline(lua_State *L) {
    luaL_checktype(L, 1, LUA_TUSERDATA);
    lua_pushinteger(L, mcs_luayield_c_readline);
    return lua_yield(L, 2);
}

static int mcslib_client_write(lua_State *L) {
    luaL_checktype(L, 1, LUA_TUSERDATA);
    lua_pushinteger(L, mcs_luayield_c_write);
    return lua_yield(L, 2);
}

static int mcslib_client_write_factory(lua_State *L) {
    luaL_checktype(L, 1, LUA_TUSERDATA);
    lua_pushinteger(L, mcs_luayield_c_write_factory);
    return lua_yield(L, 2);
}

static int mcslib_client_flush(lua_State *L) {
    luaL_checktype(L, 1, LUA_TUSERDATA);
    lua_pushinteger(L, mcs_luayield_c_flush);
    return lua_yield(L, 2);
}

static int mcslib_client_connected(lua_State *L) {
    struct mcs_func_client *c = lua_touserdata(L, 1);
    lua_pushboolean(L, c->connected);
    return 1;
}

// TODO: minimal argument validation?
// since this is a benchmark tool we should attempt to minmax, and argument
// checking does take measurable time.
static int mcslib_write(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_write);
    return lua_yield(L, 2);
}

static int mcslib_write_factory(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_write_factory);
    return lua_yield(L, 2);
}

static int mcslib_flush(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_flush);
    return lua_yield(L, 1);
}

static int mcslib_read(lua_State *L) {
    luaL_checktype(L, 1, LUA_TUSERDATA);
    lua_pushinteger(L, mcs_luayield_read);
    return lua_yield(L, 2);
}

static int mcslib_sleep_millis(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_sleep);
    return lua_yield(L, 1);
}

static int mcslib_out_wait(lua_State *L) {
    lua_pushinteger(L, mcs_luayield_outwait);
    return lua_yield(L, 1);
}

// max number of lines we can atomically output at once.
// hard to dump a full set of stats at once, but that's probably not what this
// is for.
// Might have to rethink otherwise.
#define OUT_MAXLINES 64

// If first argument is a table, iterate through and copy into result buffer.
// Adds \n if line doesn't end with one.
// For multiple args, appends strings via \t (like lua print) and caps with
// \n if missing.
// Table must be an array type.
static int mcslib_out(lua_State *L) {
    struct mcs_thread *t = *(struct mcs_thread **)lua_getextraspace(L);
    struct mcs_ctx *ctx = t->ctx;
    struct mcs_lock_buf *ob = &ctx->out_buf;
    int argc = lua_gettop(L);
    size_t sizes[OUT_MAXLINES];
    const char *lines[OUT_MAXLINES];
    size_t newlen = 0;
    int count = 0;
    bool table = false;
    // We do the lua bits before locking the buffer because any lua related
    // failure would cause us to jump out of the function with the lock held.

    if (argc == 1 && lua_type(L, 1) == LUA_TTABLE) {
        int tlen = lua_rawlen(L, 1);
        if (tlen > OUT_MAXLINES) {
            tlen = OUT_MAXLINES;
        }
        for (int x = 1; x <= tlen; x++) {
            size_t len;
            lua_rawgeti(L, 1, x);
            lines[x-1] = luaL_tolstring(L, -1, &len);
            sizes[x-1] = len;
            newlen += len;
        }
        table = true;
        newlen += tlen * 2;
        count = tlen;
    } else {
        if (argc > OUT_MAXLINES) {
            argc = OUT_MAXLINES;
        }
        for (int x = 1; x <= argc; x++) {
            size_t len;
            const char *arg = luaL_tolstring(L, x, &len);
            sizes[x-1] = len;
            lines[x-1] = arg;
            newlen += len;
        }
        newlen += argc * 2;
        count = argc;
    }

    pthread_mutex_lock(&ctx->out_buf.lock);
    mcs_check_buf(&ob->buf, newlen);
    char *p = ob->buf.data + ob->buf.used;
    char *start = p;
    bool more = false;

    // expecting each entry in the table is its own line.
    for (int x = 0; x < count; x++) {
        if (!table && more) {
            *p = '\t';
            p++;
        }
        memcpy(p, lines[x], sizes[x]);
        p += sizes[x];
        if (table && *(p-1) != '\n') {
            *p = '\n';
            p++;
        }

        more = true;
    }
    if (*(p-1) != '\n') {
        *p = '\n';
        p++;
    }

    ob->buf.used += p - start;

    // notify.
    uint64_t u = 1;
    if (write(ctx->out_fd, &u, sizeof(uint64_t)) != sizeof(uint64_t)) {
        perror("Failed to write to eventfd somehow");
        abort();
    }
    pthread_mutex_unlock(&ctx->out_buf.lock);

    return 0;
}

// TODO: could perf-golf this by ticking toconsume and avoiding memmove here.
// in mcs.out do the memmove if toconsume is nonzero. Lets the reader
// fast-loop without memmove'ing.
// Realistically there aren't enough lines moving through this thing to
// matter.
static int mcslib_out_readline(lua_State *L) {
    struct mcs_thread *t = *(struct mcs_thread **)lua_getextraspace(L);
    struct mcs_ctx *ctx = t->ctx;
    struct mcs_lock_buf *lb = &ctx->out_buf;

    // Technically a bit sketchy since we call lua with the lock held, but I
    // doubt this will fail in practice.
    // A potential fix is to lock twice: once to find the end string and again
    // to memmove afterward... but if we have multiple readers this will break
    // too.
    pthread_mutex_lock(&lb->lock);
    struct mcs_buf *ob = &lb->buf;
    if (ob->used > 0) {
        char *end = memchr(ob->data, '\n', ob->used);
        if (end != NULL) {
            size_t len = end - ob->data;
            lua_pushlstring(L, ob->data, len); // strip the \n
            ob->used -= len+1; // but remember to skip it.
            if (ob->used) {
                memmove(ob->data, ob->data+len+1, ob->used);
            }
        } else {
            lua_pushnil(L);
        }
    } else {
        lua_pushnil(L);
    }
    pthread_mutex_unlock(&lb->lock);

    return 1;
}

// A mouthful of a name for a very specific accelerator function.
// Takes destination client, res object
static int mcslib_client_write_mgres_to_ms(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, 1);
    struct mcs_func_client *c = lua_touserdata(L, 2);
    // tokenize the response to simplify rewriting the response.
    if (!r->ntokens) {
        _tokenize(r);
    }

    // Only convert VA responses.
    if (r->resp.reslen < 2 || strncmp(r->buf, "VA", 2) != 0) {
        lua_pushboolean(L, 0);
        return 1;
    }

    // make sure we have headroom in the dest wbuf
    mcs_check_buf(&c->wb, r->resp.reslen + r->resp.vlen);

    // find the key token from response
    int x = 1; // index of first flag
    char *token = NULL;
    int tlen = 0;
    for (; x < r->ntokens; x++) {
        if (r->buf[r->tokens[x]] == 'k') {
            token = r->buf + r->tokens[x];
            tlen = _token_len(r, x);
            break;
        }
    }

    // write "ms key len"
    char *p = c->wb.data + c->wb.used;
    memcpy(p, "ms ", 3);
    p += 3;

    memcpy(p, token+1, tlen-1); // skip the 'k' flag
    p += tlen-1;

    *p = ' ';
    p++;

    // write value length
    if (r->resp.vlen < 2) {
        fprintf(stderr, "HALT\n");
    }
    p = itoa_u32(r->resp.vlen-2, p);

    *p = ' ';
    p++;

    // loop tokens, rewrite:
    // f -> F
    // t -> T (t-1 -> T0)
    for (x = 1; x < r->ntokens; x++) {
        token = r->buf + r->tokens[x];
        tlen = _token_len(r, x);
        switch (r->buf[r->tokens[x]]) {
            case 'f':
                *p = 'F';
                p++;
                memcpy(p, token+1, tlen-1);
                p += tlen-1;

                *p = ' ';
                p++;
                break;
            case 't':
                *p = 'T';
                p++;
                if (token[1] == '-') {
                   *p = '0';
                   p++;
                } else {
                    memcpy(p, token+1, tlen-1);
                    p += tlen-1;
                }
                *p = ' ';
                p++;
                break;
            case 'O':
                *p = 'O';
                p++;
                memcpy(p, token+1, tlen-1);
                p += tlen-1;

                *p = ' ';
                p++;
                break;
        }
    }

    // add ME flag
    // TODO: make set mode optional.
    memcpy(p, "q ME\r\n", 6);
    p += 6;

    // copy the value data, which already includes "\r\n"
    memcpy(p, r->resp.value, r->resp.vlen);
    p += r->resp.vlen;

    // note buffer usage.
    c->wb.used += p - (c->wb.data + c->wb.used);

    lua_pushboolean(L, 1);
    return 1;
}

static int mcslib_resline(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -1);
    size_t len = r->resp.reslen;
    if (r->buf[len-2] == '\r' && r->buf[len-1] == '\n') {
        len -= 2;
    }
    lua_pushlstring(L, r->buf, len);
    return 1;
}

// just return the empty result object.
static int mcslib_res_new(lua_State *L) {
    struct mcs_func_resp *r = lua_newuserdatauv(L, sizeof(struct mcs_func_resp), 0);
    memset(r, 0, sizeof(*r));
    return 1;
}

// static/const strings passed in avoid allocations in lua.
static int mcslib_res_startswith(lua_State *L) {
    size_t len = 0;
    struct mcs_func_resp *r = lua_touserdata(L, 1);
    const char *str = lua_tolstring(L, 2, &len);

    if (r->resp.reslen >= len && (strncmp(r->buf, str, len) == 0)) {
        lua_pushboolean(L, 1);
    } else {
        lua_pushboolean(L, 0);
    }
    return 1;
}

static int mcslib_res_len(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -1);
    lua_pushinteger(L, r->resp.reslen + r->resp.vlen);
    return 1;
}

static int mcslib_res_ntokens(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -1);
    if (!r->ntokens) {
        _tokenize(r);
    }
    lua_pushinteger(L, r->ntokens-1);
    return 1;
}

static int mcslib_res_token(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -2);
    int n = lua_tointeger(L, -1) - 1; // keep lua array semantics. 1 index.
    if (!r->ntokens) {
        _tokenize(r);
    }

    if (n >= r->ntokens) {
        // TODO: error, maybe?
        return 0;
    }

    int len = _token_len(r, n);
    lua_pushlstring(L, r->buf + r->tokens[n], len);
    return 1;
}

static int mcslib_res_split(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -1);
    if (!r->ntokens) {
        _tokenize(r);
    }
    lua_newtable(L);
    for (int x = 0; x < r->ntokens; x++) {
        lua_pushlstring(L, r->buf + r->tokens[x], _token_len(r, x));
        lua_rawseti(L, -2, x+1); // lua arrays are 1 indexed.
    }

    return 1;
}

static int mcslib_res_hasflag(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -2);
    size_t len = 0;
    const char *flag = lua_tolstring(L, -1, &len);
    if (!r->ntokens) {
        _tokenize(r);
    }

    int start = 1; // index of first flag
    int found = 0;
    if (r->resp.vlen != 0) {
        start = 2; // skip past VA [len]
    }
    for (int x = start; x < r->ntokens; x++) {
        if (r->buf[r->tokens[x]] == flag[0]) {
            found = 1;
            break;
        }
    }

    if (found) {
        lua_pushboolean(L, 1);
    } else {
        lua_pushboolean(L, 0);
    }
    return 1;
}

static int mcslib_res_flagtoken(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -2);
    size_t len = 0;
    const char *flag = lua_tolstring(L, -1, &len);
    if (!r->ntokens) {
        _tokenize(r);
    }

    int x = 1; // index of first flag
    char *token = NULL;
    int tlen = 0;
    if (r->resp.vlen != 0) {
        x = 2; // skip past VA [len]
    }
    for (; x < r->ntokens; x++) {
        if (r->buf[r->tokens[x]] == flag[0]) {
            token = r->buf + r->tokens[x];
            tlen = _token_len(r, x);
            break;
        }
    }

    if (token) {
        lua_pushboolean(L, 1);
        if (tlen > 1) {
            lua_pushlstring(L, token+1, tlen-1);
            return 2;
        } else {
            return 1;
        }
    } else {
        lua_pushboolean(L, 0);
        return 1;
    }
}

static int mcslib_res_statname(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -1);
    lua_pushlstring(L, r->resp.sname, r->resp.snamelen);
    return 1;
}

static int mcslib_res_stat(lua_State *L) {
    struct mcs_func_resp *r = lua_touserdata(L, -1);
    lua_pushlstring(L, r->resp.stat, r->resp.statlen);
    return 1;
}

// TODO: func should look at the type of request *req is, and ensure the
// response received in *res makes sense, and the key or opaque matches if
// supplied.
static int mcslib_match(lua_State *L) {
    struct mcs_func_req *req = lua_touserdata(L, 1);
    struct mcs_func_resp *res = lua_touserdata(L, 2);
    int failed = 0;

    if (res->resp.vlen != 0) {
        if (_mcs_check_value(req, res) != 0) {
            failed = 1;
        }
    }

    if (failed) {
        lua_pushboolean(L, 0);
    } else {
        lua_pushboolean(L, 1);
    }

    int elapsed = (res->received.tv_sec - req->start.tv_sec) * 1000000 +
        (res->received.tv_nsec - req->start.tv_nsec) / 1000; // nano to micro
    lua_pushinteger(L, elapsed);
    return 2;
}

// TODO:
// do I care enough about cas/gets/gat/etc? I don't think so? maybe not right
// this moment.

// get(prefix, number)
static int mcslib_get(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    char *key = NULL;
    int klen = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "get ", 4);
    p += 4;

    key = p;
    memcpy(p, pfx, len);
    p += len;

    long num = lua_tointeger(L, 2);

    if (num > -1) {
        p = itoa_64(num, p);
    }
    klen = p - key;
    req->hash = XXH3_64bits(key, klen);

    memcpy(p, "\r\n", 2);
    p += 2;

    req->flat.len = p - req->data;
    req->flat.vlen = 0;

    return 1;
}

// delete(prefix, number)
static int mcslib_delete(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    char *key = NULL;
    int klen = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "delete ", 7);
    p += 4;

    key = p;
    memcpy(p, pfx, len);
    p += len;

    long num = lua_tointeger(L, 2);

    if (num > -1) {
        p = itoa_64(num, p);
    }
    klen = p - key;
    req->hash = XXH3_64bits(key, klen);

    memcpy(p, "\r\n", 2);
    p += 2;

    req->flat.len = p - req->data;
    req->flat.vlen = 0;

    return 1;
}

// touch(prefix, number, ttl)
static int mcslib_touch(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    char *key = NULL;
    int klen = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "touch ", 6);
    p += 4;

    key = p;
    memcpy(p, pfx, len);
    p += len;

    long num = lua_tointeger(L, 2);

    if (num > -1) {
        p = itoa_64(num, p);
    }
    klen = p - key;
    req->hash = XXH3_64bits(key, klen);

    int ttl = lua_tointeger(L, 3);
    *p = ' ';
    p++;
    p = itoa_32(ttl, p);

    memcpy(p, "\r\n", 2);
    p += 2;

    req->flat.len = p - req->data;
    req->flat.vlen = 0;

    return 1;
}

// TODO: xxhash the full key and use that for the value pattern.
// TODO: noreply support.
static int mcslib_set(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);

    size_t len = 0;
    char *key = NULL;
    int klen = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "set ", 4);
    p += 4;

    key = p;
    memcpy(p, pfx, len);
    p += len;

    long num = lua_tointeger(L, 2);

    if (num > -1) {
        p = itoa_64(num, p);
    }
    klen = p - key;
    req->hash = XXH3_64bits(key, klen);

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

    req->flat.vlen = lua_tointeger(L, 5);
    p = itoa_32(req->flat.vlen, p);

    memcpy(p, "\r\n", 2);
    p += 2;

    req->flat.len = p - req->data;

    return 1;
}

// new: prefix, number, vlen, flag, flag, etc
// if -1 passed as num, do not append number
// flag can be a set "a b c" or "a", "b", "C400" individually
static int mcslib_ms(lua_State *L) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);
    int argc = lua_gettop(L);

    size_t len = 0;
    char *key = NULL;
    int klen = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "ms ", 3);
    p += 3;

    key = p;
    memcpy(p, pfx, len);
    p += len;

    long num = lua_tointeger(L, 2);

    if (num > -1) {
        p = itoa_64(num, p);
    }
    klen = p - key;
    req->hash = XXH3_64bits(key, klen);

    *p = ' ';
    p++;

    req->flat.vlen = lua_tointeger(L, 3);
    p = itoa_32(req->flat.vlen, p);

    for (int x = 4; x < argc; x++) {
        const char *flags = lua_tolstring(L, x, &len);
        if (len) {
            *p = ' ';
            p++;
            memcpy(p, flags, len);
            p += len;
        }
    }

    memcpy(p, "\r\n", 2);
    p += 2;

    req->flat.len = p - req->data;

    return 1;
}

// TODO: automatically create and append an Opaque token for matching.
// arguments:
//  - key prefix
//  - numeric to append to key. if -1 do not append anything
//  - N arguments for flags to append. use instead of creating strings in lua
//  first, if possible
static int _mcslib_basic(lua_State *L, char cmd) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + REQ_MAX_LENGTH, 0);
    int argc = lua_gettop(L);

    size_t len = 0;
    char *key = NULL;
    int klen = 0;
    const char *pfx = lua_tolstring(L, 1, &len);

    char *p = req->data;
    memcpy(p, "m  ", 3);
    p[1] = cmd;
    p += 3;

    key = p;
    memcpy(p, pfx, len);
    p += len;

    long num = lua_tointeger(L, 2);

    if (num > -1) {
        p = itoa_64(num, p);
    }
    klen = p - key;
    req->hash = XXH3_64bits(key, klen);

    for (int x = 3; x < argc; x++) {
        const char *flags = lua_tolstring(L, x, &len);
        if (len) {
            *p = ' ';
            p++;
            memcpy(p, flags, len);
            p += len;
        }
    }

    memcpy(p, "\r\n", 2);
    p += 2;

    req->flat.len = p - req->data;
    req->flat.vlen = 0;

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

static int _mcslib_basic_factory(lua_State *L, char cmd) {
    struct mcs_func_req *req = lua_newuserdatauv(L, sizeof(struct mcs_func_req) + FACTORY_LENGTH, 0);
    int argc = lua_gettop(L);

    size_t len = 0;
    req->type = mcs_req_type_factory;
    const char *pfx = lua_tolstring(L, 1, &len);

    req->fac.cmd = cmd;

    char *p = req->data;

    // TODO: we can afford to do more thorough argument checking here.
    memcpy(p, pfx, len);
    req->fac.prefix_len = len;
    p += len;

    char *start = p;
    for (int x = 2; x < argc; x++) {
        const char *flags = lua_tolstring(L, x, &len);
        if (len) {
            *p = ' ';
            p++;
            memcpy(p, flags, len);
            p += len;
        }
    }

    memcpy(p, "\r\n", 2);
    p += 2;

    req->fac.postfix_len = p - start;

    return 1;
}

static int mcslib_mg_factory(lua_State *L) {
    return _mcslib_basic_factory(L, 'g');
}

static int mcslib_md_factory(lua_State *L) {
    return _mcslib_basic_factory(L, 'd');
}

static int mcslib_ma_factory(lua_State *L) {
    return _mcslib_basic_factory(L, 'a');
}

static int mcslib_ms_factory(lua_State *L) {
    return _mcslib_basic_factory(L, 's');
}

static int mcslib_time_millis(lua_State *L) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    lua_Integer t = now.tv_nsec / 1000000 + now.tv_sec * 1000;
    lua_pushinteger(L, t);
    return 1;
}

static int mcslib_stop(lua_State *L) {
    struct mcs_thread *t = *(struct mcs_thread **)lua_getextraspace(L);

    pthread_mutex_lock(&t->ctx->wait_lock);
    t->ctx->stop = true;
    pthread_cond_signal(&t->ctx->wait_cond);
    pthread_mutex_unlock(&t->ctx->wait_lock);

    return 0;
}

static int mcslib_client_gc(lua_State *L) {
    struct mcs_func_client *c = luaL_checkudata(L, -1, "mcs.client");
    // check routine just in case GC is called multiple times.
    if (c->mcmc) {
        mcmc_disconnect(c->mcmc);
        free(c->rb.data);
        free(c->wb.data);
        free(c->mcmc);
        c->mcmc = NULL;
    }
    return 0;
}

// TODO: use a differnt lib for main VM vs thread VM's?
// it should still be fine to use the same source file.
static void register_lua_libs(lua_State *L) {
    const struct luaL_Reg mcs_f [] = {
        {"thread", mcslib_thread},
        {"add", mcslib_add},
        {"add_custom", mcslib_add_custom},
        {"run", mcslib_add}, // FIXME: remove this in a week.
        {"shredder", mcslib_shredder},
        // custom func functions.
        {"client_new", mcslib_client_new},
        {"client_connect", mcslib_client_connect},
        {"client_read", mcslib_client_read},
        {"client_write", mcslib_client_write},
        {"client_write_factory", mcslib_client_write_factory},
        {"client_flush", mcslib_client_flush},
        {"client_readline", mcslib_client_readline},
        {"client_connected", mcslib_client_connected},
        {"client_write_mgres_to_ms", mcslib_client_write_mgres_to_ms},
        // func functions.
        {"time_millis", mcslib_time_millis},
        {"sleep_millis", mcslib_sleep_millis},
        {"out", mcslib_out},
        {"out_wait", mcslib_out_wait},
        {"out_readline", mcslib_out_readline},
        {"write", mcslib_write},
        {"write_factory", mcslib_write_factory},
        {"flush", mcslib_flush},
        {"read", mcslib_read},
        {"stop", mcslib_stop},
        // object functions.
        {"resline", mcslib_resline},
        {"res_new", mcslib_res_new},
        {"res_startswith", mcslib_res_startswith},
        {"res_ntokens", mcslib_res_ntokens},
        {"res_token", mcslib_res_token},
        {"res_split", mcslib_res_split},
        {"res_hasflag", mcslib_res_hasflag},
        {"res_flagtoken", mcslib_res_flagtoken},
        {"res_stat", mcslib_res_stat},
        {"res_statname", mcslib_res_statname},
        {"res_len", mcslib_res_len},
        {"match", mcslib_match},
        // request functions.
        {"get", mcslib_get},
        {"set", mcslib_set},
        {"touch", mcslib_touch},
        {"delete", mcslib_delete},
        {"mg", mcslib_mg},
        {"ms", mcslib_ms},
        {"md", mcslib_md},
        {"ma", mcslib_ma},
        {"mg_factory", mcslib_mg_factory},
        {"md_factory", mcslib_md_factory},
        {"ma_factory", mcslib_ma_factory},
        {"ms_factory", mcslib_ms_factory},
        {NULL, NULL}
    };

    const struct luaL_Reg mcs_client_m [] = {
        {"__gc", mcslib_client_gc},
        {NULL, NULL}
    };

    luaL_newmetatable(L, "mcs.client");
    lua_pushvalue(L, -1); // duplicate metatable.
    lua_setfield(L, -2, "__index"); // mt.__index = mt
    luaL_setfuncs(L, mcs_client_m, 0); // register methods
    lua_pop(L, 1); // drop metatable

    luaL_newlibtable(L, mcs_f);
    luaL_setfuncs(L, mcs_f, 0);
    lua_setglobal(L, "mcs"); // set lib table to global
}

// Splits an argument list into a lua table. Table is later passed into the
// config function.
static int _set_arguments(lua_State *L, const char *arg) {
    char *argcopy = strdup(arg); // copy arg to avoid editing commandline
    lua_newtable(L); // -> table
    char *b = NULL;
    for (char *p = strtok_r(argcopy, ",", &b);
            p != NULL;
            p = strtok_r(NULL, ",", &b)) {
        char *e =  NULL;
        char *name = strtok_r(p, "=", &e);
        lua_pushstring(L, name); // table -> key
        char *value = strtok_r(NULL, "=", &e);
        if (value == NULL) {
            lua_pushboolean(L, 1); // table -> key -> True
        } else {
            lua_pushstring(L, value); // table -> key -> value
        }
        lua_settable(L, 1); // -> table
    }
    free(argcopy);
    return luaL_ref(L, LUA_REGISTRYINDEX);
}

static void usage(struct mcs_ctx *ctx) {
    printf("usage:\n"
           "--ip=<addr> (127.0.0.1): IP to connect to\n"
           "--port=<port> (11211): Port to connect to\n"
           "--conf=<file> (none): Lua configuration file\n"
           "--arg=<key,key=val,key2=val2> (none): arguments to pass to config script\n"
#ifdef USE_TLS
           "--tls: use TLS for talking to memcached\n"
#endif
           "--memprofile: print allocation statistics once per second\n"
          );
    lua_getglobal(ctx->L, "help");
    if (!lua_isnil(ctx->L, -1)) {
        printf("usage from config file:\n");
        lua_pcall(ctx->L, 0, 0, 0);
    }
}

int main(int argc, char **argv) {
    struct mcs_conn conn = {.host = "127.0.0.1", .port_num = "11211"};
    const struct option longopts[] = {
        {"ip", required_argument, 0, 'i'},
        {"port", required_argument, 0, 'p'},
        // connect to unix socket instead
        {"sock", required_argument, 0, 's'},
        {"conf", required_argument, 0, 'c'},
        {"arg", required_argument, 0, 'a'},
        {"memprofile", no_argument, 0, 'm'},
        {"tls", no_argument, 0, 't'},
        {"help", no_argument, 0, 'h'},
        // end
        {0, 0, 0, 0}
    };
    int optindex;
    int c;

    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("failed to ignore SIGPIPE");
        exit(EXIT_FAILURE);
    }

    struct mcs_ctx *ctx = calloc(1, sizeof(struct mcs_ctx));
    pthread_mutex_init(&ctx->wait_lock, NULL);
    pthread_cond_init(&ctx->wait_cond, NULL);
    pthread_mutex_init(&ctx->out_buf.lock, NULL);
    ctx->out_fd = eventfd(0, EFD_NONBLOCK);
    if (ctx->out_fd == -1) {
        perror("failed to create notify eventfd");
        exit(EXIT_FAILURE);
    }
    ctx->out_buf.buf.data = malloc(BUF_INITIAL_SIZE);
    ctx->out_buf.buf.size = BUF_INITIAL_SIZE;

    // - create main VM
    lua_State *L = luaL_newstate();
    ctx->L = L;
    struct mcs_ctx **extra = lua_getextraspace(L);
    *extra = ctx;
    luaL_openlibs(L);
    register_lua_libs(L);

    while (-1 != (c = getopt_long(argc, argv, "", longopts, &optindex))) {
        switch (c) {
        case 'a':
            ctx->arg_ref = _set_arguments(L, optarg);
            break;
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
            ctx->conffile = strdup(optarg);
            if (luaL_dofile(L, ctx->conffile) != LUA_OK) {
                fprintf(stderr, "Failed to load config file: %s\n", lua_tostring(L, -1));
                exit(EXIT_FAILURE);
            }
            break;
#ifdef USE_TLS
        case 't':
            // TODO: err if nil
            ctx->tls_ctx = mcs_tls_init();
            break;
#endif
        case 'm':
            ctx->memprofile = true;
            break;
        case 'h':
            usage(ctx);
            return EXIT_SUCCESS;
            break;
        default:
            fprintf(stderr, "Unknown option\n");
            return EXIT_FAILURE;
        }
    }

    memcpy(&ctx->conn, &conn, sizeof(conn));
    if (ctx->conffile == NULL) {
        fprintf(stderr, "Must provide a config file: --conf etc.lua\n");
        exit(EXIT_FAILURE);
    }

    // - call "config" global cmd
    lua_getglobal(L, "config");

    if (lua_isnil(L, -1)) {
        fprintf(stderr, "Configuration missing 'config' function\n");
        exit(EXIT_FAILURE);
    }

    if (ctx->arg_ref) {
        lua_rawgeti(L, LUA_REGISTRYINDEX, ctx->arg_ref);
    } else {
        lua_newtable(L);
    }
    if (lua_pcall(L, 1, 0, 0) != LUA_OK) {
        fprintf(stderr, "Failed to execute config function: %s\n", lua_tostring(L, -1));
        exit(EXIT_FAILURE);
    }

    return EXIT_SUCCESS;
}
