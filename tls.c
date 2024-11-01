/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *  mc-shredder - burn-in load test client
 *
 *       https://github.com/memcached/mcshredder
 *
 *  Copyright 2024 Cache Forge LLC.  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.  See
 *  the LICENSE file for full text.
 *
 *  Authors:
 *      dormando <dormando@rydia.net>
 */

// General TODO's:
// - implement syserr callback:
//   - in mcs_syserror: if c.tls -> call back into tls for shutdown/free
// - on syserror cases, differentiate the errors a little (ie; closed socket
// vs syscall err vs ssl err
// - implement syserr handling for cfunc state machine
// - defines for state machine stop/go instead of true/false
// - enum for yield return (see cfunc_run)
// - graceful close: if c.tls/etc anywhere disconn is called, gracefully kill
// SSL object.
//
// PERFORMANCE NOTE:
// - in a simple test: 240k rps fetch with TLS, 350k without. haven't
// inspected yet.

#ifdef USE_TLS

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <stdbool.h>

#include <openssl/ssl.h>
#include <openssl/bio.h>
#include <openssl/err.h>

#include "mcshredder.h"
#include "tls.h"

// FIXME: needs to be higher than 2^14 to get a full TLS record in one go.
// Hopefully with a future custom BIO we can read directly into the BIO memory
// avoiding this extra bloat and data copy.
#define MCS_TLS_RBUF_SIZE 1<<16

struct mcs_tls_client {
    SSL *ssl;
    BIO *ibio; // staging input from network
    BIO *obio; // staging output to network
    int obio_remain; // remember our flush offset
    struct mcs_buf rb; // internal read buffer.
};

// initialize a global OpenSSL context to use with creating connection SSL
// objects.
void *mcs_tls_init(const char *chain_cert, const char *key, const char *ca_cert) {
    // TODO: check for a reasonable OpenSSL version.
    SSL_CTX *ctx = NULL;
    OPENSSL_init_ssl(0, NULL);
    ctx = SSL_CTX_new(TLS_client_method());
    if (ctx == NULL) {
        return NULL;
    }

    // disable older protocols
    SSL_CTX_set_min_proto_version(ctx, TLS1_3_VERSION);
    // TODO: check result in case of failure.

    // load certs if we were supplied with them
    if (chain_cert && key) {
        if (!SSL_CTX_use_certificate_chain_file(ctx, chain_cert)) {
            ERR_print_errors_fp(stderr);
            return NULL;
        } else if (!SSL_CTX_use_PrivateKey_file(ctx, key, SSL_FILETYPE_PEM)) {
            ERR_print_errors_fp(stderr);
            return NULL;
        } else if (ca_cert) {
            if (!SSL_CTX_load_verify_locations(ctx, ca_cert, NULL)) {
                ERR_print_errors_fp(stderr);
                return NULL;
            }
        }
    }
    return ctx;
}

enum mcs_tls_ret mcs_tls_do_handshake(struct mcs_tls_client *c) {
    if (SSL_is_init_finished(c->ssl)) {
        return MCS_TLS_OK;
    }

    ERR_clear_error();
    int n = SSL_do_handshake(c->ssl);
    if (n == 1) {
        return MCS_TLS_OK; // complete
    }

    int err = SSL_get_error(c->ssl, n);
    if (err == SSL_ERROR_WANT_READ ||
        err == SSL_ERROR_WANT_WRITE) {
        // FIXME: should be able to directly return WANT_WRITE/WANT_READ
        // properly from here using our internal buffers.
        // Our custom BIO should either have bytes in it or we need to read.
        ERR_clear_error();
        return MCS_TLS_WANT_IO;
    }

    // else some real error.
    ERR_print_errors_fp(stderr);
    ERR_clear_error();
    return MCS_TLS_ERROR;
}

bool mcs_tls_hs_postwrite(void *udata) {
    struct mcs_func *f = udata;
    int res = mcs_func_cqe_res(f);
    struct mcs_func_client *c = mcs_func_get_client(f);

    if (res > 0) {
        c->tls->obio_remain -= res;
        int remain = c->tls->obio_remain;
        if (remain == 0) {
            BIO_reset(c->tls->obio);
            // complete, retry tls run.
            return mcs_tls_postconnect(f, c);
        } else {
            char *data = NULL;
            long len = BIO_get_mem_data(c->tls->obio, &data);
            // still have something to write.
            _evset_wrflush_data(f, c, data + (len - remain), remain);
            return true;
        }
    } else {
        ERR_print_errors_fp(stderr);
        ERR_clear_error();
        mcs_func_set_state(f, mcs_fstate_syserr);
    }
    return false;
}

bool mcs_tls_hs_postread(void *udata) {
    struct mcs_func *f = udata;
    int res = mcs_func_cqe_res(f);
    struct mcs_func_client *c = mcs_func_get_client(f);

    if (res > 0) {
        BIO_write(c->tls->ibio, c->tls->rb.data, res);
        // don't need to move the pointers around since we've memcpy'd
        // everything useful here.
        // TODO: there is potential error handling and BIO_write() will tell
        // us how much was actually written; so deal with that.
        return mcs_tls_postconnect(f, c); // retry handshake.
    } else {
        // need to set syserr failure mode
        abort();
    }

    return false;
}

// TODO: if internal error, change func state to syserr
bool mcs_tls_postconnect(struct mcs_func *f, struct mcs_func_client *c) {
    enum mcs_tls_ret ret = mcs_tls_do_handshake(c->tls);

    if (ret == MCS_TLS_OK) {
        // good, call main postconnect routine
        mcs_func_set_state(f, mcs_fstate_postconnect);
        ERR_clear_error(); // clear for good measure.
        return false;
    } else if (ret == MCS_TLS_WANT_IO) {
        // need to check if we need to read or write.
        // sadly the handshake can say "WANT_READ" but will put bytes into the
        // output BIO, so we need to duck-type the return by first checking if
        // output bytes are pending.
        if (BIO_pending(c->tls->obio)) {
            // first do a write.
            // directly reference the data in the memory BIO to avoid dealing
            // with an extra buffer and data copy if possible.
            char *data = NULL;
            long len = BIO_get_mem_data(c->tls->obio, &data);
            c->tls->obio_remain = len;
            _evset_wrflush_data(f, c, data, len);
            mcs_func_set_state(f, mcs_fstate_tls_hs_postwrite);
            return true; // stop the parent state machine
        } else {
            // want read.
            struct mcs_buf *rb = &c->tls->rb;
            _evset_read_data(f, c, rb->data + rb->used, rb->size - rb->used);
            mcs_func_set_state(f, mcs_fstate_tls_hs_postread);
            return true; // stop parent state machine
        }
    } else {
        abort();
        // TODO: error. need to set state to syserr
    }
    return false; // don't stop the parent state machine
}

// FIXME: restrict amount of flushing at once to reduce memory footprint.
bool mcs_tls_flush(struct mcs_func *f, struct mcs_func_client *c) {
    char *todo = c->wb.data + c->wb.offset;
    int remain = c->wb.used - c->wb.offset;

    if (remain == 0) {
        c->wb.used = 0;
        c->wb.offset = 0;
        mcs_func_set_state(f, mcs_fstate_run);
        return false;
    }

    // Writes into the output memory BIO. We then need to run the syscall
    // on our own.
    int n = SSL_write(c->tls->ssl, todo, remain);

    if (n < 0) {
        // FIXME: WANT_IO
        abort();
    } else {
        if (n < remain) {
            abort(); // short read.
        }
        // advance the wbuf.
        c->wb.offset += n;

        char *data = NULL;
        long len = BIO_get_mem_data(c->tls->obio, &data);
        c->tls->obio_remain = len;
        _evset_wrflush_data(f, c, data, len);
        mcs_func_set_state(f, mcs_fstate_tls_postflush);
        return true; // stop state machine.
    }

    return false;
}

bool mcs_tls_postflush(struct mcs_func *f, struct mcs_func_client *c) {
    int res = mcs_func_cqe_res(f);

    if (res > 0) {
        c->tls->obio_remain -= res;
        int remain = c->tls->obio_remain;
        if (remain == 0) {
            BIO_reset(c->tls->obio);
            // complete, move on.
            mcs_func_set_state(f, mcs_fstate_tls_flush);
        } else {
            char *data = NULL;
            long len = BIO_get_mem_data(c->tls->obio, &data);
            // didn't flush everything.
            _evset_wrflush_data(f, c, data + (len - remain), remain);
            return true; // stop state machine
        }
    } else {
        abort(); // error handler.
    }

    return false;
}

bool mcs_tls_read(struct mcs_func *f, struct mcs_func_client *c) {
    struct mcs_buf *rb = &c->tls->rb;
    _evset_read_data(f, c, rb->data + rb->used, rb->size - rb->used);
    mcs_func_set_state(f, mcs_fstate_tls_postread);

    return true;
}

bool mcs_tls_postread(struct mcs_func *f, struct mcs_func_client *c) {
    int res = mcs_func_cqe_res(f);

    if (res > 0) {
        // Move our read buffer data into the encryption input buffer.
        BIO_write(c->tls->ibio, c->tls->rb.data, res);
        // Decrypt the data into our application read buffer.
        int n = SSL_read(c->tls->ssl, c->rb.data + c->rb.used, c->rb.size - c->rb.used);

        if (n < 0) {
            // TODO: ERR_clear_error()
            int err = SSL_get_error(c->tls->ssl, n);
            if (err == SSL_ERROR_WANT_WRITE ||
                err == SSL_ERROR_WANT_READ) {
                if (BIO_pending(c->tls->obio)) {
                    char *data = NULL;
                    long len = BIO_get_mem_data(c->tls->obio, &data);
                    _evset_wrflush_data(f, c, data, len);
                    abort(); // TODO: need another stupid state to ensure we
                             // do BIO_seek()?
                    return true; // stop the parent state machine
                } else {
                    // want read.
                    mcs_func_set_state(f, c->s_read);
                    return false; // bounce back through the read routine.
                }
            } else {
                ERR_print_errors_fp(stderr);
                abort(); // unknown error.
            }
        } else {
            mcs_func_set_cqe_res(f, n);
            mcs_func_set_state(f, mcs_fstate_postread);
            return false; // don't stop state machine
        }
    } else {
        abort();
        // error handler.
    }

    return true;
}

int mcs_tls_client_init(struct mcs_func_client *fc, void *ctx) {
    struct mcs_tls_client *c = calloc(1, sizeof(*c));
    c->ssl = SSL_new((SSL_CTX *)ctx);
    if (c->ssl == NULL)
        return -1;

    c->ibio = BIO_new(BIO_s_mem());
    c->obio = BIO_new(BIO_s_mem());
    mcs_buf_init(&c->rb, MCS_TLS_RBUF_SIZE);

    SSL_set_connect_state(c->ssl);
    SSL_set0_rbio(c->ssl, c->ibio);
    SSL_set0_wbio(c->ssl, c->obio);

    fc->tls = c;
    fc->s_read = mcs_fstate_tls_read;
    fc->s_flush = mcs_fstate_tls_flush;

    return 0;
}

#endif // USE_TLS
