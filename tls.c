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

#include "mcshredder.h"
#include "tls.h"

// TLS BUFFER FUNCS
// should be generalized into the mcs code.

struct mcs_tls_buf *mcs_tls_buf_alloc(int min) {
    struct mcs_tls_buf *b = NULL;
    size_t size = TLS_BUF_SIZE_DEFAULT;
    if (min > size) {
        size = min;
    }
    b = malloc(size + sizeof(*b));
    if (!b) {
        return NULL;
    }
    memset(b, 0, sizeof(*b));
    b->size = size;

    return b;
}

// OpenSSL BIO object start.
// Interfaces with the event loop for us.

// NOTE: BIO_(get|set)_data is _bio specific_ and should be safe to use, since
// we're not doing inheritance on an existing BIO type
// BIO_(get|set)_app_data is safer in other instances, and is aliased to
// BIO_(get|set)_ex_data(ptr, 0)
// ... but that seems to do some looping and allocation work, while
// BIO_(get|set)_data is just a pointer on the struct. So that's hopefully ok.

// This is called when SSL_read() is _reading_ from our input buffer.
// We write as much as we can into buf and return the actual length
// This can also be called by the handshake process.
static int mcs_bio_read(BIO *bio, char *buf, int blen) {
    struct mcs_tls_client *c = BIO_get_data(bio);
    //struct mcs_func_client *fc = (struct mcs_func_client *)((char *)c - offsetof(struct mcs_func_client, tls));

    if (buf == NULL) {
        return 0;
    }

    if (c->rbuf) {
        if (c->rbuf->used - c->rbuf->offset > 0) {
            char *off = c->rbuf->buf + c->rbuf->offset;
            int rem = c->rbuf->used - c->rbuf->offset;
            if (rem > blen) {
                rem = blen;
            }
            memcpy(buf, off, rem);
            c->rbuf->offset += rem;

            return rem;
        } else {
            free(c->rbuf);
            c->rbuf = NULL;
        }
    }
    BIO_clear_retry_flags(bio);
    BIO_set_retry_read(bio);

    // TODO: check our pre-existing input buffer for bytes and read them in.
    // FIXME: check bss_mem.c to see if this advances the read pointer as
    // well?

    // TODO: do we queue a read event directly if we got asked to read but
    // there was nothing here, or let SSL bubble that up via WANT_READ?
    return 0;
}

// This is called when SSL_write() or SSL_do_handshake() need to push bytes
// into our event loop. SSL_write() takes an input buffer and moves the data
// into an encrypted buffer here, which we must copy again and write out to a
// socket.
// TODO: figure out how this relates to the source data, so we can maybe just
// hold onto *buf and write it out directly?
static int mcs_bio_write(BIO *bio, const char *buf, int blen) {
    struct mcs_tls_client *c = BIO_get_data(bio);
    //struct mcs_func_client *fc = (struct mcs_func_client *)((char *)c - offsetof(struct mcs_func_client, tls));
    if (!c->wbuf) {
        c->wbuf = mcs_tls_buf_alloc(blen);
        if (!c->wbuf) {
            // FIXME: probalby incorrect.
            return -1;
        }
    }

    // FIXME: check if expansion needed.

    struct mcs_tls_buf *wb = c->wbuf;
    memcpy(wb->buf + wb->used, buf, blen);
    wb->used += blen;

    return blen;
}

// TODO: check when this is called and fill in.
static int mcs_bio_create(BIO *bio) {
    BIO_set_shutdown(bio, 1); // TODO: Sets BIO_CLOSE. double check this.
    // since we're setting the data ptr outside of this call, we should also
    // set init outside of here?
    BIO_set_init(bio, 1);
    return 1;
}

// TODO: check when this is called and fill in.
static int mcs_bio_destroy(BIO *bio) {
    if (bio) {
        return 1;
    }
    return 0;
}

// TODO: figure out howtf this is used.
static long mcs_bio_ctrl(BIO *bio, int cmd, long num, void *parg) {
    long ret = 0;
    switch (cmd) {
        case BIO_CTRL_FLUSH:
           ret = 1;
           break;
        case BIO_CTRL_EOF:
           ret = 1;
           break;
        default:
           ret = 0;
    }
    fprintf(stderr, "BIO ctrl command: %d [ret: %ld]\n", cmd, ret);
    return ret;
}

// OpenSSL BIO object end.

// initialize a global OpenSSL context to use with creating connection SSL
// objects.
SSL_CTX *mcs_tls_init(void) {
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
    return ctx;
}

int mcs_tls_client_init(SSL_CTX *ctx, struct mcs_tls_client *c) {
    // TODO: check for NULL's and return err
    c->ssl = SSL_new(ctx);
    if (c->ssl == NULL)
        return -1;

    // TODO: BIO_meth_free on close
    c->m = BIO_meth_new(BIO_TYPE_MEM, "MCS URING BIO");
    // TODO: cleanup routine?
    if (c->m == NULL)
        return -1;

    BIO_meth_set_read(c->m, mcs_bio_read);
    BIO_meth_set_write(c->m, mcs_bio_write);
    BIO_meth_set_ctrl(c->m, mcs_bio_ctrl);
    BIO_meth_set_create(c->m, mcs_bio_create);
    BIO_meth_set_destroy(c->m, mcs_bio_destroy);

    BIO *bio = BIO_new(c->m);
    if (bio == NULL) // TODO: cleanup
        return -1;

    BIO_set_data(bio, c);
    // See notes on the manpage for SSL_set0_rbio
    BIO_up_ref(bio);

    SSL_set_connect_state(c->ssl);
    SSL_set0_rbio(c->ssl, bio);
    SSL_set0_wbio(c->ssl, bio);
    c->bio = bio;

    return 0;
}

enum mcs_tls_ret mcs_tls_do_handshake(struct mcs_tls_client *c) {
    if (SSL_is_init_finished(c->ssl)) {
        return MCS_TLS_OK;
    }

    int n = SSL_do_handshake(c->ssl);
    if (n == 1) {
        return MCS_TLS_OK; // complete
    }

    // SSL_get_error
    int err = SSL_get_error(c->ssl, n);
    if (err == SSL_ERROR_WANT_READ ||
        err == SSL_ERROR_WANT_WRITE) {
        // FIXME: should be able to directly return WANT_WRITE/WANT_READ
        // properly from here using our internal buffers.
        // Our custom BIO should either have bytes in it or we need to read.
        return MCS_TLS_WANT_IO;
    }

    // else some real error.
    return MCS_TLS_ERROR;
}

// move bytes from fc->wbuf into internal BIO via SSL_write.
int mcs_tls_enqueue_encrypted(struct mcs_tls_client *c) {
    struct mcs_func_client *fc = (struct mcs_func_client *)((char *)c - offsetof(struct mcs_func_client, tls));
    char *todo = fc->wbuf + fc->wbuf_sent;
    int remain = fc->wbuf_used - fc->wbuf_sent;

    if (remain == 0) {
        return MCS_TLS_OK;
    }
    int n = SSL_write(c->ssl, todo, remain);

    if (n < 0) {
        int err = SSL_get_error(c->ssl, n);
        if (err == SSL_ERROR_WANT_WRITE ||
            err == SSL_ERROR_WANT_READ) {
            return MCS_TLS_WANT_IO;
        }
    } else {
        // SSL_write() should technically keep retrying, using the same input
        // arguments, until it "succeeds". Lets warn here to see if we ever
        // get short writes.
        if (n < remain) {
            fprintf(stderr, "SSL_write: SHORT READ: [n: %d] [rem: %d]\n", n, remain);
        }
        fc->wbuf_sent += n;
        if (fc->wbuf_sent == fc->wbuf_used) {
            fc->wbuf_sent = 0;
            fc->wbuf_used = 0;
        }
    }

    return MCS_TLS_OK;
}

int mcs_tls_enqueue_decrypted(struct mcs_tls_client *c) {
    struct mcs_func_client *fc = (struct mcs_func_client *)((char *)c - offsetof(struct mcs_func_client, tls));

    // rbuf is deallocated if empty.
    if (!c->rbuf) {
        return MCS_TLS_OK;
    }

    // FIXME: expand rbuf if full.
    int n = SSL_read(c->ssl, fc->rbuf + fc->rbuf_used, fc->rbuf_size - fc->rbuf_used);

    if (n < 0) {
        int err = SSL_get_error(c->ssl, n);
        if (err == SSL_ERROR_WANT_WRITE ||
            err == SSL_ERROR_WANT_READ) {
            return MCS_TLS_WANT_IO;
        }
    } else {
        fc->rbuf_used += n;
    }

    return MCS_TLS_OK;
}
