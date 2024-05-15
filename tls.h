#ifndef MCS_TLS_H
#define MCS_TLS_H

#include <openssl/ssl.h>
#include <openssl/bio.h>

enum mcs_tls_ret {
    MCS_TLS_OK = 1,
    MCS_TLS_WANT_READ,
    MCS_TLS_WANT_WRITE,
    MCS_TLS_WANT_IO,
    MCS_TLS_ERROR,
};

#define TLS_BUF_SIZE_DEFAULT 8192

// TODO: this structure should replace 'struct mcs_buf',
// with a third struct that has the lock + struct mcs_buf for the out
// interface
struct mcs_tls_buf {
    int used;
    int offset;
    int toconsume;
    size_t size;
    char buf[];
};

// NOTE: Think the BIO_METHOD can be shared. maybe put the SSL_CTX into a
// struct with it.
// Need to confirm, but am reasonably sure it's safe because much of the ossl
// code uses BIO_METHOD's via static globals.
struct mcs_tls_client {
    SSL *ssl;
    BIO_METHOD *m; // NOTE: see above
    BIO *bio; // Handles both input and output to event loop
    struct mcs_tls_buf *rbuf;
    struct mcs_tls_buf *wbuf;
};

SSL_CTX *mcs_tls_init(void);
int mcs_tls_client_init(SSL_CTX *ctx, struct mcs_tls_client *c);
enum mcs_tls_ret mcs_tls_do_handshake(struct mcs_tls_client *c);
int mcs_tls_enqueue_encrypted(struct mcs_tls_client *c);
int mcs_tls_enqueue_decrypted(struct mcs_tls_client *c);

struct mcs_tls_buf *mcs_tls_buf_alloc(int min);
#endif // MCS_TLS_H
