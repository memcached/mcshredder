#ifndef MCS_TLS_H
#define MCS_TLS_H

enum mcs_tls_ret {
    MCS_TLS_OK = 1,
    MCS_TLS_WANT_READ,
    MCS_TLS_WANT_WRITE,
    MCS_TLS_WANT_IO,
    MCS_TLS_ERROR,
};

struct mcs_tls_client;

#define TLS_BUF_SIZE_DEFAULT 8192

#ifdef USE_TLS
void *mcs_tls_init(void);
int mcs_tls_client_init(struct mcs_func_client *fc, void *ctx);
enum mcs_tls_ret mcs_tls_do_handshake(struct mcs_tls_client *c);
// FIXME: -> handshake?
bool mcs_tls_postconnect(struct mcs_func *f, struct mcs_func_client *c);
bool mcs_tls_hs_postwrite(void *udata);
bool mcs_tls_hs_postread(void *udata);
bool mcs_tls_read(struct mcs_func *f, struct mcs_func_client *c);
bool mcs_tls_postread(struct mcs_func *f, struct mcs_func_client *c);
bool mcs_tls_flush(struct mcs_func *f, struct mcs_func_client *c);
bool mcs_tls_postflush(struct mcs_func *f, struct mcs_func_client *c);
#else
#define mcs_tls_init(void)
#define mcs_tls_client_init(fc, ctx)
#define mcs_tls_postconnect(f, c) false
#define mcs_tls_hs_postwrite(u) false
#define mcs_tls_hs_postread(u) false
#define mcs_tls_read(f, c) false
#define mcs_tls_postread(f, c) false
#define mcs_tls_flush(f, c) false
#define mcs_tls_postflush(f, c) false
#endif // USE_TLS

#endif // MCS_TLS_H
