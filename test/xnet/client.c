/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-10-09 16:39:13 macan>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

#include "gk.h"
#include "xnet.h"
#include "mds.h"
#include "lib.h"
#include "root.h"

#ifdef UNIT_TEST
#define TYPE_MDS        0
#define TYPE_CLIENT     1
#define TYPE_ROOT       4

#define OP_CREATE       0
#define OP_LOOKUP       1
#define OP_UNLINK       2
#define OP_ALL          100

/* Global variable */
struct gk_client_info hci = {
    0, 0, 0, 0
};

char *ipaddr[] = {
    "127.0.0.1",              /* root */
};

short port[] = {
    8710,                       /* root */
    9900,                       /* client */
};

#define GK_TYPE(type, idx) ({                 \
            u64 __sid = -1UL;                   \
            switch (type){                      \
            case TYPE_MDS:                      \
                __sid = GK_MDS(idx);          \
                break;                          \
            case TYPE_CLIENT:                   \
                __sid = GK_CLIENT(idx);       \
                break;                          \
            case TYPE_ROOT:                     \
                __sid = GK_ROOT(idx);         \
                break;                          \
            default:;                           \
            }                                   \
            __sid;                              \
        })

static inline
u64 GK_TYPE_SEL(int type, int id)
{
    u64 site_id = -1UL;

    switch (type) {
    case TYPE_MDS:
        site_id = GK_MDS(id);
        break;
    case TYPE_CLIENT:
        site_id = GK_CLIENT(id);
        break;
    case TYPE_ROOT:
        site_id = GK_ROOT(id);
        break;
    default:;
    }

    return site_id;
}

int msg_wait()
{
    while (1) {
        xnet_wait_any(hmo.xc);
    }
    return 0;
}

int cli_do_put(u64, char *, char *, char *);

static inline
void __random_set(char *buf, int len)
{
    int i;

    for (i = 0; i < len; i++) {
        buf[i] = '0' + lib_random(52);
    }
}

int msg_send(int entry, int op, int base)
{
    lib_timer_def();
    int i, err = 0;
    char key[32], value[128];

    switch (op) {
    case OP_CREATE:
        lib_timer_B();
        for (i = 0; i < entry; i++) {
            memset(key, 0, sizeof(key));
            memset(value, 0, sizeof(value));
            sprintf(key, "key.%d", base + i);
            __random_set(value, 127);
            cli_do_put(GK_MDS(0), "ik141000000",
                       key, value);
        }
        lib_timer_E();
        lib_timer_O(entry, "Create Latency: ");
        break;
    case OP_LOOKUP:
        lib_timer_B();
        for (i = 0; i < entry; i++) {
            memset(key, 0, sizeof(key));
            memset(value, 0, sizeof(value));
            sprintf(key, "key.%d", base + i);
            cli_do_get(GK_MDS(0), "ik141000000",
                       key, NULL);
        }
        lib_timer_E();
        lib_timer_O(entry, "Lookup Latency: ");
        break;
    default:;
    }

    return err;
}

struct msg_send_args
{
    int tid, thread;
    int entry, op;
    pthread_barrier_t *pb;
};

pthread_barrier_t barrier;

void *__msg_send(void *arg)
{
    struct msg_send_args *msa = (struct msg_send_args *)arg;
    lib_timer_def();

    pthread_barrier_wait(msa->pb);
    if (msa->tid == 0)
        lib_timer_B();
    if (msa->op == OP_ALL) {
        msg_send(msa->entry, OP_CREATE, msa->tid * msa->entry);
        pthread_barrier_wait(msa->pb);
        if (msa->tid == 0) {
            lib_timer_E();
            lib_timer_O(msa->entry * msa->thread, "Create Aggr Lt: ");
            lib_timer_B();
        }
        msg_send(msa->entry, OP_LOOKUP, msa->tid * msa->entry);
        pthread_barrier_wait(msa->pb);
        if (msa->tid == 0) {
            lib_timer_E();
            lib_timer_O(msa->entry * msa->thread, "Lookup Aggr Lt: ");
            lib_timer_B();
        }
    } else {
        msg_send(msa->entry, msa->op, msa->tid * msa->entry);
        pthread_barrier_wait(msa->pb);
        if (msa->tid == 0) {
            lib_timer_E();
            lib_timer_O(msa->entry * msa->thread, "Aggr Latency: ");
        }
    }

    pthread_exit(0);
}

int msg_send_mt(int entry, int op, int thread)
{
    pthread_t pt[thread];
    struct msg_send_args msa[thread];
    int i, err = 0;

    memset(msa, 0, sizeof(msa));
    entry /= thread;

    for (i = 0; i < thread; i++) {
        msa[i].tid = i;
        msa[i].thread = thread;
        msa[i].entry = entry;
        msa[i].op = op;
        msa[i].pb = &barrier;
        err = pthread_create(&pt[i], NULL, __msg_send, &msa[i]);
        if (err)
            goto out;
    }

    for (i = 0; i < thread; i++) {
        pthread_join(pt[i], NULL);
    }
out:
    return err;
}

/* r2cli_do_hb()
 *
 * @gid: already right shift 2 bits
 */
static
int r2cli_do_hb(u64 request_site, u64 root_site, u64 fsid, u32 gid)
{
    struct xnet_msg *msg;
    union gk_x_info *hxi;
    int err = 0;

    hxi = (union gk_x_info *)&hci;
    
    /* alloc one msg and send it to the peer site */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(xnet, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out_nofree;
    }

    xnet_msg_fill_tx(msg, XNET_MSG_REQ, 0,
                     hmo.xc->site_id, root_site);
    xnet_msg_fill_cmd(msg, GK_R2_HB, request_site, fsid);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif
    xnet_msg_add_sdata(msg, hxi, sizeof(*hxi));

    msg->tx.reserved = gid;

    err = xnet_send(hmo.xc, msg);
    if (err) {
        gk_err(xnet, "xnet_send() failed\n");
        goto out;
    }
out:
    xnet_free_msg(msg);
out_nofree:
    
    return err;
}

void client_cb_hb(void *arg)
{
    int err = 0;

    err = r2cli_do_hb(hmo.xc->site_id, GK_ROOT(0), 0, 0);
    if (err) {
        gk_err(xnet, "hb %lx w/ r2 %x failed w/ %d\n",
               hmo.xc->site_id, GK_ROOT(0), err);
    }
}

/* cli_do_reg() for MDS
 *
 * @
 */
int cli_do_reg(u64 request_site, char *ip, int port)
{
    struct xnet_msg *msg;
    struct sockaddr_in sin;
    int err =0;

    sin.sin_port = htons(port);
    if (inet_aton(ip, &sin.sin_addr) == 0) {
        gk_err(xnet, "Invalid inet address %s\n", ip);
        err = -EINVAL;
        goto out_nofree;
    }

    /* alloc one msg and send it to the peer site */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(xnet, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out_nofree;
    }

    xnet_msg_fill_tx(msg, XNET_MSG_REQ, XNET_NEED_REPLY,
                     hmo.xc->site_id, request_site);
    xnet_msg_fill_cmd(msg, GK_CLT2MDS_REG, (u64)sin.sin_port | 
                      (u64)sin.sin_addr.s_addr << 32, 0);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif

    err = xnet_send(hmo.xc, msg);
    if (err) {
        gk_err(xnet, "xnet_send() failed w/ %d\n", err);
        goto out;
    }

    ASSERT(msg->pair, xnet);
    if (msg->pair->tx.err) {
        gk_err(xnet, "Reg site %lx with ROOT/MDS %lx failed w/ %d\n",
               hmo.xc->site_id, request_site, msg->pair->tx.err);
        err = msg->pair->tx.err;
        goto out;
    }
    
out:
    xnet_free_msg(msg);
out_nofree:
    
    return err;
}

/* cli_do_put() for MDS
 *
 * @
 */
int cli_do_put(u64 request_site, char *namespace, char *key, char *value)
{
    struct xnet_msg *msg;
    int err = 0, l1, l2, l3;

    if (!namespace || !key || !value)
        return -EINVAL;
    if (namespace)
        l1 = strlen(namespace);
    if (key)
        l2 = strlen(key);
    if (value)
        l3 = strlen(value);

    /* alloc one msg and send it to the peer site */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(xnet, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out_nofree;
    }

    xnet_msg_fill_tx(msg, XNET_MSG_REQ, XNET_NEED_REPLY,
                     hmo.xc->site_id, request_site);
    xnet_msg_fill_cmd(msg, GK_CLT2MDS_PUT, ((u64)l1 << 32) | l2, l3);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif
    xnet_msg_add_sdata(msg, namespace, l1);
    xnet_msg_add_sdata(msg, key, l2);
    xnet_msg_add_sdata(msg, value, l3);

    err = xnet_send(hmo.xc, msg);
    if (err) {
        gk_err(xnet, "xnet_send() failed w/ %d\n", err);
        goto out;
    }

out:
    xnet_free_msg(msg);
out_nofree:
    
    return err;
}

/* cli_do_get() for MDS
 *
 * @
 */
int cli_do_get(u64 request_site, char *namespace, char *key, char *value2check)
{
    struct xnet_msg *msg;
    int err = 0, l1, l2;

    if (unlikely(!namespace || !key))
        return -EINVAL;
    if (unlikely(namespace))
        l1 = strlen(namespace);
    if (unlikely(key))
        l2 = strlen(key);

    /* alloc one msg and send it to the peer site */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (unlikely(!msg)) {
        gk_err(xnet, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out_nofree;
    }

    xnet_msg_fill_tx(msg, XNET_MSG_REQ, XNET_NEED_REPLY,
                     hmo.xc->site_id, request_site);
    xnet_msg_fill_cmd(msg, GK_CLT2MDS_GET, l1, l2);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif
    xnet_msg_add_sdata(msg, namespace, l1);
    xnet_msg_add_sdata(msg, key, l2);

    err = xnet_send(hmo.xc, msg);
    if (unlikely(err)) {
        gk_err(xnet, "xnet_send() failed w/ %d\n", err);
        goto out;
    }

    /* parse and check the result */
    ASSERT(msg->pair, xnet);
    if (unlikely(msg->pair->tx.err)) {
        gk_err(xnet, "get(%s@%s) failed w/ %s\n",
               namespace, key, strerror(-msg->pair->tx.err));
        err = msg->pair->tx.err;
        goto out;
    }
    if (msg->pair->xm_datacheck) {
        char *data = msg->pair->xm_data;

        if (value2check) {
            err = memcmp(data, value2check, msg->pair->tx.arg0);
            if (err) {
                gk_err(xnet, "value check failed: recv(%.*s) expect(%s)\n",
                       (int)msg->pair->tx.arg0, data, value2check);
                err = -EFAULT;
            }
        } else {
            gk_debug(xnet, "RECV: %s -> %.*s\n", key, (int)msg->pair->tx.arg0, data);
        }
    }

out:
    xnet_free_msg(msg);
out_nofree:

    return err;
}

/* r2cli_do_reg()
 *
 * @gid: already right shift 2 bits
 */
int r2cli_do_reg(u64 request_site, u64 root_site, u64 fsid, u32 gid,
                char *ip, int port)
{
    struct xnet_msg *msg;
    struct sockaddr_in sin;
    int err = 0;

    sin.sin_port = htons(port);
    if (inet_aton(ip, &sin.sin_addr) == 0) {
        gk_err(xnet, "Invalid inet address %s\n", ip);
        err = -EINVAL;
        goto out_nofree;
    }

    /* alloc one msg and send it to the peer site */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(xnet, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out_nofree;
    }

    xnet_msg_fill_tx(msg, XNET_MSG_REQ, XNET_NEED_REPLY,
                     hmo.xc->site_id, root_site);
    xnet_msg_fill_cmd(msg, GK_R2_REG, (u64)sin.sin_port | 
                      (u64)sin.sin_addr.s_addr << 32, fsid);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif
    msg->tx.reserved = gid;

resend:
    err = xnet_send(hmo.xc, msg);
    if (err) {
        gk_err(xnet, "xnet_send() failed\n");
        goto out;
    }

    /* Reply ABI:
     * @tx.arg0: network magic
     * @tx.arg1: original ssite_id in msg, for xnet_simple to find xc
     * @tx.reserved: RANDOM select magic
     */

    /* this means we have got the reply, parse it! */
    ASSERT(msg->pair, xnet);
    if (msg->pair->tx.err == -ERECOVER) {
        gk_err(xnet, "R2 notify a client recover process on site "
                 "%lx, do it.\n", request_site);
    } else if (msg->pair->tx.err == -EHWAIT) {
        gk_err(xnet, "R2 reply that another instance is still alive, "
                 "wait a moment and retry.\n");
        xnet_free_msg(msg->pair);
        msg->pair = NULL;
        sleep(5);
        goto resend;
    } else if (msg->pair->tx.err) {
        gk_err(xnet, "Reg site %lx failed w/ %d\n", request_site,
                 msg->pair->tx.err);
        err = msg->pair->tx.err;
        goto out;
    }

    /* parse the register reply message */
    gk_debug(xnet, "Begin parse the reg reply message\n");
    if (msg->pair->xm_datacheck) {
        void *data = msg->pair->xm_data;
        union gk_x_info *hxi;
        struct root_tx *rt;
        struct gk_site_tx *hst;

        /* parse hxi */
        err = bparse_hxi(data, &hxi);
        if (err < 0) {
            gk_err(root, "bparse_hxi failed w/ %d\n", err);
            goto out;
        }
        memcpy(&hci, hxi, sizeof(hci));
        data += err;

        /* parse root_tx */
        err = bparse_root(data, &rt);
        if (err < 0) {
            gk_err(root, "bparse root failed w/ %d\n", err);
            goto out;
        }
        data += err;
        gk_info(root, "register fsid %ld -> recv_site %lx\n",
                rt->fsid, msg->pair->tx.dsite_id & GK_SITE_N_MASK);
        hmo.xc->site_id = msg->pair->tx.dsite_id;

        /* parse addr */
        err = bparse_addr(data, &hst);
        if (err < 0) {
            gk_err(root, "bparse addr failed w/ %d\n", err);
            goto out;
        }
        /* add the site table to the xnet */
        err = hst_to_xsst(hst, err - sizeof(u32));
        if (err) {
            gk_err(root, "hst to xsst failed w/ %d\n", err);
        }

        /* set network magic */
        xnet_set_magic(msg->pair->tx.arg0);

        /* send the hello message */
    }
    
out:
    xnet_free_msg(msg);
out_nofree:

    return err;
}

/* r2cli_do_unreg()
 *
 * @gid: already right shift 2 bits
 */
int r2cli_do_unreg(u64 request_site, u64 root_site, u64 fsid, u32 gid)
{
    struct xnet_msg *msg;
    union gk_x_info *hxi;
    int err = 0;

    hxi = (union gk_x_info *)&hci;

    /* alloc one msg and send it to the perr site */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(xnet, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out_nofree;
    }

    xnet_msg_fill_tx(msg, XNET_MSG_REQ, XNET_NEED_REPLY,
                     hmo.xc->site_id, root_site);
    xnet_msg_fill_cmd(msg, GK_R2_UNREG, request_site, fsid);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif
    xnet_msg_add_sdata(msg, hxi, sizeof(*hxi));

    /* send te unreeg request to root_site w/ requested siteid = request_site */
    msg->tx.reserved = gid;

    err = xnet_send(hmo.xc, msg);
    if (err) {
        gk_err(xnet, "xnet_send() failed\n");
        goto out;
    }

    /* this means we have got the reply, parse it! */
    ASSERT(msg->pair, xnet);
    if (msg->pair->tx.err) {
        gk_err(xnet, "Unreg site %lx failed w/ %d\n", request_site,
                 msg->pair->tx.err);
        err = msg->pair->tx.err;
        goto out;
    }

out:
    xnet_free_msg(msg);
out_nofree:
    return err;
}

void client_cb_exit(void *arg)
{
    int err = 0;

    err = r2cli_do_unreg(hmo.xc->site_id, GK_ROOT(0), 0, 0);
    if (err) {
        gk_err(xnet, "unreg self %lx w/ r2 %x failed w/ %d\n",
                 hmo.xc->site_id, GK_ROOT(0), err);
        return;
    }
}

void client_cb_addr_table_update(void *arg)
{
    struct gk_site_tx *hst;
    void *data = arg;
    int err = 0;

    gk_info(xnet, "Update address table ...\n");

    err = bparse_addr(data, &hst);
    if (err < 0) {
        gk_err(xnet, "bparse_addr failed w/ %d\n", err);
        goto out;
    }
    
    err = hst_to_xsst(hst, err - sizeof(u32));
    if (err) {
        gk_err(xnet, "hst to xsst failed w/ %d\n", err);
        goto out;
    }

out:
    return;
}

int client_dispatch(struct xnet_msg *msg)
{
    int err = 0;

    switch (msg->tx.cmd) {
    case GK_FR2_AU:
        err = mds_addr_table_update(msg);
        break;
    default:
        gk_err(xnet, "Client core dispatcher handle INVALID "
                 "request <0x%lx %d>\n",
                 msg->tx.ssite_id, msg->tx.reqno);
        err = -EINVAL;
    }

    return err;
}

int main(int argc, char *argv[])
{
    struct xnet_type_ops ops = {
        .buf_alloc = NULL,
        .buf_free = NULL,
        .recv_handler = client_dispatch,
    };
    int err = 0;
    int self, sport = -1, thread;
    long entry;
    int op;
    char *value;
    char profiling_fname[256], *log_home;

    gk_info(xnet, "op:   0/1/2/100   => "
              "create/lookup/unlink/OP_ALL\n");

    value = getenv("entry");
    if (value) {
        entry = atoi(value);
    } else {
        entry = 1024;
    }
    value = getenv("op");
    if (value) {
        op = atoi(value);
    } else {
        op = OP_LOOKUP;
    }
    value = getenv("thread");
    if (value) {
        thread = atoi(value);
    } else {
        thread = 1;
    }
    value = getenv("LOG_DIR");
    if (value) {
        log_home = strdup(value);
    } else
        log_home = NULL;

    pthread_barrier_init(&barrier, NULL, thread);

    st_init();
    lib_init();
    mds_pre_init();
    hmo.prof.xnet = &g_xnet_prof;
    hmo.conf.prof_plot = 1;
    mds_init(10);
    hmo.gossip_thread_stop = 1;
    if (hmo.conf.xnet_resend_to)
        g_xnet_conf.resend_timeout = hmo.conf.xnet_resend_to;
    
    //SET_TRACING_FLAG(xnet, GK_DEBUG);
    //SET_TRACING_FLAG(mds, GK_DEBUG | GK_VERBOSE);

    /* setup the profiling file */
    if (!log_home)
        log_home = ".";
    
    xnet_update_ipaddr(GK_ROOT(0), 1, &ipaddr[0], (short *)(&port[0]));
    sport = port[1];

    hmo.xc = xnet_register_type(0, sport, GK_CLIENT_RANDOM(), &ops);
    if (IS_ERR(hmo.xc)) {
        err = PTR_ERR(hmo.xc);
        return err;
    }

    self = hmo.site_id = GK_CLIENT_RANDOM();

    hmo.cb_exit = client_cb_exit;
    hmo.cb_hb = client_cb_hb;
    hmo.cb_addr_table_update = client_cb_addr_table_update;
    /* use root info to init the client */
    err = r2cli_do_reg(self, GK_ROOT(0), 0, 0, ipaddr[0], port[1]);
    if (err) {
        gk_err(xnet, "reg self %x w/ r2 %x failed w/ %d\n",
               self, GK_ROOT(0), err);
        goto out;
    }

    memset(profiling_fname, 0, sizeof(profiling_fname));
    sprintf(profiling_fname, "%s/CP-BACK-client.%d", log_home, self);
    hmo.conf.pf_file = fopen(profiling_fname, "w+");
    if (!hmo.conf.pf_file) {
        gk_err(xnet, "fopen() profiling file %s failed %d\n", 
                 profiling_fname, errno);
        err = -EINVAL;
        goto out;
    }

    err = mds_verify();
    if (err) {
        gk_err(xnet, "Verify MDS configration failed!\n");
        goto out;
    }

    err = cli_do_reg(GK_MDS(0), ipaddr[0], port[1]);
    if (err) {
        gk_err(xnet, "reg self %lx to MDS %x failed w/ %d\n",
               hmo.xc->site_id, GK_MDS(0), err);
        goto out;
    }
    
    {
        lib_timer_def();
        double acc = 0.0;

        /* Step 1: we should warmup the system a litte */
        if (op == 100) {
            gk_info(xnet, "Warmup the whole system a little ...\n");
            gk_info(xnet, "OK to real test now...\n");
        } else {
            gk_info(xnet, "Warmup by yourself, please!\n");
        }

        /* Step 2: do real test */
        lib_timer_B();
        msg_send_mt(entry, op, thread);
        lib_timer_E();
        lib_timer_A(&acc);
        gk_info(xnet, "Aggr IOPS [op=%d]: %lf\n", op,
                  (double)entry * 1000000.0 / acc);
    }
    
    sleep(200);
    pthread_barrier_destroy(&barrier);
    mds_destroy();
    xnet_unregister_type(hmo.xc);
out:
    return err;
}
#endif
