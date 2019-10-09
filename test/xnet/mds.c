/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-29 10:34:06 macan>
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
#include "lib.h"
#include "mds.h"
#include "root.h"

#ifdef UNIT_TEST
#define TYPE_MDS        0
#define TYPE_CLIENT     1
#define TYPE_ROOT       3

u64 fsid = 0;

char *ipaddr[] = {
    "127.0.0.1",              /* root */
};

short port[] = {
    8710,                       /* root */
    8210,                       /* mds */
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

/* r2cli_do_reg()
 *
 * @gid: already right shift 2 bits
 */
static
int r2cli_do_reg(u64 request_site, u64 root_site, u64 fsid, u32 gid)
{
    struct xnet_msg *msg;
    int err = 0;

    /* alloc one msg and send it to the peer site */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(xnet, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out_nofree;
    }

    xnet_msg_fill_tx(msg, XNET_MSG_REQ, XNET_NEED_REPLY,
                     hmo.xc->site_id, root_site);
    xnet_msg_fill_cmd(msg, GK_R2_REG, request_site, fsid);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif

    /* send the reg request to root_site w/ requested siteid = request_site */
    msg->tx.reserved = gid;

resend:
    err = xnet_send(hmo.xc, msg);
    if (err) {
        gk_err(xnet, "xnet_send() failed\n");
        goto out;
    }

    /* Reply ABI:
     * @tx.arg0: network magic
     */

    /* this means we have got the reply, parse it! */
    ASSERT(msg->pair, xnet);
    if (msg->pair->tx.err == -ERECOVER) {
        gk_err(xnet, "R2 notify a client recover process on site "
                 "%lx, do it.\n", request_site);
        hmo.aux_state |= HMO_AUX_STATE_RECOVER;
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
        memcpy(&hmi, hxi, sizeof(hmi));
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
static
int r2cli_do_unreg(u64 request_site, u64 root_site, u64 fsid, u32 gid)
{
    struct xnet_msg *msg;
    union gk_x_info *hxi;
    int err = 0;

    hxi = (union gk_x_info *)&hmi;

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

    hxi = (union gk_x_info *)&hmi;
    
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

void mds_cb_exit(void *arg)
{
    int err = 0;

    err = r2cli_do_unreg(hmo.xc->site_id, GK_RING(0), fsid, 0);
    if (err) {
        gk_err(xnet, "unreg self %lx w/ r2 %x failed w/ %d\n",
                 hmo.xc->site_id, GK_RING(0), err);
        return;
    }
}

void mds_cb_hb(void *arg)
{
    u64 ring_site;
    int err = 0;

    ring_site = mds_select_ring(&hmo);
    err = r2cli_do_hb(hmo.xc->site_id, ring_site, fsid, 0);
    if (err) {
        gk_err(xnet, "hb %lx w/ r2 %x failed w/ %d\n",
                 hmo.xc->site_id, GK_RING(0), err);
    }
}

void mds_cb_addr_table_update(void *arg)
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

int main(int argc, char *argv[])
{
    struct xnet_type_ops ops = {
        .buf_alloc = NULL,
        .buf_free = NULL,
        .recv_handler = mds_spool_dispatch,
        .dispatcher = mds_fe_dispatch,
    };
    int err = 0;
    int self, sport = -1;
    int plot_method;
    char *value;
    char profiling_fname[256], *log_home;
    
    gk_info(xnet, "MDS starting ...\n");

    if (argc < 2) {
        gk_err(xnet, "Usage: %s id\n", argv[0]);
        return EINVAL;
    } else {
        self = atoi(argv[1]);
        gk_info(xnet, " get self id: %d\n", self);
    }

    value = getenv("fsid");
    if (value) {
        fsid = atoi(value);
    } else
        fsid = 0;

    value = getenv("plot");
    if (value) {
        plot_method = atoi(value);
    } else
        plot_method = MDS_PROF_PLOT;

    value = getenv("LOG_DIR");
    if (value) {
        log_home = strdup(value);
    } else
        log_home = NULL;

    st_init();
    mds_pre_init();
    hmo.prof.xnet = &g_xnet_prof;
    hmo.conf.prof_plot = plot_method;

    err = mds_init(11);
    if (err) {
        return err;
    }

//    SET_TRACING_FLAG(xnet, GK_DEBUG);
//    SET_TRACING_FLAG(mds, GK_DEBUG | GK_VERBOSE);

    /* setup home and the profiling file */
    if (!log_home)
        log_home = ".";

    memset(profiling_fname, 0, sizeof(profiling_fname));
    sprintf(profiling_fname, "%s/CP-BACK-mds.%d", log_home, self);
    hmo.conf.pf_file = fopen(profiling_fname, "w+");
    if (!hmo.conf.pf_file) {
        gk_err(xnet, "fopen() profiling file %s faield %d\n",
                 profiling_fname, errno);
        return EINVAL;
    }

    self = GK_MDS(self);

    xnet_update_ipaddr(GK_ROOT(0), 1, &ipaddr[0], (short *)(&port[0]));
    sport = port[1];

    hmo.xc = xnet_register_type(0, sport, self, &ops);
    if (IS_ERR(hmo.xc)) {
        err = PTR_ERR(hmo.xc);
        return err;
    }
    hmo.site_id = self;

    hmo.cb_exit = mds_cb_exit;
    hmo.cb_hb = mds_cb_hb;
    hmo.cb_addr_table_update = mds_cb_addr_table_update;

    /* use root info to init the mds */
    err = r2cli_do_reg(self, GK_ROOT(0), fsid, 0);
    if (err) {
        gk_err(xnet, "reg self %x w/ r2 %x failed w/ %d\n",
               self, GK_ROOT(0), err);
        goto out;
    }
 
    /* setup latency dconf generator */
    hmo.cb_latency = mds_cb_latency;
    
    err = mds_verify();
    if (err) {
        gk_err(xnet, "Verify MDS configration failed!\n");
        goto out;
    }

    gk_info(xnet, "MDS is UP for serving requests now.\n");

    msg_wait();

    xnet_unregister_type(hmo.xc);

    st_destroy();
    mds_destroy();

    return 0;
out:
    mds_destroy();

    return err;
}
#endif
