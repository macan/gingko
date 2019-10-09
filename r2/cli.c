/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-25 09:59:35 macan>
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
#include "root.h"

static inline
int __pack_msg(struct xnet_msg *msg, void *data, int len)
{
    u32 *__len = xmalloc(sizeof(u32));

    if (!__len) {
        gk_err(root, "pack msg xmalloc failed\n");
        return -ENOMEM;
    }

    *__len = len;
    xnet_msg_add_sdata(msg, __len, sizeof(u32));
    xnet_msg_add_sdata(msg, data, len);

    return 0;
}

struct addr_args
{
    u64 site_id;                /* site id filled by traverse function */
    u32 state;                  /* site state filled by traverse function */

    int len;
    void *data;
};

void *__cli_send_addr_table(void *args)
{
    struct xnet_msg *msg;
    struct addr_args *aa = (struct addr_args *)args;
    int err = 0;

    if (aa->state == SE_STATE_INIT ||
        aa->state == SE_STATE_SHUTDOWN) {
        return NULL;
    }
    
    gk_info(root, "Send addr table to %lx len %d\n", aa->site_id, aa->len);

    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(root, "xnet_alloc_msg() failed\n");
        err = -ENOMEM;
        goto out;
    }
    xnet_msg_fill_tx(msg, XNET_MSG_REQ, 0,
                     hro.xc->site_id, aa->site_id);
    xnet_msg_fill_cmd(msg, GK_FR2_AU, 0, 0);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif

    /* Step 1: pack the addr table */
    err = __pack_msg(msg, aa->data, aa->len);
    if (err) {
        gk_err(root, "pack addr table len %d failed w/ %d\n",
                 aa->len, err);
        goto send;
    }

    /* Step 2: send the message to receiver */
send:
    if (err) {
        xnet_msg_set_err(msg, err);
    }
    err = xnet_send(hro.xc, msg);
    if (err) {
        gk_err(root, "xnet_send() to %lx failed\n", aa->site_id);
        goto out_msg;
    }
out_msg:
    xnet_free_msg(msg);
out:
    return ERR_PTR(err);
}

/* cli_do_addsite manipulate the address table!
 */
int cli_do_addsite(struct sockaddr_in *sin, u64 fsid, u64 site_id)
{
    struct addr_args aa;
    struct addr_entry *ae;
    int err = 0;

    err = addr_mgr_lookup_create(&hro.addr, fsid, &ae);
    if (err > 0) {
        gk_info(root, "Create addr table for fsid %ld\n", 
                  fsid);
    } else if (err < 0) {
        gk_err(root, "addr_mgr_lookup_create() fsid %ld failed w/ %d\n",
                 fsid, err);
        goto out;
    }

    err = addr_mgr_update_one(ae, GK_SITE_PROTOCOL_TCP |
                              GK_SITE_REPLACE,
                              site_id,
                              sin);
    if (err) {
        gk_err(xnet, "addr_mgr_update entry %lx failed w/ %d\n",
                 site_id, err);
        goto out;
    }

    /* export the new addr to st_table */
    {
        void *data;
        int len;

        err = addr_mgr_compact_one(ae, site_id, GK_SITE_REPLACE, 
                                   &data, &len);
        if (err) {
            gk_err(xnet, "compact addr mgr failed w/ %d\n", err);
            goto out;
        }

        err = hst_to_xsst(data, len);
        if (err) {
            gk_err(xnet, "hst to xsst failed w/ %d\n", err);
            xfree(data);
            goto out;
        }

        /* trigger an addr table update now */
#if 0
        /* DO NOT TRIGGER it 2019.8.22 */
        aa.data = data;
        aa.len = len;
        err = site_mgr_traverse(&hro.site, __cli_send_addr_table, &aa);
        if (err) {
            gk_err(root, "bcast the address table failed w/ %d\n", err);
            xfree(data);
            goto out;
        }
#endif

        xfree(data);
    }
out:
    return err;
}

int cli_do_rmvsite(struct sockaddr_in *sin, u64 fsid, u64 site_id)
{
    struct addr_args aa;
    struct addr_entry *ae;
    int err = 0;

    ae = addr_mgr_lookup(&hro.addr, fsid);
    if (IS_ERR(ae)) {
        gk_err(root, "addr_mgr_lookup() fsid %ld failed w/ %ld\n",
                 fsid, PTR_ERR(ae));
        err = PTR_ERR(ae);
        goto out;
    }

    /* export the new addr to st_table */
    {
        void *data;
        int len;

        err = addr_mgr_compact_one(ae, site_id, GK_SITE_DEL,
                                   &data, &len);
        if (err) {
            gk_err(xnet, "compact addr mgr failed w/ %d\n", err);
            goto out;
        }

        err = hst_to_xsst(data, len);
        if (err) {
            gk_err(xnet, "hst to xsst failed w/ %d\n", err);
            xfree(data);
            goto out;
        }

        err = addr_mgr_update_one(ae, GK_SITE_PROTOCOL_TCP |
                                  GK_SITE_DEL,
                                  site_id, sin);
        if (err) {
            gk_err(xnet, "addr_mgr_update entry %lx failed w/ %d\n",
                     site_id, err);
            xfree(data);
            goto out;
        }

        /* trigger an addr table update now */
#if 0
        /* DO NOT TRIGGER it 2019.8.22 */
        aa.data = data;
        aa.len = len;
        err = site_mgr_traverse(&hro.site, __cli_send_addr_table, &aa);
        if (err) {
            gk_err(root, "bcast the address table failed w/ %d\n", err);
            xfree(data);
            goto out;
        }
#endif

        xfree(data);
    }
    
out:
    return err;
}

struct site_info_args
{
    u64 site_id;
    u32 state;

    u32 flag;
    u32 init, normal, transient, error, shutdown;
};

static inline
void __sia_analyze_state(struct site_info_args *sia)
{
    switch (sia->state) {
    case SE_STATE_INIT:
        sia->init++;
        break;
    case SE_STATE_NORMAL:
        sia->normal++;
        break;
    case SE_STATE_TRANSIENT:
        sia->transient++;
        break;
    case SE_STATE_ERROR:
        sia->error++;
        break;
    case SE_STATE_SHUTDOWN:
        sia->shutdown++;
        break;
    default:;
    }
}

void *__cli_get_site_info(void *args)
{
    struct site_info_args *sia = args;

    switch (sia->flag & GK_SYSINFO_SITE_MASK) {
    case GK_SYSINFO_SITE_ALL:
        __sia_analyze_state(sia);
        break;
    case GK_SYSINFO_SITE_MDS:
        if (GK_IS_MDS(sia->site_id))
            __sia_analyze_state(sia);
        break;
    case GK_SYSINFO_SITE_MDSL:
        if (GK_IS_MDSL(sia->site_id))
            __sia_analyze_state(sia);
        break;
    case GK_SYSINFO_SITE_CLIENT:
        if (GK_IS_CLIENT(sia->site_id))
            __sia_analyze_state(sia);
        break;
    case GK_SYSINFO_SITE_BP:
        if (GK_IS_BP(sia->site_id))
            __sia_analyze_state(sia);
        break;
    case GK_SYSINFO_SITE_R2:
        if (GK_IS_ROOT(sia->site_id))
            __sia_analyze_state(sia);
        break;
    default:;
    }
    
    return NULL;
}

static inline
char *__sysinfo_type(u64 arg)
{
    switch (arg & GK_SYSINFO_SITE_MASK) {
    case GK_SYSINFO_SITE_ALL:
        return "All sites";
    case GK_SYSINFO_SITE_MDS:
        return "MDS sites";
    case GK_SYSINFO_SITE_MDSL:
        return "MDSL sites";
    case GK_SYSINFO_SITE_CLIENT:
        return "Client sites";
    case GK_SYSINFO_SITE_BP:
        return "BP sites";
    case GK_SYSINFO_SITE_R2:
        return "R2 sites";
    default:
        return "Unknown sites";
    }
    return "Unkonw sites";
}

int root_info_site(u64 arg, void **buf)
{
    struct site_info_args sia;
    char *p;
    int err = 0;

    memset(&sia, 0, sizeof(sia));
    sia.flag = arg;
    
    err = site_mgr_traverse(&hro.site, __cli_get_site_info, &sia);
    if (err) {
        gk_err(root, "Traverse site table failed w/ %d\n", err);
        goto out;
    }

    p = xzalloc(512);
    if (!p) {
        err = -ENOMEM;
        goto out;
    }
    *buf = (void *)p;

    p += sprintf(p, "%s total %d active %d inactive %d indoubt %d\n",
                 __sysinfo_type(arg),
                 sia.init + sia.normal + sia.transient + sia.error + 
                 sia.shutdown,
                 sia.normal,
                 sia.init + sia.shutdown,
                 sia.transient + sia.error);
    p += sprintf(p, " -> [INIT] %d [NORM] %d [TRAN] %d [ERROR] %d "
                 "[SHUTDOWN] %d\n",
                 sia.init, sia.normal, sia.transient,
                 sia.error, sia.shutdown);

out:
    return err;
}
