/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-21 21:42:49 macan>
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
#include "mds.h"
#include "xnet.h"
#include "lib.h"

/* return the recent reqno for COMPARE
 */
u32 __mdsdisp mds_get_recent_reqno(u64 site)
{
    /* FIXME: we do not support TXC for now */
    return 0;
}

/* update the recent reqno
 */
void __mdsdisp mds_update_recent_reqno(u64 site, u64 reqno)
{
    return;
}

/* loop_detect the route info
 *
 * Return value: 0:no loop; 1: first loop; 2: second or greater loop;
 */
int __mdsdisp __mds_fwd_loop_detect(struct mds_fwd *mf, u64 dsite)
{
    int i, looped = 0;
    
    for (i = 0; i < MDS_FWD_MAX; i++) {
        if (mf->route[i] != 0) {
            if (mf->route[i] == hmo.site_id) {
                if (looped < 2)
                    looped++;
            }
        } else
            break;
    }
    if (!looped) {
        /* check if it will be looped */
        for (i = 0; i < MDS_FWD_MAX; i++) {
            if (mf->route[i] != 0) {
                if (mf->route[i] == dsite) {
                    if (looped < 2)
                        looped++;
                }
            } else
                break;
        }
    }

    return looped;
}

static inline
void __simply_send_reply(struct xnet_msg *msg, int err)
{
    struct xnet_msg *rpy = xnet_alloc_msg(XNET_MSG_CACHE);

    if (!rpy) {
        gk_err(mds, "xnet_alloc_msg() failed\n");
        return;
    }

    xnet_msg_set_err(rpy, err);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(rpy, &rpy->tx, sizeof(rpy->tx));
#endif
    xnet_msg_fill_tx(rpy, XNET_MSG_RPY, 0, hmo.site_id,
                     msg->tx.ssite_id);
    xnet_msg_fill_reqno(rpy, msg->tx.reqno);
    xnet_msg_fill_cmd(rpy, XNET_RPY_ACK, 0, 0);
    /* match the original request at the source site */
    rpy->tx.handle = msg->tx.handle;

    if (xnet_send(hmo.xc, rpy)) {
        gk_err(mds, "xnet_send() REPLY failed\n");
    }
    xnet_free_msg(rpy);
}

/* mds_fe_handle_err()
 *
 * NOTE: how to handle the err from the dispatcher? We just print the err
 * message and free the msg.
 */
void __mdsdisp mds_fe_handle_err(struct xnet_msg *msg, int err)
{
    if (unlikely(err)) {
        gk_warning(mds, "MSG(%lx->%lx)(reqno %d) can't be handled w/ %d\n",
                     msg->tx.ssite_id, msg->tx.dsite_id, msg->tx.reqno, err);
        /* send a reply error notify here */
        __simply_send_reply(msg, err);
    }

    xnet_set_auto_free(msg);
    xnet_free_msg(msg);
}

int __mdsdisp mds_do_forward(struct xnet_msg *msg, u64 dsite)
{
    int err = 0, i, relaied = 0, looped = 0;
    
    /* Note that lots of forward request may incur the system performance, we
     * should do fast forwarding and fast bitmap changing. */
    struct mds_fwd *mf = NULL, *rmf = NULL;
    struct xnet_msg *fmsg;

    if (unlikely(msg->tx.flag & XNET_FWD)) {
        atomic64_inc(&hmo.prof.mds.loop_fwd);
        /* check if this message is looped. if it is looped, we should refresh
         * the bitmap and just forward the message as normal. until receive
         * the second looped request, we stop or slow down the request */
        rmf = (struct mds_fwd *)((void *)(msg->tx.reserved) + 
                                 msg->tx.len + sizeof(msg->tx));
        looped = __mds_fwd_loop_detect(rmf, dsite);
        
        if (unlikely((atomic64_read(&hmo.prof.mds.loop_fwd) + 1) % 
                     MAX_RELAY_FWD == 0)) {
            /* FIXME: we should trigger the bitmap reload now */
        }
        relaied = 1;
    }

    mf = xzalloc(sizeof(*mf) + MDS_FWD_MAX * sizeof(u32));
    if (!mf) {
        gk_err(mds, "alloc mds_fwd failed.\n");
        err = -ENOMEM;
        goto out;
    }
    mf->len = MDS_FWD_MAX * sizeof(u32) + sizeof(*mf);
    switch (looped) {
    case 0:
        /* not looped request */
        mf->route[0] = hmo.site_id;
        break;
    case 1:        
        /* FIXME: first loop, copy the entries */
        for (i = 0; i < MDS_FWD_MAX; i++) {
            if (rmf->route[i] != 0)
                mf->route[i] = rmf->route[i];
            else
                break;
        }
        if (i < MDS_FWD_MAX)
            mf->route[i] = hmo.site_id;
        break;
    case 2:
        /* FIXME: second loop, slow down the forwarding */
        for (i = 0; i < MDS_FWD_MAX; i++) {
            if (rmf->route[i] != 0)
                mf->route[i] = rmf->route[i];
            else
                break;
        }
        if (i < MDS_FWD_MAX)
            mf->route[i] = hmo.site_id;
        break;
    default:;
    }

    fmsg = xnet_alloc_msg(XNET_MSG_CACHE);
    if (!fmsg) {
        gk_err(mds, "xnet_alloc_msg() failed, we should retry!\n");
        err = -ENOMEM;
        goto out_free;
    }

#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(fmsg, &fmsg->tx, sizeof(fmsg->tx));
#endif
    xnet_msg_set_err(fmsg, err);
    xnet_msg_fill_tx(fmsg, XNET_MSG_REQ, 0, hmo.site_id, dsite);
    xnet_msg_fill_cmd(fmsg, GK_MDS2MDS_FWREQ, 0, 0);
    xnet_msg_add_sdata(fmsg, &msg->tx, sizeof(msg->tx));

    if (msg->xm_datacheck) {
        if (unlikely(relaied)) {
            xnet_msg_add_sdata(fmsg, msg->xm_data, msg->tx.len);
        } else {
            for (i = 0; i < msg->riov_ulen; i++) {
                xnet_msg_add_sdata(fmsg, msg->riov[i].iov_base, 
                                   msg->riov[i].iov_len);
            }
        }
    }

    /* piggyback the route info @ the last iov entry */
    xnet_msg_add_sdata(fmsg, mf, mf->len);

    err = xnet_send(hmo.xc, fmsg);

    if (err) {
        gk_err(mds, "Forwarding the request to %lx failed w/ %d.\n",
                 dsite, err);
    }

    /* cleaning */
    xnet_clear_auto_free(fmsg);
    xnet_free_msg(fmsg);
    
out_free:
    xfree(mf);
out:
    return err;
}

/* return 1 means this msg is PAUSED.
 * return 0 means this msg has passed the controler.
 */
static inline
int mds_modify_control(struct xnet_msg *msg)
{
    if (unlikely(hmo.spool_modify_pause)) {
        if (msg->tx.cmd & GK_CLT2MDS_RDONLY) {
            return 0;
        }
        /* pause this handling */
        mds_spool_modify_pause(msg);
        return 1;
    }

    return 0;
}

int mds_pause(struct xnet_msg *msg)
{
    hmo.reqin_drop = 1;
    xnet_free_msg(msg);

    return 0;
}

int mds_resume(struct xnet_msg *msg)
{
    struct xnet_msg *rpy;
    
    /* if reqin_drop is set, this means we have dropped some incoming
     * requests */
    if (hmo.reqin_drop)
        hmo.reqin_drop = 0;
    hmo.reqin_pause = 0;

    /* if msg->tx.arg1 == 1, change mds's state to ONLINE(Running) */
    if (msg->tx.arg1 == 1) {
        hmo.state = HMO_STATE_RUNNING;
    }

    rpy = xnet_alloc_msg(XNET_MSG_CACHE);
    if (!rpy) {
        gk_err(mds, "xnet_alloc_msg() failed\n");
        goto out;
    }
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(rpy, &rpy->tx, sizeof(rpy->tx));
#endif
    xnet_msg_fill_tx(rpy, XNET_MSG_RPY, 0, hmo.site_id,
                     msg->tx.ssite_id);
    xnet_msg_fill_reqno(rpy, msg->tx.reqno);
    xnet_msg_fill_cmd(rpy, XNET_RPY_ACK, 0, 0);
    /* match the original request at the source site */
    rpy->tx.handle = msg->tx.handle;

    if (xnet_send(hmo.xc, rpy)) {
        gk_err(mds, "xnet_send() failed\n");
        /* do not retry myself */
    }
    xnet_free_msg(rpy);
out:    
    xnet_free_msg(msg);

    return 0;
}

int mds_addr_table_update(struct xnet_msg *msg)
{
    if (msg->xm_datacheck) {
        if (hmo.cb_addr_table_update)
            hmo.cb_addr_table_update(msg->xm_data);
    } else {
        gk_err(mds, "Invalid addr table update message, incomplete hst!\n");
        return -EINVAL;
    }

    xnet_free_msg(msg);

    return 0;
}

/* Callback for XNET, should be thread-safe!
 */
int __mdsdisp mds_fe_dispatch(struct xnet_msg *msg)
{
    int err = 0;
#ifdef GK_DEBUG_LATENCY
    lib_timer_def();
#endif

    /* Level 0 state checking */
l0_recheck:
    switch (hmo.state) {
    case HMO_STATE_INIT:
        /* wait */
        while (hmo.state == HMO_STATE_INIT) {
            sched_yield();
        }
        /* recheck it */
        goto l0_recheck;
    case HMO_STATE_LAUNCH:
        /* reinsert back to reqin list unless it is a RECOVERY request */
        /* 1. for mds memory lookup and analyse, just fail it (because we are
         * fresh). */
        /* 2. for mdsl analyse, just do it from mdsl */
        if (GK_IS_MDS(msg->tx.ssite_id)) {
            if (msg->tx.cmd == GK_MDS_RECOVERY ||
                msg->tx.cmd == GK_MDS2MDS_GB)
                return mds_mds_dispatch(msg);
        }
        
        mds_spool_redispatch(msg, 0);

        return -EAGAIN;
    case HMO_STATE_RUNNING:
        /* accept all requests */
        break;
    case HMO_STATE_PAUSE:
        /* pause all request, the same as hmo.reqin_pause */
        break;
    case HMO_STATE_RDONLY:
        /* read-only mode. FIXME */
        break;
    case HMO_STATE_OFFLINE:
        /* only accept R2 reqeust, otherwise return an OFFLINE error */
        if (!GK_IS_RING(msg->tx.ssite_id)) {
            __simply_send_reply(msg, -EOFFLINE);
            xnet_free_msg(msg);
            return 0;
        }
        break;
    default:
        GK_BUGON("Unknown MDS state");
    }

    if (GK_IS_CLIENT(msg->tx.ssite_id)) {
        return mds_client_dispatch(msg);
    } else if (GK_IS_MDS(msg->tx.ssite_id)) {
        return mds_mds_dispatch(msg);
    } else if (GK_IS_ROOT(msg->tx.ssite_id)) {
        return mds_root_dispatch(msg);
    }

    err = -EINVAL;
    gk_err(mds, "MDS front-end handle INVALID request <0x%lx %d>\n", 
             msg->tx.ssite_id, msg->tx.reqno);

    mds_fe_handle_err(msg, err);

    return err;
}
