/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-25 09:47:00 macan>
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
int __prepare_xnet_msg(struct xnet_msg *msg, struct xnet_msg **orpy)
{
    struct xnet_msg *rpy;
    
    rpy = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!rpy) {
        gk_err(root, "xnet_alloc_msg() reply failed.\n");
        *orpy = NULL;
        return -ENOMEM;
    }
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(rpy, &rpy->tx, sizeof(rpy->tx));
#endif
    xnet_msg_fill_tx(rpy, XNET_MSG_RPY, 0, hro.site_id, msg->tx.ssite_id);
    xnet_msg_fill_reqno(rpy, msg->tx.reqno);
    xnet_msg_fill_cmd(rpy, XNET_RPY_DATA, 0, 0);
    rpy->tx.handle = msg->tx.handle;

    *orpy = rpy;
    
    return 0;
}

static inline
void __root_send_rpy(struct xnet_msg *rpy, int err)
{
    if (err && err != -ERECOVER) {
        /* delete the data payload */
        rpy->tx.len = sizeof(rpy->tx);
#ifdef XNET_EAGER_WRITEV
        rpy->siov_ulen = 1;
#else
        rpy->siov_ulen = 0;
#endif
    }
    
    xnet_msg_set_err(rpy, err);
    if (xnet_send(hro.xc, rpy)) {
        gk_err(root, "xnet_send() failed.\n");
    }
    xnet_free_msg(rpy);
}

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

static inline
void __simply_send_reply(struct xnet_msg *msg, int err)
{
    struct xnet_msg *rpy = xnet_alloc_msg(XNET_MSG_CACHE);

    if (!rpy) {
        gk_err(root, "xnet_alloc_msg() failed\n");
        return;
    }
    xnet_msg_set_err(rpy, err);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(rpy, &rpy->tx, sizeof(rpy->tx));
#endif
    xnet_msg_fill_tx(rpy, XNET_MSG_RPY, 0, hro.site_id,
                     msg->tx.ssite_id);
    xnet_msg_fill_reqno(rpy, msg->tx.reqno);
    xnet_msg_fill_cmd(rpy, XNET_RPY_ACK, 0, 0);
    /* match the original request at the source site */
    rpy->tx.handle = msg->tx.handle;

    if (xnet_send(hro.xc, rpy)) {
        gk_err(root, "xnet_isend() failed\n");
        /* do not retry myself, client is forced to retry */
    }
    xnet_free_msg(rpy);
}

/* root_do_reg()
 *
 * do register the site_id in arg0. If the site state is INIT, we should read
 * in the hxi info and return it. If the site state is NORMAL, we should alert
 * the caller that there is already a running server/client with the same
 * site_id. If the site state is SHUTDOWN, we should do state change in atomic
 * fashion. If the site state is TRANSIENT, we should just wait a moment for
 * the state change. If the site state is ERROR, we should do a recover process.
 *
 * Return ABI: | hxi info(fixed size) | root info | site_table |
 */
int root_do_reg(struct xnet_msg *msg)
{
    struct site_entry *se;
    struct xnet_msg *rpy;
    struct root_entry *root;
    struct addr_entry *addr;
    struct root_tx *root_tx;
    u64 nsite;
    void *addr_data = NULL;
    u64 fsid;
    int addr_len;
    int err = 0, saved_err = 0;

    err = __prepare_xnet_msg(msg, &rpy);
    if (err) {
        gk_err(root, "prepare rpy xnet_msg failed w/ %d\n", err);
        goto out;
    }

    /* ABI:
       @tx.ssite_id: site_id or GK_IS_RANDOM to random selected a site_id
     * @tx.arg0: socket address and port
     * @tx.arg1: fsid
     * @tx.reserved: recv_fd (high 32 bits) | gid (low 32 bits)
     */
    if (GK_IS_RANDOM(msg->tx.ssite_id)) {
        nsite = lib_random(GK_SITE_TYPE_MAX);

    rerand:
        if (GK_IS_CLIENT(msg->tx.ssite_id)) {
            nsite = GK_CLIENT(nsite);
        } else if (GK_IS_MDS(msg->tx.ssite_id)) {
            nsite = GK_MDS(nsite);
        }
        se = site_mgr_lookup(&hro.site, nsite);
        if (IS_ERR(se)) {
            if (ERR_PTR(-ENOENT) == se) {
                /* it is ok */
            } else {
                err = PTR_ERR(se);
                gk_err(root, "site_mgr_lookup() failed w/ %d\n", err);
                goto out;
            }
        } else {
            /* already contains this site, retry */
            goto rerand;
        }
        gk_debug(root, "random select site: %lx, update it to site table.\n",
                 nsite);

    } else {
        nsite = msg->tx.ssite_id;
    }
    {
        int fd = -1;

        rpy->tx.dsite_id = nsite;

        /* update the low-level socket to site table */
        fd = (msg->tx.reserved >> 32);
        if (fd > 0) {
            struct sockaddr_in sin;

            sin.sin_family = AF_INET;
            sin.sin_port = msg->tx.arg0 & 0xffffffff;
            sin.sin_addr.s_addr = msg->tx.arg0 >> 32;

            err = cli_do_addsite(&sin, msg->tx.arg1, nsite);
            if (err) {
                gk_err(root, "cli_do_addsite() for site %lx fsid %ld failed w/ %d\n",
                       nsite, msg->tx.arg1, err);
                goto send_rpy;
            }
            /* ok, update the sockfd */
            err = gst_update_sockfd(fd, nsite);
            if (err) {
                gk_err(root, "gst_update_sockfd() fd %d for site %lx failed w/ %d\n",
                       fd, nsite, err);
                goto send_rpy;
            }
        }
    }
    /* site mgr lookup create */
    err = site_mgr_lookup_create(&hro.site, nsite, &se);
    if (err > 0) {
        /* it is a new create site entry, set the fsid now */
        se->fsid = msg->tx.arg1;
        se->gid = msg->tx.reserved & 0xffffffff;
        err = root_read_hxi(se->site_id, se->fsid, &se->hxi);
        if (err == -ENOTEXIST) {
            err = root_create_hxi(se);
            if (err) {
                gk_err(root, "create hxi %ld %lx failed w/ %d\n",
                         se->fsid, se->site_id, err);
                goto send_rpy;
            }
            /* write the hxi to disk now */
            err = root_write_hxi(se);
            if (err) {
                gk_err(root, "write hxi %ld %lx failed w/ %d\n",
                         se->fsid, se->site_id, err);
                goto send_rpy;
            }
        } else if (err) {
            gk_err(root, "read %ld %lx hxi failed w/ %d\n",
                     se->fsid, se->site_id, err);
            goto send_rpy;
        }
    } else if (err < 0) {
        gk_err(root, "lookup create site entry %lx failed w/ %d\n",
               nsite, err);
        goto send_rpy;
    }
    
    /* now, we get a entry, either new created or an existed one, we should
     * act on the diffierent state */
    err = root_compact_hxi(nsite, msg->tx.arg1, msg->tx.reserved & 0xffffffff,
                           &se->hxi);
    if (err == -ERECOVER) {
        saved_err = err;
        err = 0;
    }
    if (err) {
        gk_err(root, "compact %lx hxi failed w/ %d\n", nsite,
                 err);
        goto send_rpy;
    }
    
    /* pack the hxi in the rpy message */
    xlock_lock(&se->lock);
    if (se->state == SE_STATE_NORMAL) {
        err = __pack_msg(rpy, &se->hxi, sizeof(se->hxi));
        if (err) {
            gk_err(root, "pack hxi failed w/ %d\n", err);
            xlock_unlock(&se->lock);
            goto send_rpy;
        }
    } else {
        gk_err(root, "site entry %lx in state %x\n", se->site_id,
                 se->state);
        err = -EFAULT;
        xlock_unlock(&se->lock);
        goto send_rpy;
    }
    xlock_unlock(&se->lock);

    /* pack the root info, just for verify the hxi info */
    root = root_mgr_lookup(&hro.root, msg->tx.arg1);
    if (IS_ERR(root)) {
        gk_err(root, "root_mgr_lookup() failed w/ %ld\n",
                 PTR_ERR(root));
        err = PTR_ERR(root);
        goto send_rpy;
    }
    /* set communication magic to arg0 */
    rpy->tx.arg0 = root->magic;
    rpy->tx.arg1 = msg->tx.ssite_id;
    rpy->tx.reserved = GK_SITE_RANDOM_SELECT_MAGIC;

    root_tx = (void *)root + sizeof(root->hlist);
    err = __pack_msg(rpy, root_tx, sizeof(*root_tx));
    if (err) {
        gk_err(root, "pack root tx failed w/ %d\n", err);
        goto send_rpy;
    }
        
    /* pack the global site table */
    fsid = msg->tx.arg1;
relookup_addr:
    addr = addr_mgr_lookup(&hro.addr, fsid);
    if (IS_ERR(addr)) {
        gk_err(root, "lookup addr for fsid %ld failed w/ %ld\n",
                 fsid, PTR_ERR(addr));
        /* we just fallback to the address table of fsid 0 */
        if (msg->tx.arg1) {
            fsid = 0;
            goto relookup_addr;
        }
        err = PTR_ERR(addr);
        goto send_rpy;
    }
    err = addr_mgr_compact(addr, &addr_data, &addr_len);
    if (err) {
        gk_err(root, "compact the site table for %lx failed w/ %d\n",
                 nsite, err);
        goto send_rpy;
    }
    err = __pack_msg(rpy, addr_data, addr_len);
    
    if (err) {
        /* if we got the ERECOVER error, we should send the data region to the
         * requester either. */
        if (err == -ERECOVER) {
            gk_err(root, "One recover process will rise from %lx\n",
                     nsite);
        }
    }
send_rpy:
    if (!err)
        __root_send_rpy(rpy, saved_err);
    else
        __root_send_rpy(rpy, err);
    /* free the allocated resources */
    xfree(addr_data);
    
out:
    xnet_free_msg(msg);

    return err;
}

/* root_do_unreg() do unregister the site.
 *
 * We just change the site entry's state to SE_STATE_SHUTDOWN and write the
 * hxi to the storage and flush the gdt bitmap to disk. Before flushing we
 * first update the in-memory hxi w/ the request.
 */
int root_do_unreg(struct xnet_msg *msg)
{
    union gk_x_info *hxi;
    struct site_entry *se;
    struct root_entry *re;
    struct xnet_msg *rpy;
    int err = 0;

    /* prepare the reply message */
    err = __prepare_xnet_msg(msg, &rpy);
    if (err) {
        gk_err(root, "prepare reply msg faild w/ %d\n", err);
        goto out_free;
    }

    /* sanity checking */
    if (msg->tx.len < sizeof(*hxi)) {
        gk_err(root, "Invalid unregister request from %lx w/ len %d\n",
                 msg->tx.ssite_id, msg->tx.len);
        err = -EINVAL;
        goto out;
    }

    /* ABI:
     * @tx.arg0: site_id
     * @tx.arg1: fsid
     * @tx.reserved: gid
     */

    if (msg->tx.arg0 != msg->tx.ssite_id) {
        gk_err(root, "Unreg other site %lx from site %lx\n",
                 msg->tx.arg0, msg->tx.ssite_id);
        err = -EINVAL;
#if 0
        goto out;
#endif
    }

    if (msg->xm_datacheck) {
        hxi = msg->xm_data;
    } else {
        gk_err(root, "Internal error, data lossing...\n");
        err = -EFAULT;
        goto out;
    }

    /* update the hxi to the site entry */
    se = site_mgr_lookup(&hro.site, msg->tx.arg0);
    if (IS_ERR(se)) {
        gk_err(root, "site mgr lookup %lx failed w/ %ld\n",
                 msg->tx.arg0, PTR_ERR(se));
        err = PTR_ERR(se);
        goto out;
    }

    xlock_lock(&se->lock);
    if (se->fsid != msg->tx.arg1 ||
        se->gid != msg->tx.reserved) {
        gk_err(root, "fsid mismatch %ld vs %ld or gid mismatch "
                 "%d vs %ld on site %lx\n",
                 se->fsid, msg->tx.arg1, se->gid, msg->tx.reserved,
                 msg->tx.arg0);
        err = -EINVAL;
        goto out_unlock;
    }
    switch (se->state) {
    case SE_STATE_INIT:
    case SE_STATE_TRANSIENT:
    case SE_STATE_ERROR:
        gk_err(root, "site entry %lx in state %x, check whether "
                 "we can update it.\n", msg->tx.arg0,
                 se->state);
        se->hb_lost = 0;
        /* fall-through */
    case SE_STATE_NORMAL:
        /* ok, we just change the state to shutdown */
        memcpy(&se->hxi, hxi, sizeof(*hxi));
        se->state = SE_STATE_SHUTDOWN;
        break;
    case SE_STATE_SHUTDOWN:
        gk_err(root, "the site %lx is already shutdown.",
                 se->site_id);
        break;
    default:
        gk_err(root, "site entry %lx in wrong state %x\n",
                 se->site_id, se->state);
    }
out_unlock:    
    xlock_unlock(&se->lock);
    if (err)
        goto out;

    /* ok, then we should init a flush operation now */
    re = root_mgr_lookup(&hro.root, se->fsid);
    if (IS_ERR(re)) {
        gk_err(root, "root mgr lookup fsid %ld failed w/ %ld\n",
                 se->fsid, PTR_ERR(re));
        err = PTR_ERR(re);
        goto out;
    }
    /* write the hxi */
    err = root_write_hxi(se);
    if (err) {
        gk_err(root, "Flush site %lx hxi to storage failed w/ %d.\n", 
                 se->site_id, err);
        goto out;
    }
    err = root_write_re(re);
    if (err) {
        gk_err(root, "Flush fs root %ld to storage failed w/ %d.\n",
                 re->fsid, err);
        goto out;
    }

out:
    __root_send_rpy(rpy, err);
out_free:
    xnet_free_msg(msg);
    
    return err;
}

int root_do_update(struct xnet_msg *msg)
{
    union gk_x_info *hxi;
    struct site_entry *se;
    struct xnet_msg *rpy;
    int err = 0;

    /* ABI:
     * @tx.arg0: site_id
     * @tx.arg1: fsid
     * @tx.reserved: gid
     */

    /* prepare the reply message */
    err = __prepare_xnet_msg(msg, &rpy);
    if (err) {
        gk_err(root, "prepare reply msg failed w/ %d\n", err);
        goto out_free;
    }
    
    /* sanity checking */
    if (msg->tx.len < sizeof(*hxi)) {
        gk_err(root, "Invalid update request from %lx w/ len %d\n",
                 msg->tx.ssite_id, msg->tx.len);
        err = -EINVAL;
        goto out;
    }

    if (msg->tx.arg0 != msg->tx.ssite_id) {
        gk_err(root, "Update site %lx from site %lx\n",
                 msg->tx.arg0, msg->tx.ssite_id);
        err = -EINVAL;
#if 0
        goto out;
#endif
    }

    if (msg->xm_datacheck) {
        hxi = msg->xm_data;
    } else {
        gk_err(root, "Internal error, data lossing...\n");
        err = -EFAULT;
        goto out;
    }

    /* update the hxi to the site entry */
    se = site_mgr_lookup(&hro.site, msg->tx.arg0);
    if (IS_ERR(se)) {
        gk_err(root, "site mgr lookup %lx failed w/ %ld\n",
                 msg->tx.arg0, PTR_ERR(se));
        err = PTR_ERR(se);
        goto out;
    }

    xlock_lock(&se->lock);
    if (se->fsid != msg->tx.arg1 ||
        se->gid != msg->tx.reserved) {
        gk_err(root, "fsid mismatch %ld vs %ld or gid mismatch "
                 "%d vs %ld on site %lx\n",
                 se->fsid, msg->tx.arg1, se->gid, msg->tx.reserved,
                 msg->tx.arg0);
        err = -EINVAL;
        goto out_unlock;
    }

    switch (se->state) {
    case SE_STATE_INIT:
    case SE_STATE_TRANSIENT:
    case SE_STATE_ERROR:
        gk_err(root, "site entry %lx in state %x, check whether "
                 "we can update it.\n", msg->tx.arg0,
                 se->state);
        se->hb_lost = 0;
        /* fall-through */
    case SE_STATE_NORMAL:
        /* ok, we just change the state to normal */
        memcpy(&se->hxi, hxi, sizeof(*hxi));
        se->state = SE_STATE_NORMAL;
        break;
    case SE_STATE_SHUTDOWN:
        gk_err(root, "the site %lx is already shutdown.",
                 se->site_id);
        break;
    default:
        gk_err(root, "site_entry %lx in wrong state %x\n",
                 se->site_id, se->state);
    }
out_unlock:
    xlock_unlock(&se->lock);
    if (err)
        goto out;

    /* ok, then we should init a flush operation now */
    err = root_write_hxi(se);
    if (err) {
        gk_err(root, "Flush site %lx hxi to storage failed w/ %d.\n",
                 se->site_id, err);
    }

out:    
    __root_send_rpy(rpy, err);
out_free:
    xnet_free_msg(msg);
    
    return err;
}

/* root_do_hb() handling the heartbeat from the r2 client
 *
 * The timeout checker always increase the site_entry->lost_hb, on receiving
 * the client's hb, we just update the site_entry->lost_hb to ZERO.
 */
int root_do_hb(struct xnet_msg *msg)
{
    union gk_x_info *hxi;
    struct site_entry *se;
    int err = 0, contain_hxi = 0;

    /* sanity checking */
    if (msg->tx.len >= sizeof(*hxi)) {
        contain_hxi = 1;
    }

    /* ABI:
     * tx.arg0: site_id
     * tx.arg1: fsid
     * xm_data: hxi
     */
    if (msg->tx.arg0 != msg->tx.ssite_id) {
        gk_warning(root, "Warning: site_id mismatch %lx vs %lx\n",
                     msg->tx.ssite_id, msg->tx.arg0);
    }
    
    se = site_mgr_lookup(&hro.site, msg->tx.arg0);
    if (IS_ERR(se)) {
        gk_err(root, "site mgr lookup %lx failed w/ %ld\n",
                 msg->tx.ssite_id, PTR_ERR(se));
        err = PTR_ERR(se);
        goto out;
    }
    /* clear the lost_hb */
    se->hb_lost = 0;
    /* check whether we can change the state */
    xlock_lock(&se->lock);
    if (se->state != SE_STATE_SHUTDOWN &&
        se->state != SE_STATE_INIT) {
        se->state = SE_STATE_NORMAL;
    }
    xlock_unlock(&se->lock);

    /* update the hxi info */
    if (contain_hxi) {
        if (msg->xm_datacheck) {
            hxi = msg->xm_data;
        } else {
            gk_err(root, "Internal error, data lossing ...\n");
            err = -EFAULT;
            goto out;
        }
        /* update the hxi to the site entry */
        xlock_lock(&se->lock);
        if (se->fsid != msg->tx.arg1 ||
            se->gid != msg->tx.reserved) {
            gk_err(root, "fsid mismatch %ld vs %ld or gid mismatch "
                     "%d vs %ld on site %lx\n",
                     se->fsid, msg->tx.arg1, se->gid, msg->tx.reserved,
                     msg->tx.arg0);
            err = -EINVAL;
            goto out_unlock;
        }
        switch (se->state) {
        case SE_STATE_INIT:
        case SE_STATE_TRANSIENT:
        case SE_STATE_ERROR:
            gk_err(root, "site entry %lx in state %x, check whether "
                     "we can update it.\n", msg->tx.arg0,
                     se->state);
            se->hb_lost = 0;
            /* fall-through */
        case SE_STATE_NORMAL:
            memcpy(&se->hxi, hxi, sizeof(*hxi));
            break;
        case SE_STATE_SHUTDOWN:
            gk_err(root, "the site %lx is already shutdown.\n",
                     se->site_id);
            break;
        default:
            gk_err(root, "site entry %lx in wrong state %x\n",
                     se->site_id, se->state);
        }
    out_unlock:
        xlock_unlock(&se->lock);
    }

out:
    xnet_free_msg(msg);

    return err;
}

/* root_do_prof() merge the per-MDS stats together
 */
int root_do_prof(struct xnet_msg *msg)
{
    int err = 0;

    /* ABI:
     * xm_data: transfer plot entry
     */

    gk_info(root, "Notify: You can use test/result/aggr.sh to aggregate"
              " the results now!");
    xnet_free_msg(msg);

    return err;
}

int root_do_ftreport(struct xnet_msg *msg)
{
    /* If a site_A report site_B is FAILED, we should check if it is realy
     * down by scan the site_mgr. If site_B is really failed, we mask it out
     * of our cluster. Otherwise, we just ignore the report and return a OK
     * update to site_A! */
    int err = 0;

    return err;
}

/* shutdown() only acts on a error state site_entry and flush the info to disk
 */
int root_do_shutdown(struct xnet_msg *msg)
{
    struct site_entry *se;
    int err = 0;
    
    /* ABI:
     * tx.arg0: site_id to shutdown
     */

    /* find the site entry */
    se = site_mgr_lookup(&hro.site, msg->tx.arg0);
    if (IS_ERR(se)) {
        gk_err(root, "site mgr lookup %lx failed w/ %ld\n",
                 msg->tx.arg0, PTR_ERR(se));
        err = PTR_ERR(se);
        goto out;
    }

    xlock_lock(&se->lock);
    /* do not check fsid and gid! */
    switch (se->state) {
    case SE_STATE_INIT:
    case SE_STATE_TRANSIENT:
    case SE_STATE_NORMAL:
    case SE_STATE_SHUTDOWN:
        gk_err(root, "site entry %lx is not in ERROR state(%x), "
                 "reject this shutdown request!\n",
                 se->site_id, se->state);
        err = -EINVAL;
        break;
    case SE_STATE_ERROR:
        se->state = SE_STATE_SHUTDOWN;
        break;
    default:
        gk_err(root, "site entry %lx in wrong state %x\n",
                 se->site_id, se->state);
        err = -EFAULT;
    }
    xlock_unlock(&se->lock);
    if (err)
        goto out;

    /* ok, then we should issue a flush operation now */
    err = root_write_hxi(se);
    if (err) {
        gk_err(root, "Flush site %lx hxi to storage failed w/ %d.\n",
                 se->site_id, err);
        goto out;
    }
    
out:
    __simply_send_reply(msg, err);
    xnet_free_msg(msg);

    return err;
}

/* do_profile() handles the profile response from MDS and MDSL
 */
int root_do_profile(struct xnet_msg *msg)
{
    struct gk_profile *hp;
    int err = 0;

    if (msg->tx.len < sizeof(*hp) ||
        !msg->xm_datacheck) {
        gk_err(root, "Invalid profile request from site %lx\n",
                 msg->tx.ssite_id);
        err = -EINVAL;
        goto out;
    }
    hp = msg->xm_data;
    
    /* extract the profile */
    if (GK_IS_MDS(msg->tx.ssite_id)) {
        err = root_profile_update_mds(hp, msg);
    } else if (GK_IS_BP(msg->tx.ssite_id)) {
        err = root_profile_update_bp(hp, msg);
    } else if (GK_IS_CLIENT(msg->tx.ssite_id)) {
        err = root_profile_update_client(hp, msg);
    } else {
        gk_err(root, "Invalid source site(%lx), type is ??\n",
                 msg->tx.ssite_id);
        err = -EINVAL;
    }
    if (err) {
        gk_err(root, "Profile request(%lx) handling failed w/ %d\n",
                 msg->tx.ssite_id, err);
    }
    
out:
    xnet_free_msg(msg);

    return err;
}

/* do_info() recv the cmd from a site and response with the corresponding info.
 *
 * ABI: xmdata saves hsi structure
 *
 * Return format: <string buffer>
 */
int root_do_info(struct xnet_msg *msg)
{
    struct gk_sys_info *hsi;
    struct xnet_msg *rpy = NULL;
    char tbuf[32];
    void *buf = NULL, *buf1 = NULL, *buf2 = NULL;
    int err = 0;

    if (msg->tx.len < sizeof(*hsi) ||
        !msg->xm_datacheck) {
        gk_err(root, "Invalid info request from site %lx\n",
                 msg->tx.ssite_id);
        err = -EINVAL;
        goto out;
    }

    err = __prepare_xnet_msg(msg, &rpy);
    if (err) {
        goto out;
    }

    hsi = msg->xm_data;

    switch (hsi->cmd) {
    default:
    case GK_SYSINFO_NOOP:
        __simply_send_reply(msg, 0);
        break;
    case GK_SYSINFO_ALL:
        /* stick a uptime buffer */
        snprintf(tbuf, 32, "ROOT Server Uptime %lds\n", 
                 (long)(time(NULL) - hro.uptime));
        xnet_msg_add_sdata(rpy, tbuf, strlen(tbuf));
        /* fall through */
    case GK_SYSINFO_ROOT:
        err = root_info_root(hsi->arg0, &buf);
        if (!err && buf) {
            xnet_msg_add_sdata(rpy, buf, strlen(buf));
        }
        if (hsi->cmd == GK_SYSINFO_ROOT)
            break;
    case GK_SYSINFO_SITE:
        err = root_info_site(hsi->arg0, &buf);
        if (!err && buf) {
            xnet_msg_add_sdata(rpy, buf, strlen(buf));
        }
        if (hsi->cmd == GK_SYSINFO_SITE)
            break;
    case GK_SYSINFO_MDS:
        err = root_info_mds(hsi->arg0, &buf1);
        if (!err && buf1) {
            xnet_msg_add_sdata(rpy, buf1, strlen(buf1));
        }
        if (hsi->cmd == GK_SYSINFO_MDS)
            break;
    }

    __root_send_rpy(rpy, err);
    xfree(buf);
    xfree(buf1);
    xfree(buf2);
    
out:
    xnet_free_msg(msg);

    return err;
}

/* do_getasite() get the active site from site_mgr
 *
 * ABI: arg0 saves the target site_type (GK_SITE_TYPE_****)
 */
int root_do_getasite(struct xnet_msg *msg)
{
    struct xnet_msg *rpy = NULL;
    struct xnet_group *xg;
    int err = 0;

    err = __prepare_xnet_msg(msg, &rpy);
    if (err) {
        gk_err(root, "Failed to alloc reply msg, caller would be blocked\n");
        goto out;
    }
    xg = site_mgr_get_active_site(msg->tx.arg0);
    if (!xg) {
        err = -ENOENT;
        gk_err(root, "site_mgr get active site (%ld) failed w/ %d?(%s)\n",
                 msg->tx.arg0, err, strerror(-err));
    } else {
        xnet_msg_add_sdata(rpy, xg, sizeof(*xg) + 
                           xg->asize * sizeof(struct xnet_group_entry));
    }

    __root_send_rpy(rpy, err);
    
out:
    if (!err)
        xfree(xg);
    xnet_free_msg(msg);
    
    return err;
}
