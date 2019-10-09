/**
 * Copyright (c) 2019 Ma Can <ml.macana@gmail.com>
 *                           <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-29 10:33:48 macan>
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

/* In this file, we implemented a memory hash table and disk based log
 * structure file. Each put or get operation operate on the
 * key-field-value pair. Key is used for index for a hash table. Field
 * is used for index in the hash table. Value is the payload. On
 * writting (aka hput), the k-f-v pair is inserted in to memory hash
 * table, and system generated a log entry for this pair. On reading
 * (aka hget), the k-f-v pair is lookuped from memory if existed,
 * otherwise firstly loaded it from disk and then lookuped from
 * memory. Each hash table should contain no more than 10M entries.
 */

static inline
int __prepare_xnet_msg(struct xnet_msg *msg, struct xnet_msg **orpy)
{
    struct xnet_msg *rpy;
    
    rpy = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!rpy) {
        gk_err(mds, "xnet_alloc_msg() reply failed.\n");
        *orpy = NULL;
        return -ENOMEM;
    }
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(rpy, &rpy->tx, sizeof(rpy->tx));
#endif
    xnet_msg_fill_tx(rpy, XNET_MSG_RPY, 0, hmo.site_id, msg->tx.ssite_id);
    xnet_msg_fill_reqno(rpy, msg->tx.reqno);
    xnet_msg_fill_cmd(rpy, XNET_RPY_DATA, 0, 0);
    rpy->tx.handle = msg->tx.handle;

    *orpy = rpy;
    
    return 0;
}

static inline
void __mds_send_rpy(struct xnet_msg *rpy, int err)
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
    if (xnet_send(hmo.xc, rpy)) {
        gk_err(mds, "xnet_send() failed.\n");
    }
    xnet_free_msg(rpy);
}

int mds_do_reg(struct xnet_msg *msg)
{
    struct xnet_msg *rpy;
    int err = 0, fd = -1;

    err = __prepare_xnet_msg(msg, &rpy);
    if (err) {
        gk_err(mds, "prepare rpy xnet_msg failed w/ %d\n", err);
        goto out;
    }

    /* ABI:
       @tx.ssite_id: site_id
     * @tx.arg0: socket address and port
     * @tx.reserved: recv_fd (high 32 bits) | gid (low 32 bits)
     */

    /* update the low-level socket to site table */
    fd = (msg->tx.reserved >> 32);
    if (fd > 0) {
        struct sockaddr_in sin;
        void *data;
        int len = sizeof(struct gk_site_tx) + sizeof(struct gk_addr_tx);
        u64 nsite = msg->tx.ssite_id;
        
        sin.sin_family = AF_INET;
        sin.sin_port = msg->tx.arg0 & 0xffffffff;
        sin.sin_addr.s_addr = msg->tx.arg0 >> 32;
        
        data = hst_construct_tcp2(nsite, sin, GK_SITE_REPLACE);
        if (!data) {
            gk_err(xnet, "hst construct from tcp failed\n");
            err = -ENOMEM;
            goto send_rpy;
        }

        err = hst_to_xsst(data, len);
        if (err) {
            gk_err(xnet, "hst to xsst failed w/ %d\n", err);
            xfree(data);
            goto send_rpy;
        }
        xfree(data);

        /* ok, update the sockfd */
        err = gst_update_sockfd(fd, nsite);
        if (err) {
            gk_err(mds, "gst_update_sockfd() fd %d for site %lx failed w/ %d\n",
                   fd, nsite, err);
            goto send_rpy;
        }
    }

send_rpy:
    __mds_send_rpy(rpy, err);
out:
    xnet_free_msg(msg);

    return err;
}

int mds_do_put(struct xnet_msg *msg)
{
    struct xnet_msg *rpy;
    void *data;
    struct gstring namespace, key, value;
    u32 offset = 0;
    int err = 0;

    err = __prepare_xnet_msg(msg, &rpy);
    if (unlikely(err)) {
        gk_err(mds, "prepare rpy xnet_msg failed w/ %d\n", err);
        goto out;
    }

    /* ABI:
     * @tx.arg0: len1(namespace) | len2(key)
     * @tx.arg1: len3(value)
     */
    if (likely(msg->xm_datacheck)) {
        data = msg->xm_data;
    } else {
        gk_err(mds, "Internal error, data lossing ...\n");
        err = -EFAULT;
        goto out;
    }
#if 0
    {
        char namespace[32], key[32], value[128];
        u64 offset = 0;

        memset(namespace, 0, 32);
        memcpy(namespace, data, msg->tx.arg0 >> 32);
        offset += (msg->tx.arg0 >> 32);
        
        memset(key, 0, 32);
        memcpy(key, data + offset, msg->tx.arg0 & 0xffffffff);
        offset += (msg->tx.arg0 & 0xffffffff);
        
        memset(value, 0, 128);
        memcpy(value, data + offset, msg->tx.arg1);

        gk_info(mds, "RECV: %s@%s -> %s\n", namespace, key, value);
    }
#endif
        
    namespace.start = data;
    namespace.len = msg->tx.arg0 >> 32;
    offset += msg->tx.arg0 >> 32;
    key.start = data + offset;
    key.len = msg->tx.arg0 & 0xffffffff;
    offset += (msg->tx.arg0 & 0xffffffff);
    value.start = data + offset;
    value.len = msg->tx.arg1;
    
    err = kvs_put(&namespace, &key, &value);
    if (unlikely(err)) {
        gk_err(mds, "kvs_put() failed w/ %d\n", err);
        goto out;
    }

out:    
    __mds_send_rpy(rpy, err);

    xnet_free_msg(msg);

    return err;
}

int mds_do_get(struct xnet_msg *msg)
{
    struct xnet_msg *rpy;
    void *data;
    int err = 0;

    err = __prepare_xnet_msg(msg, &rpy);
    if (err) {
        gk_err(mds, "prepare rpy xnet_msg failed w/ %d\n", err);
        goto out;
    }

    /* ABI:
     * @tx.arg0: len1(namespace)
     * @tx.arg1: len2(key)
     */
    if (msg->xm_datacheck) {
        data = msg->xm_data;
    } else {
        gk_err(mds, "Internal error, data lossing ...\n");
        err = -EFAULT;
        goto out;
    }
    {
        struct gstring namespace, key, *value;
        u32 offset = 0;

        namespace.start = data;
        namespace.len = msg->tx.arg0;
        offset += msg->tx.arg0;
        key.start = data + offset;
        key.len = msg->tx.arg1;

        value = kvs_get(&namespace, &key);
        if (IS_ERR(value)) {
            if (PTR_ERR(value) != -ENOENT)
                gk_err(mds, "kvs_get() failed w/ %ld\n", PTR_ERR(value));
            err = PTR_ERR(value);
            goto out;
        }
        xnet_msg_add_sdata(rpy, value->start, value->len);
        rpy->tx.arg0 = value->len;
    }

out:
    __mds_send_rpy(rpy, err);

    xnet_free_msg(msg);

    return err;
}
