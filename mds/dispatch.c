/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-06 09:04:00 macan>
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

int __mdsdisp mds_client_dispatch(struct xnet_msg *msg)
{
#ifdef GK_DEBUG_LATENCY
    lib_timer_def();
    lib_timer_B();
#endif

    switch (msg->tx.cmd) {
    case GK_CLT2MDS_REG:
        mds_do_reg(msg);
        break;
    case GK_CLT2MDS_GET:
        mds_do_get(msg);
        break;
    case GK_CLT2MDS_PUT:
        mds_do_put(msg);
        break;
    default:
        gk_err(mds, "Invalid client2MDS command: %ld from %lx\n", 
               msg->tx.cmd, msg->tx.ssite_id);
        xnet_free_msg(msg);
    }
#ifdef GK_DEBUG_LATENCY
    lib_timer_E();
    lib_timer_O(1, "ALLOC TX and HANDLE.");
#endif
    
    return 0;
}

int __mdsdisp mds_mds_dispatch(struct xnet_msg *msg)
{
    switch (msg->tx.cmd) {
    default:
        gk_err(mds, "Invalid MDS2MDS request %ld from %lx\n",
                 msg->tx.cmd, msg->tx.ssite_id);
        xnet_free_msg(msg);
    }

    return 0;
}

int __mdsdisp mds_root_dispatch(struct xnet_msg *msg)
{
    switch (msg->tx.cmd) {
    case GK_FR2_AU:
        mds_addr_table_update(msg);
        break;
    default:
        gk_err(mds, "Invalid request %d from R2 %lx.\n",
                 msg->tx.reqno, msg->tx.ssite_id);
        xnet_free_msg(msg);
    }
    
    return 0;
}
