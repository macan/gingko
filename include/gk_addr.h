/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-20 13:36:34 macan>
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

#ifndef __GK_ADDR_H__
#define __GK_ADDR_H__

#include "gk_u.h"

struct gk_sock_addr
{
    struct sockaddr sa;
};

/* gk_sock_addr_tx for NET transfer */
struct gk_sock_addr_tx
{
    struct sockaddr sa;
};

struct gk_addr
{
    struct list_head list;
    /* stable flag, saved */
#define GK_SITE_PROTOCOL_TCP  0x80000000
    u32 flag;
    union 
    {
        struct gk_sock_addr sock;
    };
};

struct gk_addr_tx
{
    u32 flag;
    union 
    {
        struct gk_sock_addr_tx sock;
    };
};

struct gk_site
{
    /* caller flag, not saved */
#define GK_SITE_REPLACE       0x00008000 /* replace all the addr */
#define GK_SITE_ADD           0x00004000
#define GK_SITE_DEL           0x00002000
    u32 flag;
    u32 nr;
    u64 fsid;
    struct list_head addr;      /* addr list */
};

/* gk_site_tx for NET transfer */
struct gk_site_tx
{
    u64 site_id;
    u32 flag;
    u32 nr;
    struct gk_addr_tx addr[0];
};

/* The following is the region for HXI exchange ABI */
/*
 * Read(open) from the r2 manager. If reopen an opened one, r2 should initiate
 * the recover process to get the newest values from the MDSLs.
 */
#define HMI_STATE_CLEAN         0x01
#define HMI_STATE_LASTOPEN      0x02
#define HMI_STATE_LASTMIG       0x03
#define HMI_STATE_LASTPAUSE     0x04

struct gk_client_info
{
    u32 state;
    u32 pad;
    u64 fsid;
    u64 group;
};

struct gk_mds_info 
{
    u32 state;
    u32 pad;
    u64 fsid;
    u64 group;
};

/*
 * Note: we just saving the data region to the storage, ourself do not
 * interpret it.
 */
#define GK_X_INFO_LEN         (256)
union gk_x_info
{
    u8 array[GK_X_INFO_LEN];
    struct gk_mds_info hmi;
    struct gk_client_info hci;
};

/* please refer to r2/mgr.h struct root, this is a mirror of that structure */
struct root_tx
{
    u64 fsid;
};

/* this is just a proxy array of config file line */
struct conf_site
{
    char *type;
    char *node;
    int port;
    int id;
};

#endif
