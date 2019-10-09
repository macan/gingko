/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-19 17:58:47 macan>
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

#ifndef __MDS_ASYNC_H__
#define __MDS_ASYNC_H__

#include "gk.h"

struct async_thread_arg
{
    int tid;                    /* thread id */
};

struct async_update_request
{
#define AU_PROFILE              0x06    /* w/ gk_profile in arg */

#define AU_BRANCH_REGION_BEGIN  0xf0000000
#define AU_BRANCH_REGION_END    0xffffffff
    u64 op;
    u64 arg;
    struct list_head list;
};

struct async_update_mlist
{
    struct list_head aurlist;
    xlock_t lock;
};

/* APIs */
int au_submit(struct async_update_request *);
int au_lookup(int, u64);

#endif
