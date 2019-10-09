/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-20 18:22:33 macan>
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

int bparse_hxi(void *data, union gk_x_info **hxi)
{
    u32 len;

    if (!data || !hxi)
        return -EINVAL;

    len = *(int *)data;
    if (len != sizeof(union gk_x_info)) {
        gk_err(root, "bparse hxi failed, hxi length mismatch!\n");
        return -EINVAL;
    }
    *hxi = data + sizeof(u32);

    return len + sizeof(u32);
}

int bparse_root(void *data, struct root_tx **rt)
{
    u32 len;

    if (!data || !rt)
        return -EINVAL;

    len = *(int *)data;
    if (len != sizeof(struct root_tx)) {
        gk_err(root, "bparse root failed, root length mismatch!\n");
        return -EINVAL;
    }
    *rt = data + sizeof(u32);
    
    return len + sizeof(u32);
}

int bparse_addr(void *data, struct gk_site_tx **hst)
{
    u32 len;

    if (!data || !hst)
        return -EINVAL;

    len = *(int *)data;
    *hst = data + sizeof(u32);

    return len + sizeof(u32);
}
