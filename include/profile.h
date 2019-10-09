/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-19 14:20:22 macan>
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

#ifndef __PROFILE_H__MM
#define __PROFILE_H__MM

struct gk_profile_value
{
    u64 value;
};

struct gk_profile_entry
{
    char *name;
    u64 value;
};

#define GK_PROFILE_MAX    (50)

struct gk_profile
{
    int nr;
#define HP_UP2DATE      0x01
    u32 flag;
    struct gk_profile_value hpv[GK_PROFILE_MAX];
};

struct gk_profile_ex
{
    void *fp;
    time_t ts;
    int nr;                     /* detect corrupt requests */
    struct gk_profile_entry hpe[GK_PROFILE_MAX];
};

struct gk_profile_mds_rate
{
    time_t last_update;
    double modify, nonmodify;      /* rates */
    u64 last_modify, last_nonmodify;
};

struct gk_profile_mdsl_rate
{
    time_t last_update;
    double write, read;         /* rates */
    u64 last_write, last_read;
};

#define GK_PROFILE_NAME_ADDIN(hp, idx, iname) do { \
    (hp)->hpe[idx++].name = strdup(iname);           \
    } while (0)

#define GK_PROFILE_VALUE_ADDIN(hp, idx, ivalue) do {   \
        (hp)->hpv[idx++].value = (ivalue);               \
    } while (0)

#define GK_PROFILE_VALUE_UPDATE(hp2, hp, idx) do {    \
        (hp2)->hpe[idx].value += (hp)->hpv[idx].value;  \
    } while (0)

/* API for root serverr */
void gk_mds_profile_setup(struct gk_profile_ex *hpe);

#endif
