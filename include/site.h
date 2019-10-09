/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-22 21:51:49 macan>
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

#ifndef __GK_SITE_H__
#define __GK_SITE_H__

/*
 * SITE format: used 20bits
 *
 * |----not used----|--site type--|--site #--|
 *
 * site type: 3bits, ex. <1: client>, <2: mds>, <3: mdsl>, <..>, ...
 * site #: 17bits
 */
#define GK_SITE_TYPE_CLIENT   0x01
#define GK_SITE_TYPE_MDS      0x02
#define GK_SITE_TYPE_MDSL     0x03
#define GK_SITE_TYPE_RING     0x04
#define GK_SITE_TYPE_ROOT     0x04 /* Note that, RING and ROOT are the
                                      * same server now. */
#define GK_SITE_TYPE_OSD      0x05
#define GK_SITE_TYPE_AMC      0x06 /* another metadata client */
#define GK_SITE_TYPE_BP       0x07 /* branch processor */

#define GK_SITE_TYPE_MASK     (0x7 << 17)
#define GK_SITE_MAX           (1 << 20)
#define GK_SITE_TYPE_MAX      (1 << 17)
#define GK_SITE_RANDOM_FLAG   (1 << 21)
#define GK_SITE_RANDOM_SELECT_MAGIC     0xfaed3450acbdefaf

#define GK_IS_CLIENT(site) ((((site) & GK_SITE_TYPE_MASK) >> 17) == \
                              GK_SITE_TYPE_CLIENT)

#define GK_IS_MDS(site) (((site & GK_SITE_TYPE_MASK) >> 17) ==  \
                           GK_SITE_TYPE_MDS)

#define GK_IS_MDSL(site) (((site & GK_SITE_TYPE_MASK) >> 17) == \
                            GK_SITE_TYPE_MDSL)

#define GK_IS_RING(site) (((site & GK_SITE_TYPE_MASK) >> 17) == \
                            GK_SITE_TYPE_RING)

#define GK_IS_ROOT(site) (((site & GK_SITE_TYPE_MASK) >> 17) == \
                            GK_SITE_TYPE_ROOT)

#define GK_IS_AMC(site) (((site & GK_SITE_TYPE_MASK) >> 17) == \
                           GK_SITE_TYPE_AMC)

#define GK_IS_BP(site) (((site & GK_SITE_TYPE_MASK) >> 17) ==   \
                           GK_SITE_TYPE_BP)

#define GK_IS_OSD(site) (((site & GK_SITE_TYPE_MASK) >> 17) ==   \
                           GK_SITE_TYPE_OSD)

#define GK_GET_TYPE(site) ((site & GK_SITE_TYPE_MASK) >> 17)

#define GK_SITE_N_MASK        ((1 << 17) - 1)

#define GK_CLIENT(n) ((GK_SITE_TYPE_CLIENT << 17) | (n & GK_SITE_N_MASK))

#define GK_MDS(n) ((GK_SITE_TYPE_MDS << 17) | (n & GK_SITE_N_MASK))

#define GK_MDSL(n) ((GK_SITE_TYPE_MDSL << 17) | (n & GK_SITE_N_MASK))

#define GK_RING(n) ((GK_SITE_TYPE_RING << 17) | (n & GK_SITE_N_MASK))

#define GK_ROOT(n) ((GK_SITE_TYPE_ROOT << 17) | (n & GK_SITE_N_MASK))

#define GK_AMC(n) ((GK_SITE_TYPE_AMC << 17) | (n & GK_SITE_N_MASK))

#define GK_BP(n) ((GK_SITE_TYPE_BP << 17) | (n & GK_SITE_N_MASK))

#define GK_OSD(n) ((GK_SITE_TYPE_OSD << 17) | (n & GK_SITE_N_MASK))

#define GK_CLIENT_RANDOM() ((GK_SITE_TYPE_CLIENT << 17) | GK_SITE_RANDOM_FLAG)
#define GK_MDS_RANDOM() ((GK_SITE_TYPE_MDS << 17) | GK_SITE_RANDOM_FLAG)

#define GK_IS_RANDOM(n) ((n & GK_SITE_RANDOM_FLAG) != 0)

#endif
