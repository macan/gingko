/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-03 18:25:03 macan>
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

#ifndef __GK_H__
#define __GK_H__

#ifdef __KERNEL__
#include "gk_k.h"
#else  /* !__KERNEL__ */
#include "gk_u.h"
#endif

#include "tracing.h"
#include "memory.h"
#include "xlock.h"
#include "gk_common.h"
#include "gk_const.h"
#include "gk_addr.h"
#include "xhash.h"
#include "site.h"
#include "xprof.h"

/* This section for GK cmds & reqs */
/* Client to MDS */
#define GK_CLT2MDS_BASE       0x8000000000000000
#define GK_CLT2MDS_REG        0x8001000000000000
#define GK_CLT2MDS_GET        0x8002000000000000
#define GK_CLT2MDS_PUT        0x8004000000000000
#define GK_CLT2MDS_UPDATE     0x8008000000000000

#define GK_CLT2MDS_RDONLY     (GK_CLT2MDS_GET)

/* MDS to MDS */
#define GK_MDS2MDS_FWREQ      0x0000000080000000 /* forward req */
#define GK_MDS2MDS_SPITB      0x0000000080000001 /* split itb */
#define GK_MDS2MDS_AUPDATE    0x0000000080000002 /* async update */
#define GK_MDS2MDS_REDODELTA  0x0000000080000003 /* redo delta */
#define GK_MDS2MDS_LB         0x0000000080000004 /* load bitmap */
#define GK_MDS2MDS_LD         (GK_CLT2MDS_LD)  /* load directory hash
                                                    * info */
#define GK_MDS2MDS_AUBITMAP   0x0000000080000006 /* async bitmap flip */
#define GK_MDS2MDS_AUBITMAP_R 0x0000000080000007 /* async bitmap flip confirm */
#define GK_MDS2MDS_AUDIRDELTA 0x0000000080000008 /* async dir delta update */
#define GK_MDS2MDS_AUDIRDELTA_R 0x0000000080000009 /* async dir delta update
                                                      * confirm */
#define GK_MDS2MDS_GB         0x000000008000000a /* gossip bitmap */
#define GK_MDS2MDS_GF         0x000000008000000b /* gossip ft info */
#define GK_MDS2MDS_GR         0x000000008000000c /* gossip rdir */
#define GK_MDS2MDS_BRANCH     0x000000008000000f /* branch commands */
#define GK_MDS_HA             0x0000000080000010 /* ha request */
#define GK_MDS_RECOVERY       0x0000000080000020 /* recovery analyse
                                                    * request */
/* begin recovery subregion: arg0 */
/* end recovery subregion */

/* MDSL to MDS */
/* RING/ROOT to MDS */
#define GK_R22MDS_COMMIT      0x0000000020000001 /* trigger a snapshot */
#define GK_R22MDS_PAUSE       0x0000000020000002 /* pause client/amc request
                                                    * handling */
#define GK_R22MDS_RESUME      0x0000000020000003 /* resume request
                                                    * handling */
#define GK_R22MDS_RUPDATE     0x0000000020000004 /* ring update */

/* AMC to MDS */
#define GK_AMC2MDS_REQ        0x0000000040000001 /* normal request */
#define GK_AMC2MDS_EXT        0x0000000040000002 /* extend request */

/* MDS to MDSL */
#define GK_MDS2MDSL_ITB       0x0000000080010000
#define GK_MDS2MDSL_BITMAP    0x0000000080020000
#define GK_MDS2MDSL_WBTXG     0x0000000080030000
/* subregion for arg0 */
#define GK_WBTXG_BEGIN        0x0001
#define GK_WBTXG_ITB          0x0002
#define GK_WBTXG_BITMAP_DELTA 0x0004
#define GK_WBTXG_DIR_DELTA    0x0008
#define GK_WBTXG_R_DIR_DELTA  0x0010
#define GK_WBTXG_CKPT         0x0020
#define GK_WBTXG_END          0x0040
#define GK_WBTXG_RDIR         0x0080
/* end subregion for arg0 */
#define GK_MDS2MDSL_WDATA     0x0000000080040000
#define GK_MDS2MDSL_BTCOMMIT  0x0000000080100000
#define GK_MDS2MDSL_ANALYSE   0x0000000080200000
/* subregion for arg0 */
#define GK_ANA_MAX_TXG        0x0001 /* find the max txg */
#define GK_ANA_UPDATE_LIST    0x0002 /* find the ddb and bdb (not) need
                                        * update */
/* end subregion for arg0 */

/* Client to MDSL */
#define GK_CLT2MDSL_READ      0x0000000080050000
#define GK_CLT2MDSL_WRITE     0x0000000080060000
#define GK_CLT2MDSL_SYNC      0x0000000080070000
#define GK_CLT2MDSL_BGSEARCH  0x0000000080080000
#define GK_CLT2MDSL_STATFS    0x0000000080090000

/* * to ROOT/RING */
#define GK_R2_REG             0x0000000040000001
#define GK_R2_UNREG           0x0000000040000002
#define GK_R2_UPDATE          0x0000000040000003
#define GK_R2_HB              0x0000000040000008 /* heart beat */
#define GK_R2_FTREPORT        0x0000000040000018 /* ft report */
#define GK_R2_SHUTDOWN        0x0000000040000022 /* shutdown site */
#define GK_R2_PROFILE         0x0000000040000023 /* gather profile */
#define GK_R2_INFO            0x0000000040000024 /* get info */
#define GK_R2_GETASITE        0x0000000040000026 /* get active site */

/* ROOT/RING to * */
#define GK_FR2_RU             0x0000000041000000 /* ring updates to all
                                                    * sites */
#define GK_FR2_AU             0x0000000042000000 /* address table updates to
                                                    * all sites */

/* * to OSD */
#define GK_OSD_READ           0x0000000010000001 /* object read */
#define GK_OSD_WRITE          0x0000000010000002 /* object write */
#define GK_OSD_SYNC           0x0000000010000003 /* object sync */
#define GK_OSD_STATFS         0x0000000010000004 /* stat whole server */
#define GK_OSD_QUERY          0x0000000010000005 /* query on specific object */
#define GK_OSD_SWEEP          0x0000000010000006 /* object region sweep ZERO */
#define GK_OSD_TRUNC          0x0000000010000007 /* object truncate */
#define GK_OSD_DEL            0x0000000010000008 /* object removal */

/* APIs */
#define HASH_SEL_EH     0x00
#define HASH_SEL_CBHT   0x01
#define HASH_SEL_RING   0x02
#define HASH_SEL_DH     0x03
#define HASH_SEL_GDT    0x04
#define HASH_SEL_VSITE  0x05
#define HASH_SEL_KVS    0x06

#include "hash.c"

#define __xnet __attribute__((__section__(".xnet.text")))
#define __mdsdisp __attribute__((__section__(".mds.dispatch.text")))
#define __UNUSED__ __attribute__((unused))

#define BITMAP_ROUNDUP(x) (((x + 1) + XTABLE_BITMAP_SIZE - 1) & \
                           ~(XTABLE_BITMAP_SIZE - 1))
#define BITMAP_ROUNDDOWN(x) ((x) & (~((XTABLE_BITMAP_SIZE) - 1)))

#define PAGE_ROUNDUP(x, p) ((((size_t) (x)) + (p) - 1) & ~((p) - 1))

#endif
