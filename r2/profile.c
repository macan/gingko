/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-21 21:56:43 macan>
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

#include "root.h"

static struct gk_profile_mds_rate g_hpmr = {0, 0.0, 0.0, 0, 0,};

/* This profile unit recv requests from other sites and write to corresponding
 * log file
 */
void gk_mds_profile_setup(struct gk_profile_ex *hp)
{
    int i = 0;

    GK_PROFILE_NAME_ADDIN(hp, i, "timestamp");
    GK_PROFILE_NAME_ADDIN(hp, i, "xnet.msg_alloc");
    GK_PROFILE_NAME_ADDIN(hp, i, "xnet.msg_free");
    GK_PROFILE_NAME_ADDIN(hp, i, "xnet.inbytes");
    GK_PROFILE_NAME_ADDIN(hp, i, "xnet.outbytes");
    GK_PROFILE_NAME_ADDIN(hp, i, "xnet.active_links");
    GK_PROFILE_NAME_ADDIN(hp, i, "misc.reqin_total");
    GK_PROFILE_NAME_ADDIN(hp, i, "misc.reqin_handle");
    GK_PROFILE_NAME_ADDIN(hp, i, "misc.reqin_drop");
    GK_PROFILE_NAME_ADDIN(hp, i, "mds.gossip_ft");
    GK_PROFILE_NAME_ADDIN(hp, i, "misc.reqin_qd");
    hp->nr = i;
}

int root_setup_profile(void)
{
    struct gk_profile_ex *hp;
    FILE *fp;
    char fname[256];
    char data[4096];
    size_t len;
    int i;

    /* sanity check */
    if (!hro.conf.profiling_file) {
        return -EINVAL;
    }
    
    /* Setup up mds profile */
    hp = &hro.hp_mds;
    memset(hp, 0, sizeof(*hp));
    gk_mds_profile_setup(hp);
    memset(fname, 0, sizeof(fname));
    snprintf(fname, 255, "%s.mds", hro.conf.profiling_file);
    fp = fopen(fname, "w+");
    if (!fp) {
        gk_err(xnet, "fopen() profiling file %s failed %d\n",
                 fname, errno);
        return -EINVAL;
    }
    len = fwrite("## ##\n", 1, 6, fp);
    if (len < 6) {
        gk_err(xnet, "fwrite() profiling file %s failed %d\n",
                 fname, errno);
        return -errno;
    }
    memset(data, 0, 4096);
    len = sprintf(data, "@GK MDS PLOT DATA FILE :)\nlocal_ts");
    for (i = 0; i < hp->nr; i++) {
        len += sprintf(data + len, " %s", hp->hpe[i].name);
    }
    len += sprintf(data + len, "\n");
    if (fwrite(data, 1, len, fp) < len) {
        gk_err(xnet, "fwrite() profiling file %s failed %d\n",
                 fname, errno);
        return -errno;
    }
    fflush(fp);
    hro.hp_mds.fp = fp;
    
    /* FIXME: Setup up bp profile */
    /* FIXME: Setup up client profile */

    return 0;
}

/* try to update metadata rate on each mds profile update */
static inline
void __root_profile_update_mds_rate(struct gk_profile_ex *hp)
{
    time_t cur = time(NULL);

    if (cur == g_hpmr.last_update)
        return;
    
    /* 3th entry is modify counter */
    g_hpmr.modify = (double)(hp->hpe[3].value - g_hpmr.last_modify) /
        (cur - g_hpmr.last_update);
    g_hpmr.nonmodify = (double)(hp->hpe[2].value - g_hpmr.last_nonmodify) /
        (cur - g_hpmr.last_update);

    g_hpmr.last_update = cur;
    g_hpmr.last_modify = hp->hpe[3].value;
    g_hpmr.last_nonmodify = hp->hpe[2].value;
}

int root_profile_update_mds(struct gk_profile *hp, 
                            struct xnet_msg *msg)
{
    int err = 0, i;

    if (hp->nr != hro.hp_mds.nr) {
        gk_err(xnet, "Invalid MDS request from %lx, nr mismatch "
                 "%d vs %d\n",
                 msg->tx.ssite_id, hp->nr, hro.hp_mds.nr);
        goto out;
    }

    for (i = 0; i < hp->nr; i++) {
        GK_PROFILE_VALUE_UPDATE(&hro.hp_mds, hp, i);
    }

    __root_profile_update_mds_rate(&hro.hp_mds);
    
out:
    return err;
}

int root_profile_update_bp(struct gk_profile *hp,
                           struct xnet_msg *msg)
{
    gk_err(xnet, "BP profile has not been implemented yet\n");
    return -ENOSYS;
}

int root_profile_update_client(struct gk_profile *hp,
                               struct xnet_msg *msg)
{
    gk_err(xnet, "Client profile has not been implemented yet\n");
    return -ENOSYS;
}

void root_profile_flush(time_t cur)
{
    static time_t last = 0;
    char data[1024];
    size_t len;
    int i;

    if (cur >= last + hro.conf.profile_interval) {
        last = cur;
    } else {
        return;
    }
    
    /* flush mds profile */
    memset(data, 0, sizeof(data));
    len = sprintf(data, "%ld", cur);
    for (i = 0; i < hro.hp_mds.nr; i++) {
        len += sprintf(data + len, " %ld", hro.hp_mds.hpe[i].value);
    }
    len += sprintf(data + len, "\n");
    if (fwrite(data, 1, len, hro.hp_mds.fp) < len) {
        gk_err(xnet, "fwrite() profiling file MDS failed %d\n",
                 errno);
    }
    fflush(hro.hp_mds.fp);
    
    /* FIXME: flush bp profile */
    /* FIXME: flush client profile */
}

int root_info_mds(u64 arg, void **buf)
{
    char *p;
    int err = 0, i;

    p = xzalloc(4096 << 2);
    if (!p) {
        gk_err(root, "xzalloc() info mds buffer failed\n");
        err = -ENOMEM;
        goto out;
    }
    *buf = (void *)p;
    
    switch (arg) {
    case GK_SYSINFO_MDS_RAW:
        p += sprintf(p, "MDS RAW:\n");
        for (i = 0; i < hro.hp_mds.nr; i++) {
            p += sprintf(p, " -> %20s\t\t%ld\n", hro.hp_mds.hpe[i].name,
                         hro.hp_mds.hpe[i].value);
        }
        break;
    default:
    case GK_SYSINFO_MDS_RATE:
        p += sprintf(p, "MDS Rate:\n -> [Modify] %10.4f/s "
                     "[NonModify] %10.4f/s\n",
                     g_hpmr.modify, g_hpmr.nonmodify);
    }

out:
    return err;
}

int root_info_root(u64 arg, void **buf)
{
    char *p;
    int err = 0;

    p = xzalloc(4096 << 2);
    if (!p) {
        gk_err(root, "xzalloc() info osd buffer failed\n");
        err = -ENOMEM;
        goto out;
    }
    *buf = (void *)p;

    p += sprintf(p, " -> %20s\t\t%ld\n", "misc.reqin_total", 
                 atomic64_read(&hro.prof.misc.reqin_total));
    p += sprintf(p, " -> %20s\t\t%ld\n", "misc.reqin_handle", 
                 atomic64_read(&hro.prof.misc.reqin_handle));
    p += sprintf(p, " -> %20s\t\t%ld\n", "osd.objrep_recved", 
                 atomic64_read(&hro.prof.osd.objrep_recved));
    p += sprintf(p, " -> %20s\t\t%ld\n", "osd.objrep_handled", 
                 atomic64_read(&hro.prof.osd.objrep_handled));
    

out:
    return err;
}
