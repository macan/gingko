/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-29 16:42:49 macan>
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
#include "mds.h"
#include "prof.h"
#include "profile.h"
#include "async.h"

static inline
void dump_profiling_r2(time_t t, struct gk_profile *hp)
{
    int i = 0;
    
    if (!hmo.conf.profiling_thread_interval)
        return;
    if (t < hmo.prof.ts + hmo.conf.profiling_thread_interval) {
        return;
    }
    hmo.prof.ts = t;
    hp->flag |= HP_UP2DATE;

    /* submit a async send request */
    {
        struct async_update_request *aur =
            xzalloc(sizeof(struct async_update_request));
        int err = 0;

        if (unlikely(!aur)) {
            gk_err(mds, "xzalloc() AU request faield, ignore this update.\n");
        } else {
            aur->op = AU_PROFILE;
            aur->arg = (u64)(&hmo.hp);
            INIT_LIST_HEAD(&aur->list);
            err = au_submit(aur);
            if (err) {
                gk_err(mds, "submit AU request failed, ignore this update.\n");
            }
        }
    }
}

static inline
void dump_profiling_plot(time_t t)
{
    if (!hmo.conf.profiling_thread_interval)
        return;
    if (t < hmo.prof.ts + hmo.conf.profiling_thread_interval) {
        return;
    }
    hmo.prof.ts = t;
}

static inline
void dump_profiling_human(time_t t)
{
    if (!hmo.conf.profiling_thread_interval)
        return;
    if (t < hmo.prof.ts + hmo.conf.profiling_thread_interval) {
        return;
    }
    hmo.prof.ts = t;
    gk_info(mds, "ts %ld ns_ins_collisions=%ld ns_lkp_collisions=%ld\n", t,
            atomic64_read(&hmo.prof.mds.ns_ins_collisions),
            atomic64_read(&hmo.prof.mds.ns_lkp_collisions)
        );
}

void dump_profiling(time_t t, struct gk_profile *hp)
{
    if (hmo.state < HMO_STATE_LAUNCH)
        return;

    switch (hmo.conf.prof_plot) {
    case MDS_PROF_PLOT:
        dump_profiling_plot(t);
        break;
    case MDS_PROF_HUMAN:
        dump_profiling_human(t);
        break;
    case MDS_PROF_R2:
        /* always send the current profiling copy to GK_RING(0)? */
        dump_profiling_r2(t, hp);
        break;
    case MDS_PROF_NONE:
    default:
        ;
    }
}
