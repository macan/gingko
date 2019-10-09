/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-19 18:09:16 macan>
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
#include "lib.h"
#include "async.h"

struct async_update_mlist g_aum;

int __aur_profile(struct async_update_request *aur)
{
    static struct gk_profile ghp = {.nr = 0,};
    struct gk_profile diff;
    struct gk_profile *hp = (void *)aur->arg;
    struct xnet_msg *msg;
    u64 dsite;
    int err = 0, i;

    /* if hp has not been inited, we do not send it */
    if (!hp->nr || !hp->hpv[0].value || !(hp->flag & HP_UP2DATE))
        return 0;

    if (!ghp.nr) {
        diff = ghp = *hp;
        /* reset time stamp to ZERO */
        diff.hpv[0].value = 0;
    } else {
        diff = *hp;
        for (i = 0; i < hp->nr; i++) {
            diff.hpv[i].value -= ghp.hpv[i].value;
        }
        ghp = *hp;
    }

    /* reset the flag now */
    hp->flag &= (~HP_UP2DATE);

    /* prepare the xnet_msg */
    msg = xnet_alloc_msg(XNET_MSG_NORMAL);
    if (!msg) {
        gk_err(mds, "xnet_alloc_msg() failed.\n");
        err = -ENOMEM;
        goto out;
    }
    
    /* send this profile to r2 server */
    dsite = mds_select_ring(&hmo);
    xnet_msg_fill_tx(msg, XNET_MSG_REQ, 0, hmo.site_id, dsite);
    xnet_msg_fill_cmd(msg, GK_R2_PROFILE, 0, 0);
#ifdef XNET_EAGER_WRITEV
    xnet_msg_add_sdata(msg, &msg->tx, sizeof(msg->tx));
#endif
    xnet_msg_add_sdata(msg, &diff, sizeof(diff));

    err = xnet_send(hmo.xc, msg);
    if (err) {
        gk_err(mds, "Profile request to R2(%lx) failed w/ %d\n",
                 dsite, err);
        goto out_free_msg;
    }

out_free_msg:
    xnet_free_msg(msg);
out:
    return err;
}

int __au_req_handle(void)
{
    struct async_update_request *aur = NULL, *n;
    int err = 0, found = 0;

    xlock_lock(&g_aum.lock);
    if (!list_empty(&g_aum.aurlist)) {
        list_for_each_entry_safe(aur, n, &g_aum.aurlist, list) {
            list_del_init(&aur->list);
            found = 1;
            break;
        }
    }
    xlock_unlock(&g_aum.lock);

    if (!found)
        return -EHSTOP;
    
    /* ok, deal with it */
    switch (aur->op) {
        /* the following is the MDS used async operations */
    case AU_PROFILE:
        err = __aur_profile(aur);
        break;
        /* the following is the BRANCH used async operations */
    default:
        gk_err(mds, "Invalid AU Request: op %ld arg 0x%lx\n",
                     aur->op, aur->arg);
    }
    atomic64_inc(&hmo.prof.misc.au_handle);
    if (err != -ERETRY)
        xfree(aur);

    return err;
}

int au_submit(struct async_update_request *aur)
{
    xlock_lock(&g_aum.lock);
    list_add_tail(&aur->list, &g_aum.aurlist);
    xlock_unlock(&g_aum.lock);
    atomic64_inc(&hmo.prof.misc.au_submit);
    sem_post(&hmo.async_sem);

    return 0;
}

void *async_update(void *arg)
{
    struct async_thread_arg *ata = (struct async_thread_arg *)arg;
    sigset_t set;
    int err, c;

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigaddset(&set, SIGCHLD);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */

    while (!hmo.async_thread_stop) {
        err = sem_wait(&hmo.async_sem);
        if (err == EINTR)
            continue;
        gk_debug(mds, "Async update thread %d wakeup to progress the AUs.\n",
                   ata->tid);
        /* processing N request in the list */
        for (c = 0; c < hmo.conf.async_update_N; c++) {
            err = __au_req_handle();
            if (err == -EHSTOP)
                break;
            else if (err) {
                gk_err(mds, "AU(async) handle error %d\n", err);
            }
        }
    }
    pthread_exit(0);
}

/* async_tp_init()
 */
int async_tp_init(void)
{
    struct async_thread_arg *ata;
    pthread_attr_t attr;
    int i, err = 0, stacksize;

    /* init the thread stack size */
    err = pthread_attr_init(&attr);
    if (err) {
        gk_err(mds, "Init pthread attr failed\n");
        goto out;
    }
    stacksize = (hmo.conf.stacksize > (1 << 20) ? 
                 hmo.conf.stacksize : (2 << 20));
    err = pthread_attr_setstacksize(&attr, stacksize);
    if (err) {
        gk_err(mds, "set thread stack size to %d failed w/ %d\n", 
                 stacksize, err);
        goto out;
    }
    
    /* init the global manage structure */
    INIT_LIST_HEAD(&g_aum.aurlist);
    xlock_init(&g_aum.lock);
    
    sem_init(&hmo.async_sem, 0, 0);

    /* init async threads' pool */
    if (!hmo.conf.async_threads)
        hmo.conf.async_threads = 4;

    hmo.async_thread = xzalloc(hmo.conf.async_threads * sizeof(pthread_t));
    if (!hmo.async_thread) {
        gk_err(mds, "xzalloc() pthread_t failed\n");
        return -ENOMEM;
    }

    ata = xzalloc(hmo.conf.async_threads * sizeof(struct async_thread_arg));
    if (!ata) {
        gk_err(mds, "xzalloc() struct async_thread_arg failed\n");
        err = -ENOMEM;
        goto out_free;
    }

    for (i = 0; i < hmo.conf.async_threads; i++) {
        (ata + i)->tid = i;
        err = pthread_create(hmo.async_thread + i, &attr, &async_update,
                             ata + i);
        if (err)
            goto out;
    }
    
out:
    return err;
out_free:
    xfree(hmo.async_thread);
    goto out;
}

void async_tp_destroy(void)
{
    int i;

    hmo.async_thread_stop = 1;
    for (i = 0; i < hmo.conf.async_threads; i++) {
        sem_post(&hmo.async_sem);
    }
    for (i = 0; i < hmo.conf.async_threads; i++) {
        pthread_join(*(hmo.async_thread + i), NULL);
    }
    sem_destroy(&hmo.async_sem);
}
