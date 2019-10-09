/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-30 15:42:42 macan>
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
#include "xnet.h"
#include "lib.h"
#include "kvs.h"

struct spool_mgr
{
    struct list_head reqin;
    struct list_head modify_req; /* for suspending modify requests */
    struct list_head paused_req; /* for paused pending request */
    xlock_t rin_lock;
    xlock_t pmreq_lock;
    sem_t rin_sem;
};

struct spool_thread_arg
{
    int tid;
};

static struct spool_mgr spool_mgr;

pthread_key_t spool_key;

int mds_spool_dispatch(struct xnet_msg *msg)
{
    xlock_lock(&spool_mgr.rin_lock);
    list_add_tail(&msg->list, &spool_mgr.reqin);
    xlock_unlock(&spool_mgr.rin_lock);
    atomic64_inc(&hmo.prof.misc.reqin_total);
    atomic64_inc(&hmo.prof.misc.reqin_qd);
    sem_post(&spool_mgr.rin_sem);

    return 0;
}

void mds_spool_redispatch(struct xnet_msg *msg, int sempost)
{
    xlock_lock(&spool_mgr.rin_lock);
    list_add_tail(&msg->list, &spool_mgr.reqin);
    xlock_unlock(&spool_mgr.rin_lock);
    atomic64_inc(&hmo.prof.misc.reqin_qd);
    if (sempost)
        sem_post(&spool_mgr.rin_sem);
}

int mds_spool_modify_pause(struct xnet_msg *msg)
{
    xlock_lock(&spool_mgr.pmreq_lock);
    list_add_tail(&msg->list, &spool_mgr.modify_req);
    xlock_unlock(&spool_mgr.pmreq_lock);
    atomic64_inc(&hmo.prof.mds.paused_mreq);
    atomic64_inc(&hmo.prof.misc.reqin_qd);
    
    return 0;
}

void mds_spool_provoke(void)
{
    sem_post(&spool_mgr.rin_sem);
}

static inline
void mds_spool_modify_resume(void)
{
    int i;

    for (i = 0; i < hmo.conf.spool_threads; i++) {
        sem_post(&spool_mgr.rin_sem);
    }
}

/* This function should be called BEFORE mp_check, we always check if there
 * are too many ITBs in the cache. (Out Of Date)
 */
void mds_spool_mp_check(time_t t)
{
}

static inline
int __serv_request(void)
{
    struct xnet_msg *msg = NULL, *pos, *n;

    if (likely(!hmo.spool_modify_pause)) {
        if (unlikely(!list_empty(&spool_mgr.modify_req))) {
            xlock_lock(&spool_mgr.pmreq_lock);
            list_for_each_entry_safe(pos, n, &spool_mgr.modify_req, list) {
                list_del_init(&pos->list);
                msg = pos;
                break;
            }
            xlock_unlock(&spool_mgr.pmreq_lock);
            if (msg) {
                atomic64_dec(&hmo.prof.misc.reqin_qd);
                return msg->xc->ops.dispatcher(msg);
            }
        }
    }
    if (likely(!hmo.reqin_pause)) {
        if (unlikely(!list_empty(&spool_mgr.paused_req))) {
            xlock_lock(&spool_mgr.rin_lock);
            list_splice_init(&spool_mgr.paused_req, &spool_mgr.reqin);
            xlock_unlock(&spool_mgr.rin_lock);
        }
    }
    
    xlock_lock(&spool_mgr.rin_lock);
    list_for_each_entry_safe(pos, n, &spool_mgr.reqin, list) {
        list_del_init(&pos->list);
        msg = pos;
        break;
    }
    xlock_unlock(&spool_mgr.rin_lock);

    if (!msg)
        return -EHSTOP;

    /* ok, deal with it, we just calling the secondary dispatcher */
    /* NOTE: important!
     *
     * reqin_pause will aggr many incoming request, if we have 256 or more
     * requests pending, we begin dropping request. Thus, it is important to
     * place hmo.reqin_drop branch BEFORE hmo.reqin_pause!
     */
    if (likely(!(hmo.reqin_drop | hmo.reqin_pause))) {
    dispatch:
        atomic64_dec(&hmo.prof.misc.reqin_qd);
        atomic64_inc(&hmo.prof.misc.reqin_handle);
        return msg->xc->ops.dispatcher(msg);
    } else if (hmo.reqin_drop) {
        if (GK_IS_CLIENT(msg->tx.ssite_id) ||
            GK_IS_AMC(msg->tx.ssite_id)) {
            atomic64_inc(&hmo.prof.misc.reqin_drop);
            xnet_free_msg(msg);
        } else {
            goto dispatch;
        }
    } else if (hmo.reqin_pause) {
        /* we should iterate on reqin list to drain R2 messages! */
        if (GK_IS_CLIENT(msg->tx.ssite_id) ||
            GK_IS_AMC(msg->tx.ssite_id)) {
            if (atomic64_read(&hmo.prof.misc.reqin_qd) > 256) {
                hmo.reqin_drop = 1;
            }
            /* re-insert this request to paused req list */
            xlock_lock(&spool_mgr.rin_lock);
            list_add_tail(&msg->list, &spool_mgr.paused_req);
            xlock_unlock(&spool_mgr.rin_lock);
        } else {
            goto dispatch;
        }
    }

    return 0;
}

static
void *spool_main(void *arg)
{
    struct spool_thread_arg *sta = (struct spool_thread_arg *)arg;
    struct ns_entry *nse;
    sigset_t set;
    int err = 0;

    /* first, let us block the SIGALRM and SIGCHLD */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigaddset(&set, SIGCHLD);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    
    while (!hmo.spool_thread_stop) {
        err = sem_wait(&spool_mgr.rin_sem);
        if (err == EINTR)
            continue;
        gk_debug(mds, "Service thread %d wakeup to handle the requests.\n",
                   sta->tid);
        /* trying to handle more and more requsts. */
        while (1) {
            err = __serv_request();
            if (err == -EHSTOP)
                break;
            else if (err) {
                gk_err(mds, "Service thread handle request w/ error %d\n",
                         err);
                break;
            }
        }
    }
    nse = (struct ns_entry *)pthread_getspecific(spool_key);
    if (nse) {
        kvs_ns_put(nse);
    }

    pthread_exit(0);
}

int mds_spool_create(void)
{
    struct spool_thread_arg *sta;
    int i, err = 0;
    
    /* init the mgr struct */
    INIT_LIST_HEAD(&spool_mgr.reqin);
    INIT_LIST_HEAD(&spool_mgr.modify_req);
    INIT_LIST_HEAD(&spool_mgr.paused_req);
    xlock_init(&spool_mgr.rin_lock);
    xlock_init(&spool_mgr.pmreq_lock);
    sem_init(&spool_mgr.rin_sem, 0, 0);
    hmo.spool_modify_pause = 0;

    pthread_key_create(&spool_key, NULL);

    /* init service threads' pool */
    if (!hmo.conf.spool_threads)
        hmo.conf.spool_threads = 4;

    hmo.spool_thread = xzalloc(hmo.conf.spool_threads * sizeof(pthread_t));
    if (!hmo.spool_thread) {
        gk_err(mds, "xzalloc() pthread_t failed\n");
        return -ENOMEM;
    }

    sta = xzalloc(hmo.conf.spool_threads * sizeof(struct spool_thread_arg));
    if (!sta) {
        gk_err(mds, "xzalloc() struct spool_thread_arg failed\n");
        err = -ENOMEM;
        goto out_free;
    }

    for (i = 0; i < hmo.conf.spool_threads; i++) {
        (sta + i)->tid = i;
        err = pthread_create(hmo.spool_thread + i, NULL, &spool_main,
                             sta + i);
        if (err)
            goto out;
    }

out:
    return err;
out_free:
    xfree(hmo.spool_thread);
    goto out;
}

void mds_spool_destroy(void)
{
    int i;

    hmo.spool_thread_stop = 1;
    for (i = 0; i < hmo.conf.spool_threads; i++) {
        sem_post(&spool_mgr.rin_sem);
    }
    for (i = 0; i < hmo.conf.spool_threads; i++) {
        pthread_join(*(hmo.spool_thread + i), NULL);
    }
    sem_destroy(&spool_mgr.rin_sem);
}
