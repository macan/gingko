/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-20 14:35:39 macan>
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
#include "root.h"
#include "xnet.h"
#include "lib.h"

struct spool_mgr
{
    struct list_head reqin;
    xlock_t rin_lock;
    sem_t rin_sem;
};

struct spool_thread_arg
{
    int tid;
};

static struct spool_mgr spool_mgr;

int root_spool_dispatch(struct xnet_msg *msg)
{
    xlock_lock(&spool_mgr.rin_lock);
    list_add_tail(&msg->list, &spool_mgr.reqin);
    xlock_unlock(&spool_mgr.rin_lock);
    atomic64_inc(&hro.prof.misc.reqin_total);
    sem_post(&spool_mgr.rin_sem);

    return 0;
}

static inline
int __serv_request(void)
{
    struct xnet_msg *msg = NULL, *pos, *n;

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
    ASSERT(msg->xc, root);
    ASSERT(msg->xc->ops.dispatcher, root);
    atomic64_inc(&hro.prof.misc.reqin_handle);
    return msg->xc->ops.dispatcher(msg);
}

static
void *spool_main(void *arg)
{
    struct spool_thread_arg *sta = (struct spool_thread_arg *)arg;
    sigset_t set;
    int err = 0;

    /* first, let us block the SIGALRM and SIGCHLD */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    sigaddset(&set, SIGCHLD);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    while (!hro.spool_thread_stop) {
        err = sem_wait(&spool_mgr.rin_sem);
        if (err == EINTR)
            continue;
        gk_debug(root, "Service thread %d wakeup to handle the requests.\n",
                   sta->tid);
        /* trying to handle more and more requsts. */
        while (1) {
            err = __serv_request();
            if (err == -EHSTOP)
                break;
            else if (err) {
                gk_err(root, "Service thread handle request w/ error %d\n",
                         err);
            }
        }
    }
    pthread_exit(0);
}

int root_spool_create(void)
{
    struct spool_thread_arg *sta;
    int i, err = 0;
    
    /* init the mgr struct */
    INIT_LIST_HEAD(&spool_mgr.reqin);
    xlock_init(&spool_mgr.rin_lock);
    sem_init(&spool_mgr.rin_sem, 0, 0);

    /* init service threads' pool */
    if (!hro.conf.service_threads)
        hro.conf.service_threads = 4;

    hro.spool_thread = xzalloc(hro.conf.service_threads * sizeof(pthread_t));
    if (!hro.spool_thread) {
        gk_err(root, "xzalloc() pthread_t failed\n");
        return -ENOMEM;
    }

    sta = xzalloc(hro.conf.service_threads * sizeof(struct spool_thread_arg));
    if (!sta) {
        gk_err(root, "xzalloc() struct spool_thread_arg failed\n");
        err = -ENOMEM;
        goto out_free;
    }

    for (i = 0; i < hro.conf.service_threads; i++) {
        (sta + i)->tid = i;
        err = pthread_create(hro.spool_thread + i, NULL, &spool_main,
                             sta + i);
        if (err)
            goto out;
    }

out:
    return err;
out_free:
    xfree(hro.spool_thread);
    goto out;
}

void root_spool_destroy(void)
{
    int i;

    hro.spool_thread_stop = 1;
    for (i = 0; i < hro.conf.service_threads; i++) {
        sem_post(&spool_mgr.rin_sem);
    }
    for (i = 0; i < hro.conf.service_threads; i++) {
        pthread_join(*(hro.spool_thread + i), NULL);
    }
    sem_destroy(&spool_mgr.rin_sem);
}
