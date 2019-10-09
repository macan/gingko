/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-03 17:57:50 macan>
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

#ifdef GK_TRACING
//u32 gk_mds_tracing_flags = GK_DEFAULT_LEVEL | GK_DEBUG;
u32 gk_mds_tracing_flags = GK_DEFAULT_LEVEL;

void mds_reset_tracing_flags(u64 flag)
{
    gk_mds_tracing_flags = flag;
}
#endif

/* Global variable */
struct gk_mds_info hmi;
struct gk_mds_object hmo = {.conf.option = 0,};

void mds_sigaction_default(int signo, siginfo_t *info, void *arg)
{
#ifdef GK_DEBUG_LOCK
    if (signo == SIGINT) {
        lock_table_print();
        return;
    }
#endif
    if (signo == SIGSEGV || signo == SIGBUS || signo == SIGABRT) {
        gk_info(lib, "Recv %sSIGSEGV/SIGBUS/SIGABRT%s %s @ addr %p\n",
                  GK_COLOR_RED,
                  GK_COLOR_END,
                  SIGCODES(info->si_code),
                  info->si_addr);
        lib_segv(signo, info, arg);
    }
    if (signo == SIGHUP) {
        gk_info(lib, "Exit MDS Server ...\n");
        mds_destroy();
        exit(0);
    }
    if (signo == SIGUSR1) {
        gk_info(lib, "Exit some threads ...\n");
        pthread_exit(0);
    }
    
    return;
}

/* mds_init_signal()
 */
static int mds_init_signal(void)
{
    struct sigaction ac;
    int err;
    
    ac.sa_sigaction = mds_sigaction_default;
    err = sigemptyset(&ac.sa_mask);
    if (err) {
        err = errno;
        goto out;
    }
    ac.sa_flags = SA_SIGINFO;

#ifndef UNIT_TEST
    err = sigaction(SIGTERM, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGHUP, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    /* FIXME: mask the SIGINT for testing */
#if 0
    err = sigaction(SIGINT, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
#endif
    err = sigaction(SIGSEGV, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGBUS, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGQUIT, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = sigaction(SIGUSR1, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
#endif

out:
    return err;
}

static inline
int network_congestion(time_t cur)
{
    static time_t last_ts = 0;
    static u64 last_obytes = 0;
    u64 cur_obytes;
    int ret = 0;

    if (cur > last_ts) {
        if (unlikely(!hmo.prof.xnet)) {
            /* xnet in/out bytes info is not avaliable, we randomly congested */
            ret = lib_random(1);
        } else {
            if (!last_ts) {
                last_ts = cur;
                last_obytes = atomic64_read(&hmo.prof.xnet->outbytes);
            }
            if (cur > last_ts) {
                cur_obytes = atomic64_read(&hmo.prof.xnet->outbytes);
                /* bigger than 5MB/s means network congested */
                if ((cur_obytes - last_obytes) / (cur - last_ts) > 
                    (5 << 20)) {
                    ret = 1;
                }
                last_ts = cur;
                last_obytes = cur_obytes;
            }
        }
    }

    return ret;
}

void mds_itimer_default(int signo, siginfo_t *info, void *arg)
{
    u64 cur = time(NULL);
    
    sem_post(&hmo.timer_sem);
    /* Note that, we must check the profiling interval at here, otherwise
     * checking the profiling interval at timer_thread will lost some
     * statistics */
    if (!hmo.timer_thread_stop)
        dump_profiling(cur, &hmo.hp);
    hmo.tick = cur;
    gk_verbose(mds, "Did this signal handler called?\n");

    return;
}

static int __gcd(int m, int n)
{
    int r, temp;
    if (!m && !n)
        return 0;
    else if (!m)
        return n;
    else if (!n)
        return m;

    if (m < n) {
        temp = m;
        m = n;
        n = temp;
    }
    r = m;
    while (r) {
        r = m % n;
        m = n;
        n = r;
    }

    return m;
}

void mds_hb_wrapper(time_t t)
{
    static time_t prev = 0;

    if (!hmo.cb_hb)
        return;
    
    if (hmo.state >= HMO_STATE_RUNNING) {
        if (t < prev + hmo.conf.hb_interval) {
            return;
        }
        prev = t;
        hmo.cb_hb(&hmo);
    }
}

static void *mds_timer_thread_main(void *arg)
{
    sigset_t set;
    time_t cur;
    int v, err;

    gk_debug(mds, "I am running...\n");

    /* first, let us block the SIGALRM */
    sigemptyset(&set);
    sigaddset(&set, SIGALRM);
    pthread_sigmask(SIG_BLOCK, &set, NULL); /* oh, we do not care about the
                                             * errs */
    /* then, we loop for the timer events */
    while (!hmo.timer_thread_stop) {
        err = sem_wait(&hmo.timer_sem);
        if (err == EINTR)
            continue;
        sem_getvalue(&hmo.timer_sem, &v);
        gk_debug(mds, "OK, we receive a SIGALRM event(remain %d).\n", v);
        /* should we work now */
        cur = time(NULL);
        hmo.tick = cur;
        if (hmo.state > HMO_STATE_LAUNCH) {
        }
        /* then, checking profiling */
        dump_profiling(cur, &hmo.hp);
        /* next, checking the heart beat beep */
        mds_hb_wrapper(cur);
        /* FIXME: */
    }

    gk_debug(mds, "Hooo, I am exiting...\n");
    pthread_exit(0);
}

int mds_setup_timers(void)
{
    pthread_attr_t attr;
    struct sigaction ac;
    struct itimerval value, ovalue, pvalue;
    int which = ITIMER_REAL, interval;
    int err = 0, stacksize;

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

    /* init the timer semaphore */
    sem_init(&hmo.timer_sem, 0, 0);

    /* ok, we create the timer thread now */
    err = pthread_create(&hmo.timer_thread, &attr, &mds_timer_thread_main,
                         NULL);
    if (err)
        goto out;
    /* then, we setup the itimers */
    memset(&ac, 0, sizeof(ac));
    sigemptyset(&ac.sa_mask);
    ac.sa_flags = 0;
    ac.sa_sigaction = mds_itimer_default;
    err = sigaction(SIGALRM, &ac, NULL);
    if (err) {
        err = errno;
        goto out;
    }
    err = getitimer(which, &pvalue);
    if (err) {
        err = errno;
        goto out;
    }
    interval = __gcd(hmo.conf.profiling_thread_interval,
                     hmo.conf.hb_interval);
    if (interval) {
        value.it_interval.tv_sec = interval;
        value.it_interval.tv_usec = 0;
        value.it_value.tv_sec = interval;
        value.it_value.tv_usec = 1;
        err = setitimer(which, &value, &ovalue);
        if (err) {
            err = errno;
            goto out;
        }
        gk_debug(mds, "OK, we have created a timer thread to handle hb "
                   "and profiling events every %d second(s).\n", 
                   interval);
    } else {
        gk_debug(mds, "Hoo, there is no need to setup itimers based on the"
                   " configration.\n");
        hmo.timer_thread_stop = 1;
    }
    
out:
    return err;
}

/* we support sub-second timers to promote the triggers
 */
void mds_reset_itimer_us(u64 us)
{
    struct itimerval value, ovalue;
    int err;

    if (us) {
        value.it_interval.tv_sec = 0;
        value.it_interval.tv_usec = us;
        value.it_value.tv_sec = 0;
        value.it_value.tv_usec = us;
        err = setitimer(ITIMER_REAL, &value, &ovalue);
        if (err) {
            goto out;
        }
        gk_info(mds, "OK, we reset the itimer to %ld us.\n",
                  us);
    } else {
        gk_err(mds, "Invalid sub-second timer value.\n");
    }
out:
    return;
}

void mds_reset_itimer(void)
{
    struct itimerval value, ovalue, pvalue;
    int err, interval;

    err = getitimer(ITIMER_REAL, &pvalue);
    if (err) {
        goto out;
    }
    interval = __gcd(hmo.conf.profiling_thread_interval,
                     hmo.conf.hb_interval);
    if (interval) {
        value.it_interval.tv_sec = interval;
        value.it_interval.tv_usec = 0;
        value.it_value.tv_sec = interval;
        value.it_value.tv_usec = 0;
        err = setitimer(ITIMER_REAL, &value, &ovalue);
        if (err) {
            goto out;
        }
        gk_info(mds, "OK, we reset the itimer to %d second(s).\n", 
                  interval);
    }

out:
    return;
}

/* mds_pre_init()
 *
 * setting up the internal configs.
 */
void mds_pre_init()
{
    /* prepare the hmi & hmo */
    memset(&hmi, 0, sizeof(hmi));
    memset(&hmo, 0, sizeof(hmo));
#ifdef GK_DEBUG_LOCK
    lock_table_init();
#endif
    /* setup the state */
    hmo.state = HMO_STATE_INIT;
}

/* make sure the dir exist
 */
int mds_dir_make_exist(char *path)
{
    int err;
    
    err = mkdir(path, 0755);
    if (err) {
        err = -errno;
        if (errno == EEXIST) {
            err = 0;
        } else if (errno == EACCES) {
            gk_err(mds, "Failed to create the dir %s, no permission.\n",
                     path);
        } else {
            gk_err(mds, "mkdir %s failed w/ %d\n", path, errno);
        }
    }
    
    return err;
}

/* mds_verify()
 */
int mds_verify(void)
{
    char path[256];
    int err = 0;
    
    if (!GK_IS_MDS(hmo.site_id))
        goto set_state;
    
    /* check the MDS_HOME */
    err = mds_dir_make_exist(hmo.conf.mds_home);
    if (err) {
        gk_err(mds, "dir %s does not exist %d.\n", 
                 hmo.conf.mds_home, err);
        return -EINVAL;
    }
    /* check the MDS site home directory */
    sprintf(path, "%s/%lx", hmo.conf.mds_home, hmo.site_id);
    err = mds_dir_make_exist(path);
    if (err) {
        gk_err(mds, "dir %s does not exist %d.\n", path, err);
        return -EINVAL;
    }
    
    /* check modify pause and spool usage */
    if (hmo.conf.option & GK_MDS_MEMLIMIT) {
        if (!hmo.xc || (hmo.xc->ops.recv_handler != mds_spool_dispatch)) {
            return -1;
        }
        /*
        if (hmo.conf.memlimit == 0 || hmo.conf.memlimit <
            (sizeof(struct itb) + sizeof(struct ite) * ITB_SIZE))
            return -1;
        */
    }

    /* we are almost done, but we have to check if we need a recovery */
    if (hmo.aux_state & HMO_AUX_STATE_RECOVER) {
    }

set_state:
    /* enter into running mode */
    hmo.state = HMO_STATE_RUNNING;

    return 0;
}

/* mds_config()
 *
 * Get configuration from the execution environment.
 */
int mds_config(void)
{
    char *value;

    if (hmo.state != HMO_STATE_INIT) {
        gk_err(mds, "MDS state is not in launching, please call "
                 "mds_pre_init() firstly!\n");
        return -EINVAL;
    }

    GK_MDS_GET_ENV_strncpy(dcaddr, value, MDS_DCONF_MAX_NAME_LEN);

    GK_MDS_GET_ENV_cpy(mds_home, value);
    GK_MDS_GET_ENV_cpy(kvs_home, value);
    GK_MDS_GET_ENV_cpy(profiling_file, value);
    GK_MDS_GET_ENV_cpy(conf_file, value);
    GK_MDS_GET_ENV_cpy(log_file, value);

    GK_MDS_GET_ENV_atoi(service_threads, value);
    GK_MDS_GET_ENV_atoi(async_threads, value);
    GK_MDS_GET_ENV_atoi(spool_threads, value);
    GK_MDS_GET_ENV_atoi(xnet_resend_to, value);
    GK_MDS_GET_ENV_atoi(async_update_N, value);
    GK_MDS_GET_ENV_atoi(mp_to, value);
    GK_MDS_GET_ENV_atoi(hb_interval, value);
    GK_MDS_GET_ENV_atoi(gto, value);
    GK_MDS_GET_ENV_atoi(loadin_pressure, value);
    GK_MDS_GET_ENV_atoi(stacksize, value);
    GK_MDS_GET_ENV_atoi(prof_plot, value);
    GK_MDS_GET_ENV_atoi(profiling_thread_interval, value);

    GK_MDS_GET_ENV_atoi(nshash_size, value);
    GK_MDS_GET_ENV_atoi(ns_ht_size, value);

    /* default configurations */
    if (!hmo.conf.mds_home) {
        hmo.conf.mds_home = GK_MDS_HOME;
    }
    if (!hmo.conf.kvs_home) {
        hmo.conf.kvs_home = GK_MDS_KVS_HOME;
    }
    
    if (!hmo.conf.profiling_thread_interval)
        hmo.conf.profiling_thread_interval = 5;
    if (!hmo.conf.gto)
        hmo.conf.gto = 1;
    if (!hmo.conf.loadin_pressure)
        hmo.conf.loadin_pressure = 30;

    if (!hmo.conf.nshash_size)
        hmo.conf.nshash_size = MDS_KVS_NSHASH_SIZE;
    if (!hmo.conf.ns_ht_size)
        hmo.conf.ns_ht_size = NS_HASH_SIZE;

    return 0;
}

/* mds_init()
 *
 *@bdepth: bucket depth
 *
 * init the MDS threads' pool
 */
int mds_init(int bdepth)
{
    int err;
    
    /* lib init */
    lib_init();

    /* lzo lib init */
    err = lzo_init();
    if (err != LZO_E_OK) {
        gk_err(mds, "init lzo library failed w/ %d\n", err);
        goto out_lzo;
    }
    
    /* FIXME: decode the cmdline */

    /* FIXME: configations */
    dconf_init();
    /* default configurations */

    /* unset the default spool theads number */
    /* hmo.conf.spool_threads = 8; */
    hmo.conf.mp_to = 60;
    hmo.conf.hb_interval = 60;

    /* get configs from env */
    err = mds_config();
    if (err)
        goto out_config;

    /* Init the signal handlers */
    err = mds_init_signal();
    if (err)
        goto out_signal;

    /* FIXME: setup the timers */
    err = mds_setup_timers();
    if (err)
        goto out_timers;

    /* FIXME: init the kvs subsystem */
    err = kvs_init();
    if (err)
        goto out_kvs;

    /* FIXME: init the xnet subsystem */

    /* FIXME: init the profiling subsystem */

    /* FIXME: init the fault tolerant subsystem */

    /* FIXME: register with the Ring server */
    
    /* FIXME: init the service threads' pool */
    err = mds_spool_create();
    if (err)
        goto out_spool;

    /* FIXME: init the gossip thread */
    /*err = gossip_init();
    if (err)
        goto out_gossip;
    */

    /* FIXME: waiting for the notification from R2 */

    /* FIXME: waiting for the requests from client/mds/mdsl/r2 */

    /* mask the SIGUSR1 signal for main thread */
    {
        sigset_t set;

        sigemptyset(&set);
        sigaddset(&set, SIGUSR1);
        pthread_sigmask(SIG_BLOCK, &set, NULL);
    }

    /* ok to run */
    hmo.state = HMO_STATE_LAUNCH;
    hmo.uptime = time(NULL);

out_spool:
out_kvs:
out_timers:
out_signal:
out_config:
out_lzo:
    return err;
}

void mds_destroy(void)
{
    gk_verbose(mds, "OK, stop it now...\n");

    /* unreg w/ the r2 server */
    if (hmo.state >= HMO_STATE_RUNNING && hmo.cb_exit) {
        hmo.cb_exit(&hmo);
    }

    /* stop the timer thread */
    hmo.timer_thread_stop = 1;
    /* Bug fix: hmo.timer_thread is type pthread_t, this value is a ID which
     * can be zero. Thus, we can not rely on ZERO detection for invalid
     * values. */
    pthread_join(hmo.timer_thread, NULL);

    sem_destroy(&hmo.timer_sem);

    /* stop the gossip thread */
    //gossip_destroy();
    
    /* destroy the dconf */
    dconf_destroy();

    /* destroy the service thread pool */
    mds_spool_destroy();

    /* close the files */
    if (hmo.conf.pf_file)
        fclose(hmo.conf.pf_file);

    /* destroy the kvs subsystem */
    kvs_destroy();
}

u64 mds_select_ring(struct gk_mds_object *hmo)
{
    if (hmo->ring_site)
        return hmo->ring_site;
    else
        return GK_RING(0);
}

void mds_set_ring(u64 site_id)
{
    hmo.ring_site = site_id;
}
