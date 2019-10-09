/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-30 15:11:38 macan>
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

#ifndef __MDS_H__
#define __MDS_H__

#include "gk.h"
#include "xnet.h"
#include "prof.h"
#include "lib.h"
#include "mds_config.h"
#include "profile.h"
#include "kvs.h"

#define GK_MDS_HOME "/tmp/gk"
#define GK_MDS_KVS_HOME "/tmp/gk/kvs"

struct mds_conf 
{
    /* section for dynamic configuration */
    char dcaddr[MDS_DCONF_MAX_NAME_LEN + 1];
    int dcfd, dcepfd;
    pthread_t dcpt;

    /* section for file name */
    char *mds_home;
    char *kvs_home;
    char *profiling_file;
    char *conf_file;
    char *log_file;

    /* section for file fd */
    FILE *pf_file, *cf_file, *lf_file;

    /* # of threads */
    /* NOTE: # of profiling thread is always ONE */
    int service_threads;        /* # of service threads, pass this value to
                                     lnet serve-in threads' pool
                                     initialization */
    int async_threads;          /* # of async threads */
    int spool_threads;          /* # of service threads */

    /* misc configs */
    int xnet_resend_to;         /* xnet resend timeout */
    int async_update_N;         /* default # of processing request */
    int mp_to;                  /* timeout of modify pause */
    int hb_interval;            /* heart beat interval */
    int gto;                    /* gossip timeout */
    int loadin_pressure;        /* loadin memory pressure */
    int stacksize;              /* pthread stack size */
#define MDS_PROF_NONE           0x00
#define MDS_PROF_PLOT           0x01
#define MDS_PROF_HUMAN          0x02
#define MDS_PROF_R2             0x03
    u8 prof_plot;               /* do we dump profilings for gnuplot? */

    int nshash_size;            /* namespace mgr hash table size */
    int ns_ht_size;             /* namespace self's hash table size */

    /* intervals */
    int profiling_thread_interval;

    /* conf */
#define GK_MDS_MEMONLY        0x08 /* memory only service */
#define GK_MDS_MEMLIMIT       0x10 /* limit the memory usage */
#define GK_MDS_MDZIP          0x80 /* compress the metadata */
    u64 option;
};

struct gk_mds_object
{
    u64 site_id;                /* this site */
    struct xnet_context *xc;    /* the xnet context */

    struct mem_ops *mops;         /* memory management operations */
    struct mds_prof prof;
    struct mds_conf conf;

#define HMO_STATE_INIT          0x00 /* do not accept request and wait */
#define HMO_STATE_LAUNCH        0x01 /* do not accept reqeusts */
#define HMO_STATE_RUNNING       0x02 /* accept all requests */
#define HMO_STATE_PAUSE         0x03 /* pause client/amc requests */
#define HMO_STATE_RDONLY        0x04 /* err reply modify requests */
#define HMO_STATE_OFFLINE       0x05 /* accept all requests, but return
                                      * EOFFLINE (exclude R2) */
    u32 state;
#define HMO_AUX_STATE_NORMAL    0x00 /* normal aux state, no logger */
#define HMO_AUX_STATE_LOGGER    0x01 /* only local logger */
#define HMO_AUX_STATE_HA        0x02 /* enter in HA state */
#define HMO_AUX_STATE_RECOVER   0x04 /* recovery state after launch */
    u32 aux_state;

    u64 ring_site;

    /* the following region is used for threads */
    time_t mp_ts;               /* begin time of modify pause */
    time_t uptime;              /* startup time */
    time_t tick;                /* current time */

    sem_t timer_sem;            /* for timer thread wakeup */
    sem_t async_sem;            /* for async thread wakeup */
    sem_t modify_pause_sem;     /* for pausing the modifing request
                                 * handling */
    
    pthread_t timer_thread;
    pthread_t *async_thread;    /* array of async threads */
    pthread_t *spool_thread;    /* array of service threads */
    pthread_t scrub_thread;
    pthread_t gossip_thread;

    pthread_key_t lzo_workmem;  /* for lzo zip use */

    u32 timer_thread_stop:1;    /* running flag for timer thread */
    u32 commit_thread_stop:1;   /* running flag for commit thread */
    u32 async_thread_stop:1;    /* running flag for async thread */
    u32 dconf_thread_stop:1;    /* running flag for dconf thread */
    u32 unlink_thread_stop:1;   /* running flag for unlink thread */
    u32 spool_thread_stop:1;    /* running flag for service thread */
    u32 scrub_thread_stop:1;    /* running flag for scrub thread */
    u32 gossip_thread_stop:1;   /* running flag for gossip thread */

    u8 spool_modify_pause:1;    /* pause the modification */
    u8 scrub_running:1;         /* is scrub thread running */
    u8 reqin_drop:1;            /* drop the incoming client requests */
    u8 reqin_pause:1;           /* pause the requesst handling for client and
                                 * amc */

    int scrub_op;               /* scrub operation */
    atomic_t lease_seqno;       /* lease seqno */
    
    /* mds profiling array */
    struct gk_profile hp;

    /* callback functions */
    void (*cb_exit)(void *);
    void (*cb_hb)(void *);
    void (*cb_addr_table_update)(void *);
    void (*cb_latency)(void *);

    u64 fsid;
};

extern struct gk_mds_info hmi;
extern struct gk_mds_object hmo;
extern u32 gk_mds_tracing_flags;
extern pthread_key_t spool_key;

void mds_reset_tracing_flags(u64);

struct dconf_req
{
#define DCONF_ECHO_CONF         0
#define DCONF_SET_TXG_INTV      1
#define DCONF_SET_PROF_INTV     2
#define DCONF_SET_UNLINK_INTV   3
#define DCONF_SET_MDS_FLAG      4
#define DCONF_SET_XNET_FLAG     5
#define DCONF_GET_LATENCY       6
    u64 cmd;
    u64 arg0;
};

/* this is the mds forward request header, we should save the route list
 * here. */
struct mds_fwd
{
    int len;
#define MDS_FWD_MAX     31
    u32 route[0];
};

/* APIs */
/* for mds.c */
void mds_pre_init(void);
int mds_init(int bdepth);
int mds_verify(void);
void mds_destroy(void);
void mds_reset_itimer_us(u64);
void mds_reset_itimer(void);
static inline
void mds_gossip_faster(void)
{
    if (hmo.conf.gto > 1)
        hmo.conf.gto--;
}
static inline
void mds_gossip_slower(void)
{
    if (hmo.conf.gto < 15)
        hmo.conf.gto++;
}
int mds_dir_make_exist(char *);
u64 mds_select_ring(struct gk_mds_object *);
void mds_set_ring(u64);

static inline
void mds_set_fsid(struct gk_mds_object *hmo, u64 fsid)
{
    hmo->fsid = fsid;
}

/* for fe.c */
#define MAX_RELAY_FWD    (0x1000)
int mds_do_forward(struct xnet_msg *msg, u64 site_id);
int mds_fe_dispatch(struct xnet_msg *msg);
int mds_pause(struct xnet_msg *);
int mds_resume(struct xnet_msg *);
int mds_ring_update(struct xnet_msg *);
int mds_addr_table_update(struct xnet_msg *msg);

/* for dispatch.c */
int mds_client_dispatch(struct xnet_msg *msg);
int mds_mds_dispatch(struct xnet_msg *msg);
int mds_mdsl_dispatch(struct xnet_msg *msg);
int mds_ring_dispatch(struct xnet_msg *msg);
int mds_root_dispatch(struct xnet_msg *msg);
int mds_amc_dispatch(struct xnet_msg *msg);

/* for prof.c */
void dump_profiling(time_t, struct gk_profile *hp);

/* for conf.c */
int dconf_init(void);
void dconf_destroy(void);

/* gossip.c */
int gossip_init(void);
void gossip_destroy(void);

/* latency.c */
void mds_cb_latency(void *);

/* APIs */
/* for spool.c */
int mds_spool_create(void);
void mds_spool_destroy(void);
int mds_spool_dispatch(struct xnet_msg *);
void mds_spool_redispatch(struct xnet_msg *, int sempost);
int mds_spool_modify_pause(struct xnet_msg *);
void mds_spool_mp_check(time_t);
void mds_spool_provoke(void);

/* cli.c */
int mds_do_reg(struct xnet_msg *);
int mds_do_put(struct xnet_msg *);
int mds_do_get(struct xnet_msg *);

#endif
