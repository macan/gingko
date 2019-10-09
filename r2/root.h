/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-21 21:58:18 macan>
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

#ifndef __ROOT_H__
#define __ROOT_H__

#include "gk.h"
#include "prof.h"
#include "lib.h"
#include "rprof.h"
#include "mgr.h"
#include "xnet.h"
#include "profile.h"

struct root_conf
{
    /* section for dynamic configuration */
    char dcaddr[ROOT_DCONF_MAX_NAME_LEN];
    int dcfd, dcepfd;
    pthread_t dcpt;

    /* section for file name */
    char *root_home;
    char *profiling_file;
    char *conf_file;
    char *log_file;
#ifndef GK_ROOT_HOME
#define GK_ROOT_HOME          "/tmp/gk"
#endif
#define GK_ROOT_STORE         "root_store"
#define GK_SITE_STORE         "site_store"
#define GK_ADDR_STORE         "addr_store"
    char *root_store;
    char *site_store;
    char *addr_store;

    /* section for file fd */
    FILE *pf_file, *cf_file, *lf_file;
    int root_store_fd;
    int site_store_fd, addr_store_fd;

    /* # of threads */
    /* Note: # of profiling thread is always ONE */
    int service_threads;        /* # of service threads, pass this value to
                                     lnet */

    /* misc configs */
    u32 site_mgr_htsize;        /* site mgr hash table size */
    u32 root_mgr_htsize;        /* root mgr hash table size */
    u32 addr_mgr_htsize;        /* addr mgr hash table size */
    u32 hb_interval;            /* interval to check the site entry
                                 * heartbeat */
    u32 sync_interval;          /* interval to do self sync */
    u32 profile_interval;       /* interval to do profile */
    u32 log_print_interval;     /* interval to dump sth */

#define ROOT_PROF_NONE          0x00
#define ROOT_PROF_PLOT          0x01
    u8 prof_plot;

    /* conf */
#define GK_ROOT_MEMONLY       0x01 /* memory only service */
    u64 option;
};

struct gk_root_object
{
#define HRO_STATE_INIT          0x00
#define HRO_STATE_LAUNCH        0x01
#define HRO_STATE_RUNNING       0x02
#define HRO_STATE_PAUSE         0x03
#define HRO_STATE_RDONLY        0x04
    u32 state;                  /* this site id */
    u64 site_id;
    struct xnet_context *xc;

    /* Register pool of client, mds, mdsl and Other ROOT servers. 
     *
     * Note that: now we just support ONE root server, next step we can
     * support BFT root service. */
    struct site_mgr site;

    /* root service manager */
    struct root_mgr root;

    /* address service manager */
    struct addr_mgr addr;

    struct root_conf conf;
    struct root_prof prof;

    sem_t timer_sem;

    /* the following region is used for threads */
    pthread_t *spool_thread;    /* array of service threads */
    pthread_t timer_thread;

    /* profile section */
    struct gk_profile_ex hp_mds, hp_bp, hp_client;

    /* uptime */
    time_t uptime;
    
    u8 spool_thread_stop;       /* running flag for service thread */
    u8 timer_thread_stop;       /* running flag for timer thread */
};

extern struct gk_root_object hro;

#ifdef GK_TRACING
extern u32 gk_root_tracing_flags;
#endif

struct gk_sys_info
{
#define GK_SYSINFO_NOOP               0
#define GK_SYSINFO_SITE               1
#define GK_SYSINFO_MDS                2
#define GK_SYSINFO_MDSL               3
#define GK_SYSINFO_OSD                4
#define GK_SYSINFO_ROOT               5

#define GK_SYSINFO_ALL                100
    u32 cmd;
    u32 arg0;
#define GK_SYSINFO_SITE_ALL           0
#define GK_SYSINFO_SITE_MDS           1
#define GK_SYSINFO_SITE_MDSL          2
#define GK_SYSINFO_SITE_CLIENT        3
#define GK_SYSINFO_SITE_BP            4
#define GK_SYSINFO_SITE_R2            5
#define GK_SYSINFO_SITE_OSD           6

#define GK_SYSINFO_SITE_MASK          0x0f

#define GK_SYSINFO_MDS_RATE           0
#define GK_SYSINFO_MDS_RAW            1

#define GK_SYSINFO_MDSL_RATE          0
#define GK_SYSINFO_MDSL_RAW           1

#define GK_SYSINFO_OSD_RATE           0
#define GK_SYSINFO_OSD_RAW            1
};

/* API Region */
void root_pre_init(void);
int root_verify(void);
int root_config(void);
int root_init(void);
void root_destroy(void);

int root_spool_create(void);
void root_spool_destroy(void);
int root_spool_dispatch(struct xnet_msg *msg);

int root_dispatch(struct xnet_msg *msg);

int root_do_reg(struct xnet_msg *);
int root_do_unreg(struct xnet_msg *);
int root_do_update(struct xnet_msg *);
int root_do_hb(struct xnet_msg *);
int root_do_ftreport(struct xnet_msg *);
int root_do_shutdown(struct xnet_msg *);
int root_do_profile(struct xnet_msg *);
int root_do_info(struct xnet_msg *);

int bparse_hxi(void *, union gk_x_info **);
int bparse_root(void *, struct root_tx **);
int bparse_addr(void *, struct gk_site_tx **);

/* cli.c */
int cli_do_addsite(struct sockaddr_in *, u64, u64);
int cli_do_rmvsite(struct sockaddr_in *, u64, u64);
int root_info_site(u64 arg, void **buf);

/* profile.c */
int root_profile_update_mds(struct gk_profile *,
                            struct xnet_msg *);
int root_profile_update_mdsl(struct gk_profile *,
                             struct xnet_msg *);
int root_profile_update_bp(struct gk_profile *,
                           struct xnet_msg *);
int root_profile_update_client(struct gk_profile *,
                               struct xnet_msg *);
int root_profile_update_osd(struct gk_profile *,
                            struct xnet_msg *);
int root_setup_profile(void);
void root_profile_flush(time_t);
int root_info_mds(u64, void **);
int root_info_mdsl(u64, void **);
int root_info_root(u64, void **);

/* x2r.c */
int root_do_objrep(struct xnet_msg *);
int root_do_query_obj(struct xnet_msg *);
int root_do_getasite(struct xnet_msg *);

#endif
