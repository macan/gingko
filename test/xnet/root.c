/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-21 21:54:27 macan>
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
#include "root.h"

#ifdef UNIT_TEST
#define TYPE_MDS        0
#define TYPE_CLIENT     1
#define TYPE_ROOT       4

char *ipaddr[] = {
    "127.0.0.1",              /* root */
};

short port[] = {
    8710, /* root */
};

#define GK_TYPE(type, idx) ({                   \
            u64 __sid = -1UL;                   \
            switch (type){                      \
            case TYPE_MDS:                      \
                __sid = GK_MDS(idx);            \
                break;                          \
            case TYPE_CLIENT:                   \
                __sid = GK_CLIENT(idx);         \
                break;                          \
            case TYPE_ROOT:                     \
                __sid = GK_ROOT(idx);           \
                break;                          \
            default:;                           \
            }                                   \
            __sid;                              \
        })

static inline
u64 GK_TYPE_SEL(int type, int id)
{
    u64 site_id = -1UL;

    switch (type) {
    case TYPE_MDS:
        site_id = GK_MDS(id);
        break;
    case TYPE_CLIENT:
        site_id = GK_CLIENT(id);
        break;
    case TYPE_ROOT:
        site_id = GK_RING(id);
        break;
    default:;
    }

    return site_id;
}

int msg_wait()
{
    while (1) {
        xnet_wait_any(hro.xc);
    }
    return 0;
}

int main(int argc, char *argv[])
{
    struct xnet_type_ops ops = {
        .buf_alloc = NULL,
        .buf_free = NULL,
        .recv_handler = root_spool_dispatch,
        .dispatcher = root_dispatch,
    };
    int err = 0, i;
    int self, sport = -1, mode, do_create;
    char profiling_fname[256], *log_home;
    char *value;
    char *conf_file;
    struct sockaddr_in sin = {
        .sin_family = AF_INET,
    };
    int nr = 10000;
    struct conf_site cs[nr];

    gk_info(xnet, "R2 Unit Testing ...\n");

    if (argc < 2) {
        gk_err(xnet, "Self ID or config file is not provided.\n");
        err = EINVAL;
        goto out;
    } else {
        self = atoi(argv[1]);
        gk_info(xnet, "Self type+ID is R2:%d.\n", self);
        if (argc > 2) {
            conf_file = argv[2];
            if (argc == 4) {
                sport = atoi(argv[3]);
            }
        } else {
            conf_file = NULL;
        }
    }

    value = getenv("mode");
    if (value) {
        mode = atoi(value);
    } else 
        mode = 0;

    value = getenv("create");
    if (value) {
        do_create = atoi(value);
    } else 
        do_create = 0;

    value = getenv("LOG_DIR");
    if (value) {
        log_home = strdup(value);
    }
    else
        log_home = NULL;

    st_init();
    root_pre_init();

    /* setup the profiling file */
    if (!log_home)
        log_home = ".";
    
    memset(profiling_fname, 0, sizeof(profiling_fname));
    sprintf(profiling_fname, "%s/CP-BACK-root.%d", log_home, self);
    hro.conf.profiling_file = strdup(profiling_fname);
    hro.conf.prof_plot = ROOT_PROF_PLOT;
    
    err = root_init();
    if (err) {
        gk_err(xnet, "root_init() failed w/ %d\n", err);
        goto out;
    }

    /* init misc configurations */
    hro.prof.xnet = &g_xnet_prof;

    if (sport == -1)
        sport = port[0];
    self = GK_ROOT(self);

    hro.xc = xnet_register_type(0, sport, self, &ops);
    if (IS_ERR(hro.xc)) {
        err = PTR_ERR(hro.xc);
        goto out;
    }

    hro.site_id = self;
    root_verify();

    /* we should setup the global address table and then export it as the
     * st_table */

    SET_TRACING_FLAG(xnet, GK_DEBUG);
    SET_TRACING_FLAG(root, GK_DEBUG);

    {
        struct addr_entry *ae;
        
        /* setup a file system id 0 */
        err = addr_mgr_lookup_create(&hro.addr, 0UL, &ae);
        if (err > 0) {
            gk_info(xnet, "Create addr table for fsid %ld\n", 0UL);
        } else if (err < 0) {
            gk_err(xnet, "addr_mgr_lookup_create fsid 0 failed w/ %d\n",
                     err);
            goto out;
        }

        if (mode == 0) {
            sin.sin_port = htons(port[0]);
            inet_aton(ipaddr[0], &sin.sin_addr);
                    
            err = addr_mgr_update_one(ae, 
                                      GK_SITE_PROTOCOL_TCP |
                                      GK_SITE_ADD,
                                      GK_TYPE(TYPE_ROOT, 0),
                                      &sin);
            if (err) {
                gk_err(xnet, "addr mgr update entry %lx failed w/"
                       " %d\n", GK_TYPE(TYPE_ROOT, 0), err);
                goto out;
            }
        } else if (conf_file) {
            err = conf_parse(conf_file, cs, &nr);
            if (err) {
                gk_err(xnet, "conf_parse failed w/ %d\n", err);
                goto out;
            }
            for (i = 0; i < nr; i++) {
                sin.sin_port = htons(cs[i].port);
                inet_aton(cs[i].node, &sin.sin_addr);

                err = addr_mgr_update_one(ae,
                                          GK_SITE_PROTOCOL_TCP |
                                          GK_SITE_ADD,
                                          conf_site_id(cs[i].type, cs[i].id),
                                          &sin);
                if (err) {
                    gk_err(xnet, "addr_mgr_update entry %lx failed w/ "
                             " %d\n", conf_site_id(cs[i].type, cs[i].id), err);
                    goto out;
                }
            }
        }
        
        /* export the addr mgr to st_table */
        {
            void *data;
            int len;
            
            err = addr_mgr_compact(ae, &data, &len);
            if (err) {
                gk_err(xnet, "compact addr mgr faild w/ %d\n", err);
                goto out;
            }
            
            err = hst_to_xsst(data, len);
            if (err) {
                gk_err(xnet, "hst to xsst failed w/ %d\n", err);
                goto out;
            }
            xfree(data);
        }
    }

    /* next, we setup the root entry for fsid == 0 */
    {
        struct root_entry *re, __attribute__((unused))*res;

        err = root_mgr_lookup_create(&hro.root, 0, &re);
        if (err > 0) {
            /* create a new root entry, and read in the content from the
             * disk  */
            gk_info(xnet, "Read in the fs %ld: \n", 0UL);
        } else if (err == -ENOENT) {
            /* create a new root entry and insert it */
            re = root_mgr_alloc_re();
            if (!re) {
                gk_err(xnet, "root mgr alloc re failed\n");
                err = -ENOMEM;
                goto out;
            }
            re->fsid = 0;

            if (do_create) {
                /* change the root_salt to a random value */
                res = root_mgr_insert(&hro.root, re);
                if (IS_ERR(res)) {
                    gk_err(xnet, "insert root entry faild w/ %ld\n",
                             PTR_ERR(res));
                    err = PTR_ERR(res);
                    goto out;
                }
                gk_info(root, "We just do create a new fs %ld\n", re->fsid);
            } else {
                /* do not create a root entry, we just read from the disk
                 * now */
                ;
            }
        } else if (err < 0) {
            gk_err(xnet, "lookup create root 0 failed w/ %d\n", err);
        }
    }

    gk_info(xnet, "R2 is UP for serving requests now.\n");

    msg_wait();

    root_destroy();
    xnet_unregister_type(hro.xc);
out:
    return err;
}
#endif
