/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-22 15:42:32 macan>
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

#include "mgr.h"
#include "root.h"

int site_mgr_init(struct site_mgr *sm)
{
    int err = 0, i;
    
    if (!hro.conf.site_mgr_htsize) {
        /* default to ...SITE_MGR_HTSIZE */
        hro.conf.site_mgr_htsize = GK_ROOT_SITE_MGR_HTSIZE;
    }

    sm->sht = xzalloc(hro.conf.site_mgr_htsize * sizeof(struct regular_hash));
    if (!sm->sht) {
        gk_err(root, "xzalloc() site mgr hash table failed.\n");
        err = -ENOMEM;
        goto out;
    }

    /* init the hash table */
    for (i = 0; i < hro.conf.site_mgr_htsize; i++) {
        INIT_HLIST_HEAD(&(sm->sht + i)->h);
        xlock_init(&(sm->sht + i)->lock);
    }

    /* at last, we just open the site entry file */
    ASSERT(hro.conf.site_store, root);
    err = open(hro.conf.site_store, O_CREAT | O_RDWR | O_SYNC,
               S_IRUSR | S_IWUSR);
    if (err < 0) {
        gk_err(root, "open site store %s failed w/ %s\n",
                 hro.conf.site_store, strerror(errno));
        err = -errno;
        goto out;
    }
    gk_info(root, "Open site store %s success.\n",
              hro.conf.site_store);
    hro.conf.site_store_fd = err;
    err = 0;

out:
    return err;
}

void site_mgr_destroy(struct site_mgr *sm)
{
    if (sm->sht) {
        xfree(sm->sht);
    }
    if (hro.conf.site_store_fd)
        close(hro.conf.site_store_fd);
}

struct site_entry *site_mgr_alloc_se()
{
    struct site_entry *se;

    se = xzalloc(sizeof(*se));
    if (se) {
        INIT_HLIST_NODE(&se->hlist);
        xlock_init(&se->lock);
    }

    return se;
}

void site_mgr_free_se(struct site_entry *se)
{
    xfree(se);
}

struct site_entry *site_mgr_lookup(struct site_mgr *sm, u64 site_id)
{
    struct site_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_site_mgr(site_id, GK_ROOT_SITE_MGR_SALT) % 
        hro.conf.site_mgr_htsize;
    rh = sm->sht + idx;
    
    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (site_id == pos->site_id) {
            /* ok, we find this entry */
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    if (found) {
        return pos;
    } else {
        return ERR_PTR(-ENOENT);
    }
}

struct site_args
{
    u64 site_id;
    u32 state;
};

int site_mgr_traverse(struct site_mgr *sm, site_mgr_trav_callback_t callback, 
                      void *args)
{
    struct site_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    struct site_args sa;
    int err = 0, i;

    for (i = 0; i < hro.conf.site_mgr_htsize; i++) {
        rh = sm->sht + i;
        xlock_lock(&rh->lock);
        hlist_for_each_entry(pos, n, &rh->h, hlist) {
            gk_warning(root, "Hit site %lx\n", pos->site_id);
            if (callback) {
                if (!args) {
                    sa.site_id = pos->site_id;
                    sa.state = pos->state;
                    /* we pass a site_args structure to user function */
                    callback((void *)&sa);
                } else {
                    ((struct site_args *)args)->site_id = pos->site_id;
                    ((struct site_args *)args)->state = pos->state;
                    
                    callback(args);
                }
            }
        }
        xlock_unlock(&rh->lock);
    }

    return err;
}

struct site_entry *site_mgr_insert(struct site_mgr *sm, struct site_entry *se)
{
    struct site_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_site_mgr(se->site_id, GK_ROOT_SITE_MGR_SALT) %
        hro.conf.site_mgr_htsize;
    rh = sm->sht + idx;
    
    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (se->site_id == pos->site_id) {
            /* ok, we find a conflict entry */
            found = 1;
            break;
        }
    }
    if (!found) {
        hlist_add_head(&se->hlist, &(sm->sht + idx)->h);
        pos = se;
    }
    xlock_unlock(&rh->lock);

    return pos;
}

/* site_mgr_lookup_create()
 *
 * This function lookup or create a site entry.
 *
 * Return Value: <0 error; ==0 ok(found); >0 new
 */
int site_mgr_lookup_create(struct site_mgr *sm, u64 site_id, 
                           struct site_entry **ose)
{
    struct site_entry *se;
    int err = 0;

    if (!ose) {
        err = -EINVAL;
        goto out;
    }

    se = site_mgr_lookup(sm, site_id);
    if (IS_ERR(se)) {
        if (ERR_PTR(-ENOENT) == se) {
            /* we should create a new site entry now */
            se = site_mgr_alloc_se();
            if (!se) {
                gk_err(root, "site_mgr_alloc_se() failed w/ ENOMEM.\n");
                err = -ENOMEM;
                goto out;
            }
            se->site_id = site_id;
            /* try to insert to the site mgr */
            *ose = site_mgr_insert(sm, se);
            if (IS_ERR(*ose)) {
                gk_err(root, "site_mgr_insert() failed w/ %ld\n",
                         PTR_ERR(*ose));
                err = PTR_ERR(*ose);
                site_mgr_free_se(se);
                goto out;
            }
            if (se != *ose) {
                gk_err(root, "Someone insert site %lx prior us, self free\n",
                         site_id);
                site_mgr_free_se(se);
            }
            err = 1;
        } else {
            /* error here */
            err = PTR_ERR(se);
            gk_err(root, "site_mgr_lookup() failed w/ %d\n", err);
            goto out;
        }
    } else {
        /* set ose to the lookuped entry */
        *ose = se;
    }
    
out:
    return err;
}

int addr_mgr_init(struct addr_mgr *am)
{
    int err = 0, i;

    if (!hro.conf.addr_mgr_htsize) {
        /* default to ...ADDR_MGR_HTSIZE */
        hro.conf.addr_mgr_htsize = GK_ROOT_ADDR_MGR_HTSIZE;
    }

    am->rht = xzalloc(hro.conf.addr_mgr_htsize * sizeof(struct regular_hash));
    if (!am->rht) {
        gk_err(root, "xzalloc() addr mgr hash table failed.\n");
        err = -ENOMEM;
        goto out;
    }
    
    xrwlock_init(&am->rwlock);

    /* init the hash table */
    for (i = 0; i < hro.conf.addr_mgr_htsize; i++) {
        INIT_HLIST_HEAD(&(am->rht + i)->h);
        xlock_init(&(am->rht + i)->lock);
    }

    /* at last, we just open the root entry file */
    ASSERT(hro.conf.addr_store, root);
    err = open(hro.conf.addr_store, O_CREAT | O_RDWR | O_SYNC,
               S_IRUSR | S_IWUSR);
    if (err < 0) {
        gk_err(root, "open addr store %s failed w/ %s\n",
                 hro.conf.addr_store, strerror(errno));
        err = -errno;
        goto out;
    }
    gk_info(root, "Open addr store %s success.\n",
              hro.conf.addr_store);
    hro.conf.addr_store_fd = err;
    err = 0;

out:
    return err;
}

void addr_mgr_destroy(struct addr_mgr *am)
{
    /* should we do something */
    xfree(am->rht);
    xrwlock_destroy(&am->rwlock);
    if (hro.conf.addr_store_fd)
        close(hro.conf.addr_store_fd);
}

struct addr_entry *addr_mgr_alloc_ae()
{
    struct addr_entry *ae;

    ae = xzalloc(sizeof(*ae));
    if (ae) {
        INIT_HLIST_NODE(&ae->hlist);
        xrwlock_init(&ae->rwlock);
        ae->used_addr = 0;
        ae->active_site = 0;
    }

    return ae;
}

void addr_mgr_free_ae(struct addr_entry *ae)
{
    xfree(ae);
}

/* addr_mgr_lookup() */
struct addr_entry *addr_mgr_lookup(struct addr_mgr *am, u64 fsid)
{
    struct addr_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_root_mgr(fsid, GK_ROOT_ADDR_MGR_SALT) %
        hro.conf.addr_mgr_htsize;
    rh = am->rht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (fsid == pos->fsid) {
            /* ok, we find the entry */
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    if (found) {
        return pos;
    } else {
        return ERR_PTR(-ENOENT);
    }
}

struct addr_entry *addr_mgr_insert(struct addr_mgr *am, struct addr_entry *ae)
{
    struct addr_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_root_mgr(ae->fsid, GK_ROOT_ADDR_MGR_SALT) %
        hro.conf.addr_mgr_htsize;
    rh = am->rht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (ae->fsid == pos->fsid) {
            /* ok, we find a conflict entry */
            found = 1;
            break;
        }
    }
    if (!found) {
        xrwlock_wlock(&am->rwlock);
        hlist_add_head(&ae->hlist, &(am->rht + idx)->h);
        xrwlock_wunlock(&am->rwlock);
        pos = ae;
    }
    xlock_unlock(&rh->lock);

    return pos;
}

void addr_mgr_remove(struct addr_mgr *am, u64 fsid)
{
    struct addr_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_root_mgr(fsid, GK_ROOT_ADDR_MGR_SALT) %
        hro.conf.addr_mgr_htsize;
    rh = am->rht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (fsid == pos->fsid) {
            /* ok, we find the entry here */
            found = 1;
            break;
        }
    }
    if (found) {
        /* remove it */
        xrwlock_wlock(&am->rwlock);
        hlist_del_init(&pos->hlist);
        xrwlock_wunlock(&am->rwlock);
    } else {
        gk_err(root, "Try to remove fsid %ld failed, not found.\n",
                 fsid);
    }
    xlock_unlock(&rh->lock);
}

int addr_mgr_lookup_create(struct addr_mgr *am, u64 fsid,
                           struct addr_entry **oae)
{
    struct addr_entry *ae;
    int err = 0;

    if (!oae) {
        err = -EINVAL;
        goto out;
    }

    ae = addr_mgr_lookup(am, fsid);
    if (IS_ERR(ae)) {
        if (ae == ERR_PTR(-ENOENT)) {
            /* we should create a new addr entry now */
            ae = addr_mgr_alloc_ae();
            if (!ae) {
                gk_err(root, "addr_mgr_alloc_re() failed w/ ENOMEM.\n");
                err = -ENOMEM;
                goto out;
            }
            gk_err(root, "alloc ae %p\n", ae);
            /* we just create an empty addr entry */
            ae->fsid = fsid;
            /* try to insert to the addr mgr */
            *oae = addr_mgr_insert(am, ae);
            if (IS_ERR(*oae)) {
                gk_err(root, "addr_mgr_insert() failed w/ %ld\n",
                         PTR_ERR(*oae));
                err = PTR_ERR(*oae);
                addr_mgr_free_ae(ae);
            }
            if (ae != *oae) {
                gk_err(root, "Someone insert addr %ld prior us, self free\n",
                         fsid);
                addr_mgr_free_ae(ae);
            }
            err = 1;
        } else {
            /* error here */
            err = PTR_ERR(ae);
            gk_err(root, "addr_mgr_lookup() failed w/ %d\n", err);
            goto out;
        }
    } else {
        /* set oae to the lookuped entry */
        *oae = ae;
    }

out:
    return err;
}

/* addr_mgr_update_one()
 *
 * @flag: GK_SITE_REPLACE/ADD/DEL | GK_SITE_PROTOCOL_TCP
 */
int addr_mgr_update_one(struct addr_entry *ae, u32 flag, u64 site_id,
                        void *addr)
{
    int err = 0;
    
    /* sanity checking */
    if (!ae || site_id < 0 || site_id >= (1 << 20))
        return -EINVAL;

    if (flag & GK_SITE_PROTOCOL_TCP) {
        struct gk_addr *hta, *pos, *n;
        struct sockaddr_in *si = (struct sockaddr_in *)addr;

        hta = xzalloc(sizeof(*hta));
        if (!hta) {
            gk_err(root, "xzalloc() gk_tcp_addr failed.\n");
            err = -ENOMEM;
            goto out;
        }
        hta->flag = GK_SITE_PROTOCOL_TCP;
        INIT_LIST_HEAD(&hta->list);
        *((struct sockaddr_in *)&hta->sock.sa) = *(si);

        /* next, we should do the OP on the site table */
        if (flag & GK_SITE_REPLACE) {
            xrwlock_wlock(&ae->rwlock);
            if (ae->xs[site_id]) {
                /* the gk_site exists, we just free the table list */
                list_for_each_entry_safe(pos, n, &ae->xs[site_id]->addr, 
                                         list) {
                    list_del(&pos->list);
                    ae->xs[site_id]->nr--;
                    ae->used_addr--;
                    xfree(pos);
                }
                INIT_LIST_HEAD(&ae->xs[site_id]->addr);
            } else {
                /* add a new gk_site to the table */
                struct gk_site *hs;
                
                hs = xzalloc(sizeof(*hs));
                if (!hs) {
                    gk_err(root, "xzalloc() gk_site failed.\n");
                    err = -ENOMEM;
                    goto out_unlock_replace;
                }
                INIT_LIST_HEAD(&hs->addr);
                /* setup the gk_site to site table */
                ae->xs[site_id] = hs;
                ae->active_site++;
            }
            
            /* add the new addr to the list */
            list_add_tail(&hta->list, &ae->xs[site_id]->addr);
            ae->xs[site_id]->nr++;
            ae->used_addr++;
        out_unlock_replace:
            xrwlock_wunlock(&ae->rwlock);
        } else if (flag & GK_SITE_ADD) {
            xrwlock_wlock(&ae->rwlock);
            if (!ae->xs[site_id]) {
                /* add a new gk_site to the table */
                struct gk_site *hs;

                hs =xzalloc(sizeof(*hs));
                if (!hs) {
                    gk_err(root, "xzalloc() gk_site failed.\n");
                    err = -ENOMEM;
                    goto out_unlock_add;
                }
                INIT_LIST_HEAD(&hs->addr);
                /* setup the gk_site to site table */
                ae->xs[site_id] = hs;
                ae->active_site++;
            }
            /* add the new addr to the list */
            list_add_tail(&hta->list, &ae->xs[site_id]->addr);
            ae->xs[site_id]->nr++;
            ae->used_addr++;
        out_unlock_add:
            xrwlock_wunlock(&ae->rwlock);
        } else if (flag & GK_SITE_DEL) {
            err = -ENOTEXIST;
            xrwlock_wlock(&ae->rwlock);
            if (ae->xs[site_id]) {
                /* iterate on the table to find the entry */
                list_for_each_entry_safe(pos, n, &ae->xs[site_id]->addr, list) {
                    if ((si->sin_port == 
                         ((struct sockaddr_in *)&hta->sock.sa)->sin_port) ||
                        (si->sin_addr.s_addr ==
                         ((struct sockaddr_in *)&hta->sock.sa)->sin_addr.s_addr) ||
                        (si->sin_family ==
                         ((struct sockaddr_in *)&hta->sock.sa)->sin_family)) {
                        list_del(&pos->list);
                        xfree(pos);
                        ae->xs[site_id]->nr--;
                        ae->used_addr--;
                        err = 0;
                        break;
                    }
                }
            } else {
                goto out_unlock_del;
            }
        out_unlock_del:
            xrwlock_wunlock(&ae->rwlock);
        } else {
            /* no OP, we just free the allocated resouces */
            xfree(hta);
        }
    }
out:
    return err;
}

/* compact to a replace buffer */
int addr_mgr_compact(struct addr_entry *ae, void **data, int *len)
{
    struct gk_addr *pos;
    struct gk_site_tx *hst;
    int err =0, i, j = 0, k;

    if (!len || !data)
        return -EINVAL;

    /* NOTE THAT: we lock the site table to protect the ae->used_addr and
     * ae->active_site extends. */
    xrwlock_rlock(&ae->rwlock);

    /* try to alloc the memory space */
    *len = sizeof(struct gk_site_tx) * ae->active_site +
        sizeof(struct gk_addr_tx) * ae->used_addr;
    *data = xmalloc(*len);
    if (!*data) {
        gk_err(root, "xmalloc addr space failed.\n");
        err = -ENOMEM;
        goto out_unlock;
    }

    gk_err(root, "active site nr %d used addr %d %p\n", 
             ae->active_site, ae->used_addr, *data);
    hst = *data;
    for (i = 0; i < (GK_SITE_MAX); i++) {
        if (ae->xs[i]) {
            hst->site_id = i;
            hst->flag = GK_SITE_REPLACE;
            hst->nr = ae->xs[i]->nr;
            if (hst->nr) {
                k = 0;
                /* add the addr to the gk_addr region */
                list_for_each_entry(pos, &ae->xs[i]->addr, list) {
                    if (pos->flag & GK_SITE_PROTOCOL_TCP) {
                        hst->addr[k].flag = pos->flag;
                        hst->addr[k].sock.sa = pos->sock.sa;
#if 0
                        {
                            struct sockaddr_in *sin = (struct sockaddr_in *)
                                &pos->sock.sa;
                            
                            gk_err(root, "compact addr %s %d on site %x\n", 
                                     inet_ntoa(sin->sin_addr),
                                     ntohs(sin->sin_port), i);
                        }
#endif
                        k++;
                    } else {
                        gk_err(root, "Unknown address protocol type, "
                                 "reject it!\n");
                    }
                }
                if (k > hst->nr) {
                    gk_err(root, "Address in site %x extends, we failed\n",
                             i);
                    err = -EFAULT;
                    goto out_free;
                } else if (k < hst->nr) {
                    gk_err(root, "Address in site %x shrinks, we continue\n",
                             i);
                    hst->nr = k;
                }
            }
            hst = (void *)hst + hst->nr * sizeof(struct gk_addr_tx) +
                sizeof(*hst);
            j++;
        }
    }
    if (j != ae->active_site) {
        gk_err(root, "We missed some active sites (%d vs %d).\n",
                 ae->active_site, j);
    }

out_unlock:
    xrwlock_runlock(&ae->rwlock);

    return err;
out_free:
    xrwlock_runlock(&ae->rwlock);
    xfree(*data);
    return err;
}

/* addr_mgr_compact_one
 *
 * This function compact one gk_site to a buffer, you can set the flag by
 * yourself.
 */
int addr_mgr_compact_one(struct addr_entry *ae, u64 site_id, u32 flag,
                         void **data, int *len)
{
    struct gk_addr *pos;
    struct gk_site_tx *hst;
    int err = 0, i = 0;

    if (!len || !data)
        return -EINVAL;

    /* Note that: we lock the site table to protect the ae->xs[i]->nr */
    xrwlock_rlock(&ae->rwlock);
    if (ae->xs[site_id]) {
        *len = sizeof(struct gk_site_tx) +
            sizeof(struct gk_addr_tx) * ae->xs[site_id]->nr;
        *data = xmalloc(*len);
        if (!*data) {
            gk_err(root, "xmalloc addr space failed.\n");
            err = -ENOMEM;
            goto out_unlock;
        }
        hst = *data;
        hst->site_id = site_id;
        hst->flag = flag;
        hst->nr = ae->xs[site_id]->nr;

        if (ae->xs[site_id]->nr) {
            list_for_each_entry(pos, &ae->xs[site_id]->addr, list) {
                if (pos->flag & GK_SITE_PROTOCOL_TCP) {
                    hst->addr[i].flag = pos->flag;
                    hst->addr[i].sock.sa = pos->sock.sa;
                    i++;
                } else {
                    gk_err(root, "Unknown address protocol type, "
                             "reject it!\n");
                }
            }
            if (i > hst->nr) {
                gk_err(root, "Address in site %lx extends, we failed\n",
                         site_id);
                err = -EFAULT;
                goto out_free;
            } else if (i < hst->nr) {
                gk_err(root, "Address in site %lx shrinks, we continue\n",
                         site_id);
                hst->nr = i;
            }
        }
    }

out_unlock:
    xrwlock_runlock(&ae->rwlock);

    return err;
out_free:
    xrwlock_runlock(&ae->rwlock);
    xfree(*data);
    return err;
}

int root_mgr_init(struct root_mgr *rm)
{
    int err = 0, i;

    if (!hro.conf.root_mgr_htsize) {
        /* default to ...ROOT_MGR_HTSIZE */
        hro.conf.root_mgr_htsize = GK_ROOT_ROOT_MGR_HTSIZE;
    }

    rm->rht = xzalloc(hro.conf.root_mgr_htsize * sizeof(struct regular_hash));
    if (!rm->rht) {
        gk_err(root, "xzalloc() root mgr hash table failed.\n");
        err = -ENOMEM;
        goto out;
    }

    xrwlock_init(&rm->rwlock);

    /* init the hash table */
    for (i = 0; i < hro.conf.root_mgr_htsize; i++) {
        INIT_HLIST_HEAD(&(rm->rht + i)->h);
        xlock_init(&(rm->rht + i)->lock);
    }

    /* at last, we just open the root entry file */
    ASSERT(hro.conf.root_store, root);
    err = open(hro.conf.root_store, O_CREAT | O_RDWR | O_SYNC,
               S_IRUSR | S_IWUSR);
    if (err < 0) {
        gk_err(root, "open root store %s failed w/ %s\n",
                 hro.conf.root_store, strerror(errno));
        err = -errno;
        goto out;
    }
    gk_info(root, "Open root store %s success.\n",
              hro.conf.root_store);
    hro.conf.root_store_fd = err;

    err = 0;

out:
    return err;
}

void root_mgr_destroy(struct root_mgr *rm)
{
    if (rm->rht) {
        xfree(rm->rht);
    }
    xrwlock_destroy(&rm->rwlock);
    if (hro.conf.root_store_fd)
        close(hro.conf.root_store_fd);
}

struct root_entry *root_mgr_alloc_re()
{
    struct root_entry *re;

    re = xzalloc(sizeof(*re));
    if (re) {
        INIT_HLIST_NODE(&re->hlist);
    }

    return re;
}

void root_mgr_free_re(struct root_entry *re)
{
    xfree(re);
}

/* root_mgr_lookup() to lookup the root entry
 *
 * @fsid: file system id
 */
struct root_entry *root_mgr_lookup(struct root_mgr *rm, u64 fsid)
{
    struct root_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_root_mgr(fsid, GK_ROOT_ROOT_MGR_SALT) %
        hro.conf.root_mgr_htsize;
    rh = rm->rht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (fsid == pos->fsid) {
            /* ok, we find the entry */
            found = 1;
            break;
        }
    }
    xlock_unlock(&rh->lock);

    if (found) {
        return pos;
    } else {
        return ERR_PTR(-ENOENT);
    }
}

struct root_entry *root_mgr_insert(struct root_mgr *rm, struct root_entry *re)
{
    struct root_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_root_mgr(re->fsid, GK_ROOT_ROOT_MGR_SALT) %
        hro.conf.root_mgr_htsize;
    rh = rm->rht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (re->fsid == pos->fsid) {
            /* ok, we find a conflict entry */
            found = 1;
            break;
        }
    }
    if (!found) {
        xrwlock_wlock(&rm->rwlock);
        hlist_add_head(&re->hlist, &(rm->rht + idx)->h);
        rm->active_root++;
        xrwlock_wunlock(&rm->rwlock);
        pos = re;
    }
    xlock_unlock(&rh->lock);

    return pos;
}

void root_mgr_remove(struct root_mgr *rm, u64 fsid)
{
    struct root_entry *pos;
    struct hlist_node *n;
    struct regular_hash *rh;
    int idx, found = 0;

    idx = gk_hash_root_mgr(fsid, GK_ROOT_ROOT_MGR_SALT) %
        hro.conf.root_mgr_htsize;
    rh = rm->rht + idx;

    xlock_lock(&rh->lock);
    hlist_for_each_entry(pos, n, &rh->h, hlist) {
        if (fsid == pos->fsid) {
            /* ok, we find the entry here */
            found = 1;
            break;
        }
    }
    if (found) {
        /* remove it */
        xrwlock_wlock(&rm->rwlock);
        hlist_del_init(&pos->hlist);
        rm->active_root--;
        xrwlock_wunlock(&rm->rwlock);
    } else {
        gk_err(root, "Try to remove fsid %ld failed, not found.\n",
                 fsid);
    }
    xlock_unlock(&rh->lock);
}

/* root_mgr_lookup_create()
 *
 * This function lookup or create a root enty and load it. if it does not
 * exist, we just return -ENOENT
 *
 * Return value: <0 error; ==0 ok(found); >0 new
 */
int root_mgr_lookup_create(struct root_mgr *rm, u64 fsid,
                           struct root_entry **ore)
{
    struct root_entry *re;
    int err = 0;

    if (!ore) {
        err = -EINVAL;
        goto out;
    }

    re = root_mgr_lookup(rm, fsid);
    if (IS_ERR(re)) {
        if (re == ERR_PTR(-ENOENT)) {
            /* we should create a new site entry now */
            re = root_mgr_alloc_re();
            if (!re) {
                gk_err(root, "root_mgr_alloc_re() failed w/ ENOEM.\n");
                err = -ENOMEM;
                goto out;
            }
            re->fsid = fsid;
            /* we should read in the content of the root entry */
            err = root_read_re(re);
            if (err == -ENOENT) {
                gk_err(root, "fsid %ld not exist\n", fsid);
                root_mgr_free_re(re);
                goto out;
            } else if (err) {
                gk_err(root, "root_read_re() failed w/ %d\n", err);
                root_mgr_free_re(re);
                goto out;
            }
            /* try to insert to the root mgr */
            *ore = root_mgr_insert(rm, re);
            if (IS_ERR(*ore)) {
                gk_err(root, "root_mgr_insert() failed w/ %ld\n",
                         PTR_ERR(*ore));
                err = PTR_ERR(*ore);
                root_mgr_free_re(re);
            }
            if (re != *ore) {
                gk_err(root, "Someone insert root %ld prior us, self free\n",
                         fsid);
                root_mgr_free_re(re);
            }
            err = 1;
        } else {
            /* error here */
            err = PTR_ERR(re);
            gk_err(root, "root_mgr_lookup() failed w/ %d\n", err);
            goto out;
        }
    } else {
        /* set ore to the lookuped entry */
        *ore = re;
    }
        
out:
    return err;
}

/* root_mgr_lookup_create2()
 *
 * This function lookup or create a root enty. if the root entry does not
 * exist, we just create a new one!
 *
 * Return value: <0 error; ==0 ok(found); >0 new
 */
int root_mgr_lookup_create2(struct root_mgr *rm, u64 fsid,
                           struct root_entry **ore)
{
    struct root_entry *re;
    int err = 0;

    if (!ore) {
        err = -EINVAL;
        goto out;
    }

    re = root_mgr_lookup(rm, fsid);
    if (IS_ERR(re)) {
        if (re == ERR_PTR(-ENOENT)) {
            /* we should create a new site entry now */
            re = root_mgr_alloc_re();
            if (!re) {
                gk_err(root, "root_mgr_alloc_re() failed w/ ENOEM.\n");
                err = -ENOMEM;
                goto out;
            }
            re->fsid = fsid;
            /* we should read in the content of the root entry */
            err = root_read_re(re);
            if (err == -ENOENT) {
                gk_err(root, "fsid %ld not exist, however we just "
                         "create it\n", fsid);
                re->magic = lib_random(0xe) + 1; /* in [1,15] */
            } else if (err) {
                gk_err(root, "root_read_re() failed w/ %d\n", err);
                root_mgr_free_re(re);
                goto out;
            }
            /* try to insert to the root mgr */
            *ore = root_mgr_insert(rm, re);
            if (IS_ERR(*ore)) {
                gk_err(root, "root_mgr_insert() failed w/ %ld\n",
                         PTR_ERR(*ore));
                err = PTR_ERR(*ore);
                root_mgr_free_re(re);
            }
            if (re != *ore) {
                gk_err(root, "Someone insert root %ld prior us, self free\n",
                         fsid);
                root_mgr_free_re(re);
            }
            /* finally, write it to disk now */
            err = root_write_re(re);
            if (err) {
                gk_err(root, "Flush fs root %ld to storage failed w/ %d.\n",
                         re->fsid, err);
            }
            err = 1;
        } else {
            /* error here */
            err = PTR_ERR(re);
            gk_err(root, "root_mgr_lookup() failed w/ %d\n", err);
            goto out;
        }
    } else {
        /* set ore to the lookuped entry */
        *ore = re;
    }
        
out:
    return err;
}

/* root_compact_hxi()
 *
 * This function compact the need info for a request client:
 * mds/mdsl/client/osd. The caller should supply the needed arguments.
 *
 * @site_id: requested site_id
 * @fsid: requested fsid
 * @gid: request group id
 */
int root_compact_hxi(u64 site_id, u64 fsid, u32 gid, union gk_x_info *hxi)
{
    struct root_entry *root;
    struct site_entry *se;
    u64 prev_site_id;
    int err = 0;

    if (!hxi)
        return -EINVAL;

    if (GK_IS_CLIENT(site_id) |
        GK_IS_BP(site_id)) {
        /* we should reject if root->root_salt is -1UL */
        /* Step 1: find site state in the site_mgr */
        se = site_mgr_lookup(&hro.site, site_id);
        if (se == ERR_PTR(-ENOENT)) {
            gk_err(root, "site_mgr_lookup() site %lx failed, "
                     "no such site.\n", site_id);
            err = -ENOENT;
            goto out;
        }
        xlock_lock(&se->lock);
        /* check whether the group id is correct */
        if (gid != se->gid) {
            gk_err(root, "CHRING group mismatch: "
                     "request %d conflict w/ %d\n", 
                     gid, se->gid);
            err = -EINVAL;
            goto out_client_unlock;
        }
        switch (se->state) {
        case SE_STATE_INIT:
            /* we should init the se->hxi by read or create the hxi */
            err = root_read_hxi(site_id, fsid, hxi);
            if (err == -ENOTEXIST) {
                err = root_create_hxi(se);
                if (err) {
                    gk_err(root, "create hxi %ld %lx failed w/ %d\n",
                             se->fsid, se->site_id, err);
                    goto out_client_unlock;
                }
                /* write the hxi to disk now */
                err = root_write_hxi(se);
                if (err) {
                    gk_err(root, "write hxi %ld %lx failed w/ %d\n",
                             se->fsid, se->site_id, err);
                    goto out_client_unlock;
                }
            } else if (err) {
                gk_err(root, "root_read_hxi() failed w/ %d\n", err);
                goto out_client_unlock;
            }
            se->hxi.hci = hxi->hci;
            se->state = SE_STATE_NORMAL;
            break;
        case SE_STATE_SHUTDOWN:
            /* we should check whether the fsid is the same as in the se->hxi,
             * if not, we must reload the new view from storage. */

            /* fall through */
        case SE_STATE_NORMAL:
            /* we should check whether the fsid is the same as in the
             * se->hxi. if not, we must reject the new request. */
            err = root_mgr_lookup_create(&hro.root, fsid, &root);
            if (err < 0) {
                gk_err(root, "root_mgr_lookup() fsid %ld failed w/"
                         "%d\n", fsid, err);
                goto out_client_unlock;
            }

            if (fsid != se->fsid) {
                gk_err(root, "This means that we have just update "
                       "the root entry\n");
                if (se->state != SE_STATE_SHUTDOWN) {
                    /* ok, we reject the fs change */
                    err = -EEXIST;
                    goto out_client_unlock;
                }
                /* ok, we can change the fs now */
                err = root_read_hxi(site_id, fsid, hxi);
                if (err == -ENOTEXIST) {
                    err = root_create_hxi(se);
                    if (err) {
                        gk_err(root, "create hxi %ld %lx failed w/ %d\n",
                                 se->fsid, se->site_id, err);
                        goto out_client_unlock;
                    }
                    /* write the hxi to disk now */
                    err = root_write_hxi(se);
                    if (err) {
                        gk_err(root, "write hxi %ld %lx failed w/ %d\n",
                                 se->fsid, se->site_id, err);
                        goto out_client_unlock;
                    }
                } else if (err) {
                    gk_err(root, "root_read_hxi() failed w/ %d\n",
                             err);
                    goto out_client_unlock;
                }
                se->hxi.hci = hxi->hci;
                se->state = SE_STATE_NORMAL;
            } else {
                /* ok, do not need fs change */
                if (se->state != SE_STATE_SHUTDOWN) {
                    /* hoo, there is another server instanc running, we should
                     * reject this request. */
                    err = -EEXIST;
                    goto out_client_unlock;
                }

                /* fs not change, we just modify the state */
                hxi->hci = se->hxi.hci;
                se->state = SE_STATE_NORMAL;
            }
            break;
        case SE_STATE_TRANSIENT:
            /* we should just wait for the system come back to normal or error
             * state */
            err = -EHWAIT;
            break;
        case SE_STATE_ERROR:
            /* in the error state means, we can safely reload/unload the se
             * state */
            prev_site_id = se->site_id;

            err = root_write_hxi(se);
            if (err) {
                gk_err(root, "root_write_hxi() failed w/ %d\n", err);
            }
            se->state = SE_STATE_INIT;
            /* check if we should init a recover process */
            if (prev_site_id == site_id) {
                gk_err(root, "The previous %lx failed to unreg itself, "
                         "we should init a recover process.\n",
                         site_id);
            }

            /* reload the requested fsid */
            err = root_read_hxi(site_id, fsid, hxi);
            if (err) {
                gk_err(root, "root_read_hxi() failed w/ %d\n", err);
                goto out_client_unlock;
            }
            se->hxi.hci = hxi->hci;
            se->state = SE_STATE_NORMAL;
            (prev_site_id == site_id) ? (err = -ERECOVER) : (err = 0);
            break;
        default:;
        }
        /* Step final: release all the resources */
    out_client_unlock:
        xlock_unlock(&se->lock);
    } else if (GK_IS_MDS(site_id)) {
        /* Step 1: find state in the site_mgr */
        se = site_mgr_lookup(&hro.site, site_id);
        if (se == ERR_PTR(-ENOENT)) {
            gk_err(root, "site_mgr_lookup() site %lx failed, "
                     "no such site.\n", site_id);
            err = -ENOENT;
            goto out;
        }

        xlock_lock(&se->lock);
        /* check whether the group id is correct */
        if (gid != se->gid) {
            gk_err(root, "CHRING group mismatch: "
                     "request %d conflict w/ %d\n",
                     gid, se->gid);
            err = -EINVAL;
            goto out_mds_unlock;
        }

        switch (se->state) {
        case SE_STATE_INIT:
            /* we should init the se->hxi by read the hxi in from MDSL */
            err = root_read_hxi(site_id, fsid, hxi);
            if (err == -ENOTEXIST) {
                err = root_create_hxi(se);
                if (err) {
                    gk_err(root, "create hxi %ld %lx failed w/ %d\n",
                             se->fsid, se->site_id, err);
                    goto out_mds_unlock;
                }
                /* write the hxi to disk now */
                err = root_write_hxi(se);
                if (err) {
                    gk_err(root, "write hxi %ld %lx failed w/ %d\n",
                             se->fsid, se->site_id, err);
                    goto out_mds_unlock;
                }
            } else if (err) {
                gk_err(root, "root_read_hxi() failed w/ %d\n", err);
                goto out_mds_unlock;
            }
            se->hxi.hmi = hxi->hmi;
            se->state = SE_STATE_NORMAL;
            break;
        case SE_STATE_SHUTDOWN:
            /* we should check whether the fsid is the same as in the
             * se->hxi. if not, we must reload the new view from MDSL */

            /* fall through */
        case SE_STATE_NORMAL:
            /* we should check whether the fsid is the same as in the
             * se->hxi. if not, we must reject the new request. */
            err = root_mgr_lookup_create(&hro.root, fsid, &root);
            if (err < 0) {
                gk_err(root, "root_mgr_lookup() fsid %ld failed w/"
                         "%d\n", fsid, err);
                goto out_mds_unlock;
            }

            if (fsid != se->fsid) {
                gk_err(root, "This means that we have just update "
                       "the root entry\n");
                if (se->state != SE_STATE_SHUTDOWN) {
                    /* ok, we reject the fs change */
                    err = -EEXIST;
                    goto out_mds_unlock;
                }
                /* ok, we can change the fs now */
                err = root_read_hxi(site_id, fsid, hxi);
                if (err) {
                    gk_err(root, "root_read_hxi() failed w/ %d\n", err);
                    goto out_mds_unlock;
                }
                se->hxi.hmi = hxi->hmi;
                se->state = SE_STATE_NORMAL;
            } else {
                /* ok, do not need fs change */
                if (se->state != SE_STATE_SHUTDOWN) {
                    /* hoo, there is another server instance running, we
                     * should reject this request. */
                    err = -EEXIST;
                    goto out_mds_unlock;
                }
                
                /* fs not change, we just modify the state */
                hxi->hmi = se->hxi.hmi;
                se->state = SE_STATE_NORMAL;
            }
            break;
        case SE_STATE_TRANSIENT:
            /* we should just wait for the system come back to normal or
             * error. */
            err = -EHWAIT;
            break;
        case SE_STATE_ERROR:
            /* in the error state means, we can safely reload/unload the se
             * state. */
            prev_site_id = se->site_id;
            
            err = root_write_hxi(se);
            if (err) {
                gk_err(root, "root_write_hxi() failed w/ %d\n", err);
            }
            se->state = SE_STATE_INIT;
            /* check if we should init a recover process */
            if (prev_site_id == site_id) {
                gk_err(root, "The previous %lx failed to unreg itself, "
                         "we should init a recover process.\n",
                         site_id);
            }
            
            /* reload the requested fsid */
            err = root_read_hxi(site_id, fsid, hxi);
            if (err) {
                gk_err(root, "root_read_hxi() failed w/ %d\n", err);
                goto out_mds_unlock;
            }
            se->hxi.hmi = hxi->hmi;
            se->state = SE_STATE_NORMAL;
            (prev_site_id == site_id) ? (err = -ERECOVER) : (err = 0);
            break;
        default:;
        }
        
        /* Step final: release all the resources */
    out_mds_unlock:
        xlock_unlock(&se->lock);
    } else if (GK_IS_ROOT(site_id)) {
    } else {
        gk_err(root, "Unknown site type: %lx\n", site_id);
        err = -EINVAL;
        goto out;
    }

out:
    return err;
}

int root_read_hxi(u64 site_id, u64 fsid, union gk_x_info *hxi)
{
    struct root_entry *root;
    struct site_disk sd;
    u64 offset;
    int err = 0, bl, br;

    /* Note: if the site_id is a new one, we should use fsid to find the root
     * entry. if the root entry does not exist, we just return an error. The
     * mkfs utility can create a new file system w/ a fsid. After reading the
     * root entry we can construct the site by ourself:) */

    if (GK_IS_MDS(site_id)) {
        err = root_mgr_lookup_create2(&hro.root, fsid, &root);
        if (err < 0) {
            gk_err(root, "lookup create entry %ld failed w/ %d\n",
                     fsid, err);
            goto out;
        } else if (err > 0) {
            gk_err(root, "create fs %ld on-the-fly\n", fsid);
            err = 0;
        }
    } else if (GK_IS_CLIENT(site_id) || 
               GK_IS_BP(site_id)) {
        /* Bug-xxxx: we approve create requests from client and amc sites, it
         * is really need. */
        err = root_mgr_lookup_create2(&hro.root, fsid, &root);
        if (err < 0) {
            gk_err(root, "lookup create entry %ld failed w/ %d\n",
                     fsid, err);
            goto out;
        } else if (err > 0) {
            gk_err(root, "create fs %ld on-the-fly\n", fsid);
            err = 0;
        }
    }

    /* read in the site hxi info from site store file */
    /* Note that the site store file's layout is as follows:
     *
     * [fsid:0 [site table]] | [fsid:1 [site table]]
     */
    offset = fsid * SITE_DISK_WHOLE_FS + site_id * sizeof(struct site_disk);

    bl = 0;
    do {
        br = pread(hro.conf.site_store_fd, ((void *)&sd) + bl, sizeof(sd) - bl,
                   offset + bl);
        if (br < 0) {
            gk_err(root, "pread site disk %ld %lx failed w/ %s\n",
                     fsid, site_id, strerror(errno));
            err = -errno;
            goto out;
        } else if (br == 0) {
            gk_err(root, "pread site disk %ld %lx faild w/ EOF\n",
                     fsid, site_id);
            if (bl == 0)
                err = -ENOTEXIST;
            else
                err = -EINVAL;
            goto out;
        }
        bl += br;
    } while (bl < sizeof(sd));

    if (sd.state != SITE_DISK_VALID) {
        err = -ENOTEXIST;
        goto out;
    }
    
    /* parse site_disk to site_entry->hxi */
    if (sd.fsid != fsid || sd.site_id != site_id) {
        gk_err(root, "Internal error, fsid/site_id mismatch!\n");
        err = -EFAULT;
        goto out;
    }
    if (GK_IS_CLIENT(site_id) | GK_IS_BP(site_id)) {
        struct gk_client_info *hci = (struct gk_client_info *)hxi;

        memcpy(hxi, &sd.hxi, sizeof(*hci));
    } else if (GK_IS_MDS(site_id)) {
        struct gk_mds_info *hmi = (struct gk_mds_info *)hxi;

        memcpy(hxi, &sd.hxi, sizeof(*hmi));
    }
    
out:
    return err;
}

int root_write_hxi(struct site_entry *se)
{
    struct site_disk sd;
    u64 offset;
    int err = 0, bl, bw;

    gk_warning(root, "Write site %lx fsid %ld hxi: \n",
                 se->site_id, se->fsid);
    sd.state = SITE_DISK_VALID;
    sd.gid = se->gid;
    sd.fsid = se->fsid;
    sd.site_id = se->site_id;
    memcpy(&sd.hxi, &se->hxi, sizeof(se->hxi));

    offset = se->fsid * SITE_DISK_WHOLE_FS + 
        se->site_id * sizeof(struct site_disk);

    bl = 0;
    do {
        bw = pwrite(hro.conf.site_store_fd, ((void *)&sd) + bl,
                    sizeof(sd) - bl, offset + bl);
        if (bw < 0) {
            gk_err(root, "pwrite site disk %ld %lx failed w/ %s\n",
                     se->fsid, se->site_id, strerror(errno));
            err = -errno;
            goto out;
        } else if (bw == 0) {
            /* just retry it */
        }
        bl += bw;
    } while (bl < sizeof(sd));

out:
    return err;
}

int root_clean_hxi(struct site_entry *se)
{
    struct site_disk sd;
    u64 offset;
    int err = 0, bw;

    sd.state = SITE_DISK_INVALID;
    offset = se->fsid * SITE_DISK_WHOLE_FS +
        se->site_id * sizeof(struct site_disk);
    offset += offsetof(struct site_disk, state);

    do {
        bw = pwrite(hro.conf.site_store_fd, &sd.state, 1,
                    offset);
        if (bw < 0) {
            gk_err(root, "clean site disk %ld %lx failed w/ %s\n",
                     se->fsid, se->site_id, strerror(errno));
            err = -errno;
            goto out;
        }
    } while (bw > 0);

out:
    return err;
}

int root_create_hxi(struct site_entry *se)
{
    struct root_entry *root;
    int err = 0;

    root = root_mgr_lookup(&hro.root, se->fsid);
    if (IS_ERR(root)) {
        gk_err(root, "lookup root %ld failed w/ %ld\n",
                 se->fsid, PTR_ERR(root));
        err = PTR_ERR(root);
        goto out;
    }
    if (GK_IS_CLIENT(se->site_id) |
        GK_IS_BP(se->site_id)) {
        struct gk_client_info *hci = (struct gk_client_info *)&se->hxi;

        memset(hci, 0, sizeof(*hci));
        hci->state = HMI_STATE_CLEAN;
        hci->group = se->gid;
    } else if (GK_IS_MDS(se->site_id)) {
        struct gk_mds_info *hmi = (struct gk_mds_info *)&se->hxi;

        memset(hmi, 0, sizeof(*hmi));
        hmi->state = HMI_STATE_CLEAN;
        hmi->group = se->gid;
    }
    
out:
    return err;
}

/* root_read_re() reading the root entry from the file
 *
 * Note: the root file should be sorted by fsid, then we can get the root
 * entry very fast!
 */
int root_read_re(struct root_entry *re)
{
    loff_t offset;
    struct root_disk rd;
    int err = 0, bl, br;

    /* we read the root entry based on the re->fsid */
    if (!hro.conf.root_store_fd) {
        return -EINVAL;
    }

    offset = re->fsid * sizeof(rd);

    bl = 0;
    do {
        br = pread(hro.conf.root_store_fd, ((void *)&rd) + bl, 
                   sizeof(rd) - bl, offset + bl);
        if (br < 0) {
            gk_err(root, "read root entry %ld failed w/ %s\n",
                     re->fsid, strerror(errno));
            err = -errno;
            goto out;
        } else if (br == 0) {
            gk_err(root, "read root entry %ld failed w/ EOF\n",
                     re->fsid);
            if (bl == 0)
                err = -ENOENT;
            else
                err = -EINVAL;
            goto out;
        }
        bl += br;
    } while (bl < sizeof(rd));

    if (rd.state == ROOT_DISK_INVALID) {
        err = -ENOENT;
    } else {
        if (re->fsid != rd.fsid) {
            gk_err(root, "fsid mismatch expect %ld read %ld\n", re->fsid, rd.fsid);
        }
        re->magic = rd.magic;
    }

out:
    return err;
}

/* root_write_re() write the root entry to the file
 *
 * Note: we write to the fixed location by lseek
 */
int root_write_re(struct root_entry *re)
{
    loff_t offset;
    struct root_disk rd;
    int err = 0, bl, bw;

    /* we read the root entry based on the re->fsid */
    if (!hro.conf.root_store_fd) {
        return -EINVAL;
    }

    offset = re->fsid * sizeof(rd);

    rd.state = ROOT_DISK_VALID;
    rd.fsid = re->fsid;
    rd.magic = re->magic;

    bl = 0;
    do {
        bw = pwrite(hro.conf.root_store_fd, ((void *)&rd) + bl,
                    sizeof(rd) - bl, offset + bl);
        if (bw < 0) {
            gk_err(root, "write root entry %ld failed w/ %s\n",
                     re->fsid, strerror(errno));
            err = -errno;
            goto out;
        } else if (bw == 0) {
            /* we just retry write */
        }
        bl += bw;
    } while (bl < sizeof(rd));

out:
    return err;
}

void site_mgr_check(time_t ctime)
{
    struct site_entry *pos;
    struct hlist_node *n;
    static time_t ts = 0;
    int i;

    if (ctime - ts < hro.conf.hb_interval) {
        return;
    } else
        ts = ctime;
    
    for (i = 0; i < hro.conf.site_mgr_htsize; i++) {
        hlist_for_each_entry(pos, n, &hro.site.sht[i].h, hlist) {
            xlock_lock(&pos->lock);
            switch (pos->state) {
            case SE_STATE_NORMAL:
                pos->hb_lost++;
                if (pos->hb_lost > TRANSIENT_HB_LOST) {
                    gk_err(root, "Site %lx lost %d, transfer to TRANSIENT.\n",
                             pos->site_id, pos->hb_lost);
                    pos->state = SE_STATE_TRANSIENT;
                } else if (pos->hb_lost > MAX_HB_LOST) {
                    gk_err(root, "Site %lx lost %d, transfer to ERROR.\n",
                         pos->site_id, pos->hb_lost);
                    pos->state = SE_STATE_ERROR;
                }
                break;
            case SE_STATE_TRANSIENT:
                pos->hb_lost++;
                if (pos->hb_lost > MAX_HB_LOST) {
                    gk_err(root, "Site %lx lost %d, transfer to ERROR.\n",
                         pos->site_id, pos->hb_lost);
                    pos->state = SE_STATE_ERROR;
                }
                break;
            default:;
            }
            xlock_unlock(&pos->lock);
        }
    }
}

char *__int2state(u32 state)
{
    switch (state) {
    case SE_STATE_INIT:
        return "INIT";
        break;
    case SE_STATE_NORMAL:
        return "NORM";
        break;
    case SE_STATE_SHUTDOWN:
        return "STDN";
        break;
    case SE_STATE_TRANSIENT:
        return "TRAN";
        break;
    case SE_STATE_ERROR:
        return "ERRO";
        break;
    default:
        return "ERRO";
    }
}

/* dump the site state array to stdout
 */
void site_mgr_check2(time_t ctime)
{
    struct site_entry *pos;
    struct hlist_node *n;
#define MAX_STR (10 * 1024)
    char mds[MAX_STR], client[MAX_STR], bp[MAX_STR];
    char *pmds = mds, *pclient = client, *pbp = bp;
    static time_t ts = 0;
    int i;

    if (ctime - ts < hro.conf.log_print_interval) {
        return;
    } else
        ts = ctime;
    
    for (i = 0; i < hro.conf.site_mgr_htsize; i++) {
        hlist_for_each_entry(pos, n, &hro.site.sht[i].h, hlist) {
            if (GK_IS_MDS(pos->site_id)) {
                struct gk_mds_info *hmi = (struct gk_mds_info *)(&pos->hxi);

                if (pmds == mds) {
                    pmds += sprintf(pmds, "MDS : [site_id, state, hb_lost, "
                                    "next_tx, next_txg, next_uuid, fnum, "
                                    "dnum, next_bid]\n");
                }
                pmds += sprintf(pmds, "[%lx, %s, %d]\n",
                                pos->site_id, __int2state(pos->state),
                                pos->hb_lost);
            } else if (GK_IS_CLIENT(pos->site_id)) {
                if (pclient == client) {
                    pclient += sprintf(pclient, "CLT : [site_id, state, hb_lost]\n");
                }
                pclient += sprintf(pclient, "[%lx, %s, %d]\n",
                                   pos->site_id, __int2state(pos->state),
                                   pos->hb_lost);
            } else if (GK_IS_BP(pos->site_id)) {
                if (pbp == bp) {
                    pbp += sprintf(pbp, "BP  : [site_id, state, hb_lost]\n");
                }
                pbp += sprintf(pbp, "[%lx, %s, %d]\n",
                                   pos->site_id, __int2state(pos->state),
                                   pos->hb_lost);
            }
        }
    }

    /* print the info to console */
    if (pmds > mds)
        gk_plain(root, "%s", mds);
    if (pbp > bp)
        gk_plain(root, "%s", bp);
    if (pclient > client)
        gk_plain(root, "%s", client);
}

/* get the non-shutdown sites, ignore any errors
 */
struct xnet_group *site_mgr_get_active_site(u32 type)
{
    struct xnet_group *xg = NULL;
    struct site_entry *pos;
    struct hlist_node *n;
    int i, __UNUSED__ err;

    for (i = 0; i < hro.conf.site_mgr_htsize; i++) {
        hlist_for_each_entry(pos, n, &hro.site.sht[i].h, hlist) {
            if (GK_GET_TYPE(pos->site_id) == type &&
                (pos->state == SE_STATE_NORMAL ||
                 pos->state == SE_STATE_TRANSIENT ||
                 pos->state == SE_STATE_ERROR))
                err = xnet_group_add(&xg, pos->site_id);
        }
    }

    return xg;
}
