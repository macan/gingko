/**
 * Copyright (c) 2019 Ma Can <ml.macana@gmail.com>
 *                           <macan@iie.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-10-09 16:36:42 macan>
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

/* This is a memory kv store.
 *
 * namespace -> hash table
 *                  |
 *                  ----> key -> value
 */

struct namespace_mgr ns_mgr;

static inline
char *GET_TYPE_STR(struct ns_entry *nse)
{
    if (nse->type == NSE_F_LMDB) {
        return "lmdb";
    } else if (nse->type == NSE_F_LOG) {
        return "log";
    } else {
        return "none";
    }
}

/* return non-zero if the dir exists
 */
int kvs_dir_is_exist(char *path)
{
    struct stat s = {0,};
    int err;

    err = stat(path, &s);
    if (err == ENOENT) {
        return 0;
    } else
        return 1;
}

int kvs_dir_make_exist(char *path)
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

int kvs_init(void)
{
    char path[GK_MAX_NAME_LEN] = {0,};
    int err, i;
    
    if (hmo.conf.nshash_size) {
        hmo.conf.nshash_size = MDS_KVS_NSHASH_SIZE;
    }
    if (hmo.conf.ns_ht_size) {
        hmo.conf.ns_ht_size = NS_HASH_SIZE;
    }
    ns_mgr.nsht = xmalloc(hmo.conf.nshash_size * sizeof(struct regular_hash_rw));
    if (!ns_mgr.nsht) {
        gk_err(mds, "alloc namespace hash table (size=%ld) failed.\n",
               hmo.conf.nshash_size * sizeof(struct regular_hash_rw));
        return -ENOMEM;
    }

    /* init the hash table */
    for (i = 0; i < hmo.conf.nshash_size; i++) {
        INIT_HLIST_HEAD(&(ns_mgr.nsht + i)->h);
        xrwlock_init(&(ns_mgr.nsht + i)->lock);
    }
    INIT_LIST_HEAD(&ns_mgr.lru);
    xlock_init(&ns_mgr.lru_lock);

    /* init the directory */
    err = kvs_dir_make_exist(hmo.conf.kvs_home);
    if (err) {
        gk_err(mds, "dir %s do not exist %d.\n", hmo.conf.kvs_home, err);
        return -ENOTEXIST;
    }

    gk_info(mds, "MDS KVS init ok\n");

    return err;
}

static inline
void kvs_ns_lru_update(struct ns_entry *nse)
{
    xlock_lock(&ns_mgr.lru_lock);
    list_del_init(&nse->lru);
    list_add(&nse->lru, &ns_mgr.lru);
    xlock_unlock(&ns_mgr.lru_lock);
}

static inline
void __cache_put(struct ns_entry *nse)
{
    struct ns_entry *last = (struct ns_entry *)pthread_getspecific(spool_key);

    if (last) {
        kvs_ns_put(last);
    }
    pthread_setspecific(spool_key, nse);
}

struct ns_entry *kvs_ns_lookup(struct gstring *namespace)
{
    struct ns_entry *nse;
    struct hlist_node *pos;
    int idx;

    /* Step 1: lookup the thread local cache */
    nse = (struct ns_entry *)pthread_getspecific(spool_key);
    if (likely(nse)) {
        if (nse->namespace.len == namespace->len &&
            (memcmp(nse->namespace.start, namespace->start, 
                    min(namespace->len, nse->namespace.len)) == 0)) {
            /* cache hit */
            atomic_inc(&nse->ref);
            kvs_ns_lru_update(nse);
            return nse;
        }
    }

    /* Step 2: lookup the ns entry */
    idx = gk_hash_nsht(namespace->start, namespace->len) % hmo.conf.nshash_size;
    xrwlock_rlock(&(ns_mgr.nsht + idx)->lock);
    hlist_for_each_entry(nse, pos, &(ns_mgr.nsht + idx)->h, list) {
        if ((namespace->len == nse->namespace.len) && 
            (memcmp(namespace->start, nse->namespace.start, 
                    min(namespace->len, nse->namespace.len)) == 0)) {
            atomic_add(2, &nse->ref);
            xrwlock_runlock(&(ns_mgr.nsht + idx)->lock);
            /* lru update */
            kvs_ns_lru_update(nse);
            /* put it into the thread local cache */
            __cache_put(nse);
            return nse;
        }
    }
    xrwlock_runlock(&(ns_mgr.nsht + idx)->lock);

    return ERR_PTR(-ENOENT);
}

void kvs_ns_remove(struct ns_entry *new)
{
    int idx;

    idx = gk_hash_nsht(new->namespace.start, 
                       new->namespace.len) % hmo.conf.nshash_size;
    xrwlock_wlock(&(ns_mgr.nsht + idx)->lock);
    hlist_del_init(&new->list);
    xlock_lock(&ns_mgr.lru_lock);
    /* race with kvs_ns_limit_check() */
    if (list_empty(&new->lru)) {
        xlock_unlock(&(ns_mgr.lru_lock));
        xrwlock_wunlock(&(ns_mgr.nsht + idx)->lock);
        return;
    }
    list_del_init(&new->lru);
    xlock_unlock(&ns_mgr.lru_lock);
    xrwlock_wunlock(&(ns_mgr.nsht + idx)->lock);
    atomic_dec(&ns_mgr.active);
}

void kvs_ns_limit_check(time_t cur)
{
    /* Step 1: check if we can free some nse entries */
    /* Step 2: check the memcache */
}

/* Insert a nse to namespace
 * 
 * If the return value(nse) is not equal *new, then (nse)'s reference
 * is increased.
 */
struct ns_entry *kvs_ns_insert(struct ns_entry *new)
{
    struct ns_entry *nse;
    struct hlist_node *pos;
    int idx, found = 0;

    if (unlikely(!new->namespace.start))
        return ERR_PTR(-EINVAL);
    
    idx = gk_hash_nsht(new->namespace.start,
                       new->namespace.len) % hmo.conf.nshash_size;
    xrwlock_wlock(&(ns_mgr.nsht + idx)->lock);
    hlist_for_each_entry(nse, pos, &(ns_mgr.nsht + idx)->h, list) {
        if ((new->namespace.len == nse->namespace.len) && 
            (memcmp(new->namespace.start, nse->namespace.start, 
                    min(new->namespace.len, nse->namespace.len)) == 0)) {
            atomic_inc(&nse->ref);
            found = 1;
            break;
        }
    }
    if (!found) {
        hlist_add_head(&new->list, &(ns_mgr.nsht + idx)->h);
        atomic_inc(&ns_mgr.active);
    }
    xrwlock_wunlock(&(ns_mgr.nsht + idx)->lock);

    if (found) {
        /* lru update */
        kvs_ns_lru_update(nse);
        return nse;
    } else {
        /* lru update */
        kvs_ns_lru_update(new);
        return new;
    }
}

/* __logf_open() do logf file open
 */
int __logf_open(struct ns_entry *nse, char *path)
{
    int err = 0;

    xlock_lock(&nse->lock);
    if (nse->state == NSE_FREE) {
        /* ok, we should open it */
        nse->fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (nse->fd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                   path, strerror(errno), -errno);
            err = -errno;
            goto out_unlock;
        }
        gk_warning(mds, "open file %s w/ fd %d\n", path, nse->fd);
        nse->state = NSE_OPEN;
    }
    if (nse->state == NSE_OPEN) {
        /* we should lseek to the tail of the file */
        nse->f.log.foffset = lseek(nse->fd, 0, SEEK_END);
        if (nse->f.log.foffset == -1UL) {
            gk_err(mds, "lseek to tail of fd(%d) failed w/ %d\n",
                     nse->fd, -errno);
            err = -errno;
            goto out_unlock;
        }
        nse->state = NSE_LOGF;
    }
out_unlock:
    xlock_unlock(&nse->lock);

    return err;
}

void __logf_close(struct ns_entry *nse)
{
    if (nse->fd) {
        fsync(nse->fd);
        close(nse->fd);
    }
}

/* __logf_read()
 *
 * read with offset == -1 means read from the current offset
 */
int __logf_read(struct ns_entry *nse, struct kvs_storage_access *ksa)
{
    loff_t offset;
    long bl, br;
    int err = 0, i;

    if (unlikely(nse->state < NSE_OPEN || !ksa->iov_nr || !ksa->iov))
        return -EINVAL;
    
    offset = ksa->offset;
    if (offset == -1UL) {
        offset = lseek(nse->fd, 0, SEEK_CUR);
    }
    
    gk_debug(mds, "read offset %ld len %ld\n", offset, ksa->iov->iov_len);
    for (i = 0; i < ksa->iov_nr; i++) {
        bl = 0;
        do {
            br = pread(nse->fd, (ksa->iov + i)->iov_base + bl, 
                       (ksa->iov + i)->iov_len - bl, offset + bl);
            if (br < 0) {
                gk_err(mds, "pread failed w/ %d\n", errno);
                err = -errno;
                goto out;
            } else if (br == 0) {
                gk_err(mds, "reach EOF.\n");
                err = -EINVAL;
                goto out;
            }
            bl += br;
        } while (bl < (ksa->iov + i)->iov_len);
        offset += bl;
        atomic64_add(bl, &hmo.prof.storage.rbytes);
    }

out:
    
    return err;
}

/* __logf_write()
 *
 * write with offset == -1 means write to the tail, return the write
 * offset at ksa->arg
 * write with offset == -2 means write by the f.log.foffset
 */
int __logf_write(struct ns_entry *nse, struct kvs_storage_access *ksa)
{
    loff_t offset = 0;
    long bl, bw;
    int err = 0, i;
    
    xlock_lock(&nse->lock);
    if (ksa->offset == -1) {
        nse->f.log.foffset = lseek(nse->fd, 0, SEEK_END);
        if (nse->f.log.foffset == -1UL) {
            gk_err(mds, "do lseek on fd(%d) failed w/ %d\n",
                     nse->fd, -errno);
            err = -errno;
            goto out;
        }
        offset = *((u64 *)ksa->arg) = nse->f.log.foffset;
    } else if (ksa->offset == -2) {
        offset = *((u64 *)ksa->arg) = nse->f.log.foffset;
    } else
        offset = *((u64 *)ksa->arg) = ksa->offset;

    for (i = 0; i < ksa->iov_nr; i++) {
        bl = 0;
        do {
            bw = pwrite(nse->fd, (ksa->iov + i)->iov_base + bl,
                        (ksa->iov + i)->iov_len - bl, offset + bl);
            if (bw < 0) {
                gk_err(mds, "pwrite failed w/ %d\n", errno);
                err = -errno;
                goto out;
            } else if (bw == 0) {
                gk_err(mds, "reach EOF?\n");
                err = -EINVAL;
                goto out;
            }
            bl += bw;
        } while (bl < (ksa->iov + i)->iov_len);
        offset += bl;
        atomic64_add(bl, &hmo.prof.storage.wbytes);
    }
out:
    nse->f.log.foffset = offset;
    xlock_unlock(&nse->lock);

    return err;
}

/* __lmdb_open() do lmdb file open
 */
int __lmdb_open(struct ns_entry *nse, char *path)
{
    MDB_env *env;
    MDB_txn *txn;
    int err = 0;

    xlock_lock(&nse->lock);
    if (nse->state == NSE_FREE) {
        err = mdb_env_create(&env);
        if (err) {
            gk_err(mds, "lmdb env create failed w %d\n", err);
            err = -err;
            goto out_unlock;
        }
        err = mdb_env_set_mapsize(env, LMDB_DEFAULT_SIZE);
        if (err) {
            gk_err(mds, "lmdb env set mapsize failed w/ %d\n", err);
            err = -err;
            goto out_unlock;
        }
        err = mdb_env_open(env, path, 0, 0664);
        if (err) {
            gk_err(mds, "lmdb env open %s failed w/ %d\n", path, err);
            err = -err;
            goto out_unlock;
        }
        gk_warning(mds, "open lmdb file %s w/ env %p\n", path, env);
        nse->state = NSE_OPEN;
        nse->f.lmdb.env = env;
    }
    if (nse->state == NSE_OPEN) {
        err = mdb_txn_begin(nse->f.lmdb.env, NULL, 0, &txn);
        if (err) {
            gk_err(mds, "lmdb txn begin failed w/ %d\n", err);
            err = -err;
            goto out_unlock;
        }
	err = mdb_dbi_open(txn, NULL, 0, &nse->f.lmdb.dbi);
        if (err) {
            gk_err(mds, "lmdb dbi open failed w/ %d\n", err);
            err = -err;
            goto out_unlock;
        }
        err = mdb_txn_commit(txn);
        if (err) {
            gk_err(mds, "lmdb txn commit failed w/ %d\n", err);
            err = -err;
            goto out_unlock;
        }
        nse->state = NSE_LMDB;
    }
out_unlock:
    xlock_unlock(&nse->lock);

    return err;
}

void __lmdb_close(struct ns_entry *nse)
{
    mdb_dbi_close(nse->f.lmdb.env, nse->f.lmdb.dbi);
    mdb_env_close(nse->f.lmdb.env);
}

/* __lmdb_write()
 *
 */
int __lmdb_write(struct ns_entry *nse, struct kvs_storage_access *ksa)
{
    MDB_val key, data;
    MDB_txn *txn;
    int err = 0, i;

    xlock_lock(&nse->lock);
    err = mdb_txn_begin(nse->f.lmdb.env, NULL, 0, &txn);
    if (err) {
        xlock_unlock(&nse->lock);
        gk_err(mds, "lmdb txn begin failed w/ %d\n", err);
        return -err;
    }
    for (i = 0; i < ksa->iov_nr; i += 2) {
        key.mv_size = ksa->iov[i].iov_len;
        key.mv_data = ksa->iov[i].iov_base;
        data.mv_size = ksa->iov[i + 1].iov_len;
        data.mv_data = ksa->iov[i + 1].iov_base;
    
        err = mdb_put(txn, nse->f.lmdb.dbi, &key, &data, 0);
        if (err) {
            gk_err(mds, "lmdb put failed w/ %d\n", err);
            continue;
        }
    }
    
    err = mdb_txn_commit(txn);
    xlock_unlock(&nse->lock);
    if (err) {
        gk_err(mds, "lmdb txn commit failed w/ %d\n", err);
        err = -err;
    }

    return err;
}

int __lmdb_read(struct ns_entry *nse, struct kvs_storage_access *ksa)
{
    MDB_val key, data;
    MDB_txn *txn;
    void *_tmp;
    int err = 0, i;

    xlock_lock(&nse->lock);
    err = mdb_txn_begin(nse->f.lmdb.env, NULL, MDB_RDONLY, &txn);
    if (err) {
        xlock_unlock(&nse->lock);
        gk_err(mds, "lmdb txn rdonly begin failed w/ %d\n", err);
        return -err;
    }
    for (i = 0; i < ksa->iov_nr; i+=2) {
        key.mv_size = ksa->iov[i].iov_len;
        key.mv_data = ksa->iov[i].iov_base;
        
        err = mdb_get(txn, nse->f.lmdb.dbi, &key, &data);
        if (err) {
            gk_err(mds, "lmdb get failed w/ %d\n", err);
            continue;
        }
        _tmp = xmalloc(data.mv_size);
        if (!_tmp) {
            gk_err(mds, "xmalloc() failed\n");
            continue;
        }
        memcpy(_tmp, data.mv_data, data.mv_size);
        ksa->iov[i + 1].iov_len = data.mv_size;
        ksa->iov[i + 1].iov_base = _tmp;
    }
    mdb_txn_abort(txn);
    xlock_unlock(&nse->lock);

    return err;
}

static inline
int kvs_ns_init(struct ns_entry *nse)
{
    char path[GK_MAX_NAME_LEN] = {0,};
    int err = 0;

    if (nse->type == NSE_F_MEMONLY)
        goto out;

    sprintf(path, "%s/%s", hmo.conf.kvs_home, nse->namespace.start);
    err = kvs_dir_make_exist(path);
    if (err) {
        gk_err(mds, "dir %s does not exist %d.\n", path, err);
        goto out;
    }

    switch (nse->type) {
    case NSE_F_LOG:
        sprintf(path, "%s/%s/%s", hmo.conf.kvs_home, nse->namespace.start, GET_TYPE_STR(nse));
        err = __logf_open(nse, path);
        if (err) {
            gk_err(mds, "logf file create %s failed w/ %d\n", path, err);
            goto out;
        }
        break;
    case NSE_F_LMDB:
        sprintf(path, "%s/%s/%s", hmo.conf.kvs_home, nse->namespace.start, GET_TYPE_STR(nse));
        err = kvs_dir_make_exist(path);
        if (err) {
            gk_err(mds, "dir %s does not exist %d.\n", path, err);
            goto out;
        }
        err = __lmdb_open(nse, path);
        if (err) {
            gk_err(mds, "lmdb file create %s failed w/ %d\n", path, err);
            goto out;
        }
        break;
    }

out:
    return err;
}

struct ns_entry *kvs_ns_lookup_create(struct gstring *namespace, short type)
{
    struct ns_entry *nse;
    int err = 0;

relookup:
    nse = kvs_ns_lookup(namespace);
    if (likely(!IS_ERR(nse))) {
        if (unlikely(nse->state <= NSE_OPEN)) {
            goto reinit;
        } else
            return nse;
    } else if (PTR_ERR(nse) == -ENOENT) {
        /* Step 1: create a new ns_entry */
        nse = xmalloc(sizeof(*nse));
        if (unlikely(!nse)) {
            gk_err(mds, "xmalloc() struct ns_entry failed.\n");
            return ERR_PTR(-ENOMEM);
        } else {
            struct ns_entry *inserted;
            int i;
            
            nse->namespace.start = strndup(namespace->start, namespace->len);
            if (unlikely(!nse->namespace.start)) {
                gk_err(mds, "strdup() ns %.*s failed.\n", namespace->len, namespace->start);
                xfree(nse);
                return ERR_PTR(-ENOMEM);
            }
            nse->namespace.len = namespace->len;
            nse->ht = xmalloc(sizeof(struct regular_hash) * hmo.conf.ns_ht_size);
            if (unlikely(!nse->ht)) {
                gk_err(mds, "xmalloc() ns hash table failed.\n");
                xfree(nse->namespace.start);
                xfree(nse);
                return ERR_PTR(-ENOMEM);
            }
            /* init the ns hash table */
            for (i = 0; i < hmo.conf.ns_ht_size; i++) {
                INIT_HLIST_HEAD(&(nse->ht + i)->h);
                xlock_init(&(nse->ht + i)->lock);
            }
            
            INIT_HLIST_NODE(&nse->list);
            INIT_LIST_HEAD(&nse->lru);
            xlock_init(&nse->lock);
            atomic_set(&nse->ref, 1);
            nse->type = type;
            nse->state = NSE_FREE;
            /* insert into the ns hash table */
            inserted = kvs_ns_insert(nse);
            if (inserted != nse) {
                gk_warning(mds, "someone insert this nse(%.*s) before us.\n", 
                           namespace->len, namespace->start);
                xfree(nse->namespace.start);
                xfree(nse->ht);
                xfree(nse);
                kvs_ns_put(inserted);
                goto relookup;
            }
        }
        
        /* Step 2: we should open the backend file now */
    reinit:
        err = kvs_ns_init(nse);
        if (unlikely(err)) {
            goto out_clean;
        }
    }
    
    return nse;
out_clean:
    /* we should release the nse on error */
    if (atomic_dec_return(&nse->ref) == 0) {
        kvs_ns_remove(nse);
        xfree(nse);
    }

    return ERR_PTR(err);
}

static inline
int __ns_store_read(struct ns_entry *nse, struct gstring *key, struct gstring *value)
{
    int err = 0;

    switch (nse->type) {
    case NSE_F_LOG:
        break;
    case NSE_F_LMDB:
    {
        struct iovec iovs[] = {
            {
                .iov_base = key->start,
                .iov_len = key->len,
            },
            {
                .iov_base = NULL,
                .iov_len = 0,
            },
        };
        struct kvs_storage_access ksa = {
            .iov = iovs,
            .arg = 0,
            .offset = 0,
            .iov_nr = 2,
        };

        if (nse->state < NSE_LMDB) {
            gk_err(mds, "namespace %.*s lmdb read: invalid state %d\n",
                   nse->namespace.len, nse->namespace.start, nse->state);
            err = -EFAULT;
            goto out;
        }
        err = __lmdb_read(nse, &ksa);
        if (err) {
            gk_err(mds, "lmdb read failed w/ %d\n", err);
            goto out;
        }
        value->len = ksa.iov[1].iov_len;
        value->start = xmalloc(value->len);
        if (!value->start) {
            gk_err(mds, "xmalloc failed\n");
            goto out;
        }
        memcpy(value->start, ksa.iov[1].iov_base, value->len);
        break;
    }
    default:
        gk_err(mds, "namespace %.*s has invalid storage type: %d\n",
               nse->namespace.len, nse->namespace.start, nse->type);
    }
out:
    return err;
}

static inline
int __ns_store_write(struct ns_entry *nse, struct gstring *key, struct gstring *value, int force)
{
    int err = 0;
    u64 offset = 0;

    switch (nse->type) {
    case NSE_F_LOG:
    {
        struct iovec iovs[] = {
            {
                .iov_base = key->start,
                .iov_len = key->len,
            },
            {
                .iov_base = ",",
                .iov_len = 1,
            },
            {
                .iov_base = value->start,
                .iov_len = value->len,
            },
            {
                .iov_base = "\n",
                .iov_len = 1,
            },
        };
        struct kvs_storage_access ksa = {
            .iov = iovs,
            .arg = &offset,
            .offset = -2,
            .iov_nr = 4,
        };
        
        if (nse->state < NSE_LOGF) {
            gk_err(mds, "namespace %.*s logf write: invalid state %d\n",
                   nse->namespace.len, nse->namespace.start, nse->state);
            err = -EFAULT;
            goto out;
        }
        err = __logf_write(nse, &ksa);
        break;
    }
    case NSE_F_LMDB:
    {
        struct iovec iovs[] = {
            {
                .iov_base = key->start,
                .iov_len = key->len,
            },
            {
                .iov_base = value->start,
                .iov_len = value->len,
            },
        };
        struct kvs_storage_access ksa = {
            .iov = iovs,
            .arg = &offset,
            .offset = -1,
            .iov_nr = 2,
        };
        
        if (nse->state < NSE_LMDB) {
            gk_err(mds, "namespace %.*s lmdb write: invalid state %d\n",
                   nse->namespace.len, nse->namespace.start, nse->state);
            err = -EFAULT;
        }
        err = __lmdb_write(nse, &ksa);
        break;
    }
    default:
        gk_err(mds, "namespace %.*s has invalid storage type: %d\n",
               nse->namespace.len, nse->namespace.start, nse->type);
    }
out:    
    return err;
}

/* Insert a kv pair to the ns entry
 */
int __ns_insert(struct ns_entry *nse, struct gstring *key, struct gstring *value, int force)
{
    struct nsh_entry *nshe, *new;
    struct hlist_node *pos;
    char *_tmp;
    int idx, err = 0, found = 0;

#if 1
    _tmp = ((_tmp = xmalloc(value->len)) ? memcpy(_tmp, value->start, value->len) : 0);
#else
    _tmp = strndup(value->start, value->len);
#endif
    if (unlikely(!_tmp)) {
        gk_err(mds, "dup value %.*s for key %.*s failed\n",
               value->len, value->start, 
               key->len, key->start);
        return -ENOMEM;
    }

    /* create a nsh entry */
    new = xzalloc(sizeof(*new));
    if (unlikely(!new)) {
        gk_err(mds, "xmalloc() nsh_entry failed\n");
        xfree(_tmp);
        return -ENOMEM;
    }

    /* init the nsh entry */
    INIT_HLIST_NODE(&new->list);
    new->key.start = strndup(key->start, key->len);
    if (unlikely(!new->key.start)) {
        gk_err(mds, "strdup(%.*s) failed\n", key->len, key->start);
        err = -ENOMEM;
        goto out_free;
    }
    new->key.len = key->len;
    new->value.start = _tmp;
    new->value.len = value->len;
    
    idx = gk_hash_ns(key->start, key->len) % hmo.conf.ns_ht_size;
    xlock_lock(&(nse->ht + idx)->lock);
    hlist_for_each_entry(nshe, pos, &(nse->ht + idx)->h, list) {
        atomic64_inc(&hmo.prof.mds.ns_ins_collisions);
        if ((nshe->key.len == key->len) &&
            (memcmp(nshe->key.start, key->start, 
                    min(key->len, nshe->key.len)) == 0)) {
            /* found it: 
             * if force == 1, update it; otherwise return
             * -EEXIST */
            if (force) {
                xfree(nshe->value.start);
                xfree(new);
                nshe->value.start = _tmp;
                nshe->value.len = value->len;
            } else {
                err = -EEXIST;
            }
            found = 1;
            break;
        }
    }
    if (!found) {
        /* insert the new entry to the list */
        hlist_add_head(&new->list, &(nse->ht + idx)->h);
        atomic_inc(&nse->nr);
    }
    xlock_unlock(&(nse->ht + idx)->lock);

out_free:
    if (unlikely(err)) {
        xfree(_tmp);
        if (!new->key.start)
            xfree(new->key.start);
        xfree(new);
    } else {
        /* relay the operation to low level storage */
        err = __ns_store_write(nse, key, value, force);
        if (err < 0) {
            gk_err(mds, "namespace %.*s store write failed w/ %d\n",
                   nse->namespace.len, nse->namespace.start, err);
            /* ignore the write error */
            err = 0;
        }
    }

    return err;
}

/* Return value: follows PTR_ERR ABI
 */
struct gstring *__ns_lookup(struct ns_entry *nse, struct gstring *key)
{
    struct nsh_entry *nshe;
    struct hlist_node *pos;
    struct gstring *value = NULL;
    int idx;

    value = xzalloc(sizeof(*value));
    if (unlikely(!value)) {
        gk_err(mds, "xmalloc() gstring failed\n");
        return ERR_PTR(-ENOMEM);
    }

    idx = gk_hash_ns(key->start, key->len) % hmo.conf.ns_ht_size;
    xlock_lock(&(nse->ht + idx)->lock);
    hlist_for_each_entry(nshe, pos, &(nse->ht + idx)->h, list) {
        atomic64_inc(&hmo.prof.mds.ns_lkp_collisions);
        if ((nshe->key.len == key->len) &&
            (memcmp(nshe->key.start, key->start, 
                    min(key->len, nshe->key.len)) == 0)) {
            /* found it */
            value->start = strndup(nshe->value.start, nshe->value.len);
            value->len = nshe->value.len;
            break;
        }
    }
    xlock_unlock(&(nse->ht + idx)->lock);
    if (!value->start && !value->len) {
        int err = __ns_store_read(nse, key, value);
        if (err) {
            gk_err(mds, "__ns_store_read() failed w/ %d\n", err);
            xfree(value);
            value = ERR_PTR(err);
        }
    }
    if (unlikely(!value->start && value->len)) {
        gk_err(mds, "strdup(valule) failed\n");
        xfree(value);
        value = ERR_PTR(-ENOMEM);
    }

    return value;
}

void __ns_remove(struct ns_entry *nse, struct gstring *key)
{
    struct nsh_entry *nshe;
    struct hlist_node *pos, *n;
    int idx;
    
    if (unlikely(!key))
        return;

    idx = gk_hash_ns(key->start, key->len) % hmo.conf.ns_ht_size;
    xlock_lock(&(nse->ht + idx)->lock);
    hlist_for_each_entry_safe(nshe, pos, n, &(nse->ht + idx)->h, list) {
        if ((nshe->key.len == key->len) &&
            (memcmp(nshe->key.start, key->start, 
                    min(key->len, nshe->key.len)) == 0)) {
            hlist_del(&nshe->list);
            xfree(nshe);
        }
    }
    xlock_unlock(&(nse->ht + idx)->lock);
}

/* Return ABI: ERR_PTR
 *
 * NOTE: user should free the returned 'value'
 */
struct gstring *kvs_get(struct gstring *namespace, struct gstring *key)
{
    struct ns_entry *nse;
    struct gstring *value;

    if (unlikely(!namespace || !key || !namespace->len || !key->len)) {
        return ERR_PTR(-EINVAL);
    }
    nse = kvs_ns_lookup(namespace);
    if (unlikely(IS_ERR(nse))) {
        if (PTR_ERR(nse) == -ENOENT) {
            /* try to load the namespace? */
            char path[GK_MAX_NAME_LEN] = {0,};
            sprintf(path, "%s/%s", hmo.conf.kvs_home, namespace->start);
            if (kvs_dir_is_exist(path)) {
                nse = kvs_ns_lookup_create(namespace, NSE_F_LMDB);
                if (IS_ERR(nse)) {
                    gk_err(mds, "kvs_ns_lookup_create(%.*s) failed w/ %ld\n",
                           namespace->len, namespace->start, PTR_ERR(nse));
                } else 
                    goto do_lookup;
            }
        } else {
            gk_err(mds, "kvs_ns_lookup(%.*s) failed w/ %ld\n", 
                   namespace->len, namespace->start, PTR_ERR(nse));
        }
        value = (struct gstring *)nse;
        goto out;
    }
do_lookup:
    value = __ns_lookup(nse, key);
    if (unlikely(IS_ERR(value))) {
        gk_err(mds, "__ns_lookup(%.*s@%.*s) failed w/ %ld.\n", 
               namespace->len, namespace->start, 
               key->len, key->start, PTR_ERR(value));
        goto out_put;
    }
out_put:
    kvs_ns_put(nse);
out:
    return value;
}

int __kvs_put(struct gstring *namespace, struct gstring *key, struct gstring *value, int force)
{
    struct ns_entry *nse;
    int err;

    if (unlikely(!namespace || !key || !value || !namespace->len || !key->len || !value->len)) {
        return -EINVAL;
    }
    nse = kvs_ns_lookup_create(namespace, NSE_F_LMDB);
    if (IS_ERR(nse)) {
        gk_err(mds, "kvs_ns_lookup_create(%.*s) failed w/ %ld\n", 
               namespace->len, namespace->start, PTR_ERR(nse));
        return PTR_ERR(nse);
    }
    err = __ns_insert(nse, key, value, force);
    if (unlikely(err)) {
        if (err == -EEXIST)
            gk_debug(mds, "__ns_insert(%.*s@%.*s) failed w/ %d\n", 
                   namespace->len, namespace->start, 
                   key->len, key->start, err);
        else
            gk_err(mds, "__ns_insert(%.*s@%.*s) failed w/ %d\n", 
                   namespace->len, namespace->start, 
                   key->len, key->start, err);
        goto out_put;
    }

out_put:
    kvs_ns_put(nse);

    return err;
}

int kvs_put(struct gstring *namespace, struct gstring *key, struct gstring *value)
{
    return __kvs_put(namespace, key, value, 0);
}

int kvs_update(struct gstring *namespace, struct gstring *key, struct gstring *value)
{
    return __kvs_put(namespace, key, value, 1);
}

void kvs_ns_destroy(struct ns_entry *nse)
{
    switch (nse->type) {
    case NSE_F_MEMONLY:
        break;
    case NSE_F_LOG:
        __logf_close(nse);
        break;
    case NSE_F_LMDB:
        __lmdb_close(nse);
        break;
    }
}

void kvs_destroy(void)
{
    struct ns_entry *nse;
    struct hlist_node *pos, *n;
    time_t begin, current;
    int i, notdone, force_close = 0;

    begin = time(NULL);
    do {
        current = time(NULL);
        if (current - begin > 30) {
            gk_err(mds, "30 seconds elasped, we will close all pending files forcely.\n");
            force_close = 1;
        }
        notdone = 0;
        for (i = 0; i < hmo.conf.nshash_size; i++) {
            xrwlock_wlock(&(ns_mgr.nsht + i)->lock);
            hlist_for_each_entry_safe(nse, pos, n,
                                      &(ns_mgr.nsht + i)->h, list) {
                if (atomic_read(&nse->ref) == 0 || force_close) {
                    gk_debug(mds, "Final close fd %d.\n", nse->fd);
                    kvs_ns_destroy(nse);
                    hlist_del(&nse->list);
                    list_del(&nse->lru);
                    xfree(nse);
                    atomic_dec(&ns_mgr.active);
                } else {
                    gk_debug(mds, "Ref of idx=%d is %d\n", i, atomic_read(&nse->ref));
                    notdone = 1;
                }
            }
            xrwlock_wunlock(&(ns_mgr.nsht + i)->lock);
        }
    } while (notdone);
}
