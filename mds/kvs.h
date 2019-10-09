/**
 * Copyright (c) 2019 Ma Can <ml.macana@gmail.com>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-10-09 14:38:34 macan>
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

#ifndef __KVS_H__
#define __KVS_H__


struct namespace_mgr
{
#define MDS_KVS_NSHASH_SIZE     (512 * 1024)
    struct regular_hash_rw *nsht;
    struct list_head lru;
    xlock_t lru_lock;
    atomic_t active;            /* active ns entries */
#define NS_MGR_MEMLIMIT (2 * 1024 * 1024 * 1024)
    u64 memlimit;
};

struct gstring
{
    char *start;
    int len;
};

struct ns_log_file
{
    loff_t foffset;             /* last seek position */
};

struct ns_lmdb_file
{
#define LMDB_DEFAULT_SIZE       (10 * 1024 * 1024 * 1024UL)
    MDB_env *env;
    MDB_dbi dbi;
};

union ns_file
{
    struct ns_log_file log;
    struct ns_lmdb_file lmdb;
};

struct ns_entry
{
    struct hlist_node list;
    struct list_head lru;
    struct gstring namespace;
#define NS_HASH_SIZE    (2 * 1024 * 1024) /* for 2M hash list */
    struct regular_hash *ht;    /* hash table for this namespace */
    atomic_t ref;               /* reference for fd? */
    atomic_t nr;             /* this namespace's hash table entries */
    xlock_t lock;
    int fd;
#define NSE_F_MEMONLY   0
#define NSE_F_LOG       1
#define NSE_F_LMDB      2
    short type;
#define NSE_FREE        0
#define NSE_OPEN        1
#define NSE_LOGF        2
#define NSE_LMDB        3
    short state;

    union ns_file f;
};

struct nsh_entry
{
    struct hlist_node list;
    struct gstring key, value;
};

static inline
void kvs_ns_put(struct ns_entry *nse)
{
    atomic_dec(&nse->ref);
}

#define KVS_NAMESPACE_SIZE 32

struct kvs_storage_access
{
    struct iovec *iov;
    void *arg;
    loff_t offset;              /* < 0 means different in different
                                 * calls, refer to the function
                                 * help */
    int iov_nr;
};

/* APIs */
int kvs_init(void);
void kvs_destroy(void);
struct gstring *kvs_get(struct gstring *namespace, struct gstring *key);
int kvs_put(struct gstring *namespace, struct gstring *key, struct gstring *value);
int kvs_update(struct gstring *namespace, struct gstring *key, struct gstring *value);

#endif
