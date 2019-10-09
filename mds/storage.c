/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-24 11:45:23 macan>
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

struct deleted_dir_entry
{
    struct list_head list;
    u64 uuid;
};

struct deleted_dirs
{
    struct list_head dd;
    struct deleted_dir_entry *cur;
    xlock_t lock;
};

static struct deleted_dirs g_dd = {
    .cur = NULL,
};

void mds_storage_deleted_dir(u64 duuid)
{
    struct deleted_dir_entry *dde;
    int found = 0;
    
    /* check if this entry has already exist */
    xlock_lock(&g_dd.lock);
    list_for_each_entry(dde, &g_dd.dd, list) {
        if (duuid == dde->uuid) {
            found = 1;
            break;
        }
    }
    xlock_unlock(&g_dd.lock);
    if (found)
        return;
    
    dde = xmalloc(sizeof(*dde));
    if (!dde) {
        gk_err(mds, "Lost deleted dir %lx, memory leaks\n", duuid);
        return;
    }
    INIT_LIST_HEAD(&dde->list);
    dde->uuid = duuid;
    xlock_lock(&g_dd.lock);
    list_add_tail(&dde->list, &g_dd.dd);
    xlock_unlock(&g_dd.lock);
    if (!g_dd.cur)
        g_dd.cur = dde;
}

/* Return value: -1UL means nothing got
 */
u64 mds_storage_get_next_deleted_dir(void)
{
    u64 ret = -1UL;

    if (list_empty(&g_dd.dd)) {
        return ret;
    }
    xlock_lock(&g_dd.lock);
    ret = g_dd.cur->uuid;
    if (g_dd.cur->list.next == &g_dd.dd) {
        /* I am the tail, jump to the head */
        g_dd.cur = list_entry(g_dd.dd.next, struct deleted_dir_entry, list);
    } else {
        g_dd.cur = list_entry(g_dd.cur->list.next, struct deleted_dir_entry,
                              list);
    }
    xlock_unlock(&g_dd.lock);

    return ret;
}

/* append_buf_create()
 *
 * @state: fde state w/ OPEN/READ/WRITE
 */
int append_buf_create(struct fdhash_entry *fde, char *name, int state)
{
    size_t buf_len;
    int err = 0;
    
    if (state == FDE_FREE)
        return 0;
    
    if (fde->type == MDS_STORAGE_ITB) {
        buf_len = hmo.conf.itb_file_chunk;
    } else if (fde->type == MDS_STORAGE_DATA) {
        buf_len = hmo.conf.data_file_chunk;
    } else {
        buf_len = MDS_STORAGE_DEFAULT_CHUNK;
    }

    xlock_lock(&fde->lock);
    if (!hmo.conf.itb_falloc) {
        hmo.conf.itb_falloc = CPU_CORE; /* should be the number of cores of this
                                         * machine? */
        ASSERT(CPU_CORE != 0, mds);
    }
    fde->abuf.falloc_size = hmo.conf.itb_falloc * buf_len;
    
    if (fde->state == FDE_FREE) {
        /* ok, we should open it */
        fde->fd = open(name, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fde->fd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                     name, strerror(errno), -errno);
            xlock_unlock(&fde->lock);

            return -errno;
        }
        gk_warning(mds, "open file %s w/ fd %d\n", name, fde->fd);
        fde->state = FDE_OPEN;
    }
    if (state == FDE_ABUF && fde->state == FDE_OPEN) {
        /* get the file end offset */
        fde->abuf.file_offset = lseek(fde->fd, 0, SEEK_END);
        if (fde->abuf.file_offset < 0) {
            gk_err(mds, "lseek to end of file %s failed w/ %d\n", 
                     name, errno);
            err = -errno;
            goto out_close;
        }
        fde->abuf.falloc_offset = fde->abuf.file_offset;
        err = ftruncate(fde->fd, fde->abuf.falloc_offset + fde->abuf.falloc_size);
        if (err) {
            gk_err(mds, "ftruncate file %s failed w/ %d\n",
                     name, err);
            goto out_close;
        }
        /* we create the append buf now */
        fde->abuf.addr = mmap(NULL, buf_len, PROT_WRITE | PROT_READ, 
                              MAP_SHARED, fde->fd, fde->abuf.file_offset);
        if (fde->abuf.addr == MAP_FAILED) {
            gk_err(mds, "mmap file %s in region [%ld,%ld] failed w/ %d\n",
                     name, fde->abuf.file_offset, 
                     fde->abuf.file_offset + buf_len, errno);
            err = -errno;
            goto out_close;
        }
        fde->abuf.len = buf_len;
        if (fde->type == MDS_STORAGE_ITB) {
            if (!fde->abuf.file_offset)
                fde->abuf.offset = 1;
        } else {
            fde->abuf.offset = 0;
        }
        fde->state = FDE_ABUF;
        atomic64_add(buf_len, &hmo.storage.memcache);
    }

    xlock_unlock(&fde->lock);

    return err;
out_close:
    xlock_unlock(&fde->lock);
    /* close the file */
    close(fde->fd);
    fde->state = FDE_FREE;

    return err;
}

/* append_buf_flush()
 *
 * Note: holding the fde->lock
 * Return Value: 0 for no error, 1 for fallback, <0 for error.
 */
static inline
int append_buf_flush(struct fdhash_entry *fde, int flag)
{
    int err = 0;

    if (fde->state == FDE_ABUF) {
        if (flag & ABUF_ASYNC) {
            err = mds_aio_submit_request(fde->abuf.addr, fde->abuf.offset,
                                          fde->abuf.len, fde->abuf.file_offset, 
                                          flag, fde->fd);
            if (err) {
                gk_err(mds, "Submit AIO async request failed w/ %d\n", err);
                err = 1;
                goto fallback;
            }
            gk_info(mds, "ASYNC FLUSH offset %lx %p fd %d, submitted.\n", 
                      fde->abuf.file_offset, fde->abuf.addr, fde->fd);
        } else {
        fallback:
            err = msync(fde->abuf.addr, fde->abuf.offset, MS_SYNC);
            if (err < 0) {
                gk_err(mds, "msync() fd %d failed w/ %d\n", fde->fd, errno);
                err = -errno;
                goto out;
            }
            atomic64_add(fde->abuf.offset, &hmo.prof.storage.wbytes);
            posix_madvise(fde->abuf.addr, fde->abuf.len,
                          POSIX_MADV_DONTNEED);
            posix_fadvise(fde->fd, fde->abuf.file_offset,
                          fde->abuf.len, POSIX_FADV_DONTNEED);
            if (flag & ABUF_UNMAP) {
                err = munmap(fde->abuf.addr, fde->abuf.len);
                if (err) {
                    gk_err(mds, "munmap() fd %d faield w/ %d\n", 
                             fde->fd, errno);
                    err = -errno;
                }
            }
            gk_debug(mds, "sync flush fd %d offset %lx\n", 
                       fde->fd, fde->abuf.file_offset);
        }
        if (flag & ABUF_UNMAP)
            fde->state = FDE_ABUF_UNMAPPED;
    }
out:
    return err;
}

void append_buf_destroy(struct fdhash_entry *fde)
{
    int err;
    
    /* munmap the region */
    if (fde->state == FDE_ABUF) {
        gk_debug(mds, "begin destroy SYNC on fd %d.\n", 
                   fde->fd);
        append_buf_flush(fde, ABUF_SYNC);
        gk_debug(mds, "end destroy SYNC on fd %d.\n",
                   fde->fd);
        err = munmap(fde->abuf.addr, fde->abuf.len);
        if (err) {
            gk_err(mds, "munmap fd %d failed w/ %d\n", 
                     fde->fd, err);
        }
        /* close the backend file and truncate the file length (page
         * boundary) */
        err = ftruncate(fde->fd, PAGE_ROUNDUP(fde->abuf.file_offset +
                                              fde->abuf.offset,
                                              getpagesize()));
        if (err) {
            gk_err(mds, "ftruncate fd %d failed w/ %d\n",
                     fde->fd, err);
        }
        atomic64_add(-fde->abuf.len, &hmo.storage.memcache);
    }
    memset(&fde->abuf, 0, sizeof(fde->abuf));
    fde->state = FDE_OPEN;
}

/* Returan value: 1: truely issue request; 0: no request issued
 */
int append_buf_destroy_async(struct fdhash_entry *fde)
{
    int res = 0;
    
    if (fde->state == FDE_ABUF) {
        append_buf_flush(fde, ABUF_ASYNC | ABUF_UNMAP | ABUF_TRUNC);
        res = 1;
    }

    memset(&fde->abuf, 0, sizeof(fde->abuf));
    fde->state = FDE_OPEN;

    return res;
}

/* Return value: 0: truely drop (or no request issued); 1: actually written */
int append_buf_destroy_drop(struct fdhash_entry *fde)
{
    char path[GK_MAX_NAME_LEN] = {0, };
    int err = 0;
    
#ifdef MDS_RADICAL_DEL
    if (1) {
#else
    if (hmo.conf.option & GK_MDS_RADICAL_DEL) {
#endif
        posix_madvise(fde->abuf.addr, fde->abuf.len,
                      POSIX_MADV_DONTNEED);
        posix_fadvise(fde->fd, fde->abuf.file_offset,
                      fde->abuf.len, POSIX_FADV_DONTNEED);
        if (fde->state == FDE_ABUF) {
            err = munmap(fde->abuf.addr, fde->abuf.len);
            if (err) {
                gk_err(mds, "munmap() fd %d failed w/ %d\n",
                         fde->fd, errno);
                err = -errno;
                goto out;
            }
            atomic64_add(-fde->abuf.len, &hmo.storage.memcache);
        }
        /* unlink the file to drop the page cache! ignore errors */
        if (fde->type == MDS_STORAGE_DATA) {
            sprintf(path, "%s/%lx/%lx/data-%ld", hmo.conf.mds_home,
                    hmo.site_id, fde->uuid, fde->arg);
            if (unlink(path) < 0) {
                /* calculate the written length */
                atomic64_add(fde->abuf.len, &hmo.prof.storage.wbytes);
            }
            gk_warning(mds, "ABuf: unlink file '%s'\n", path);
        }
        close(fde->fd);
    } else {
        return append_buf_destroy_async(fde);
    }
out:
    return err;
}

/*
 * Note: holding the fde->lock please
 */
int append_buf_flush_trunc(struct fdhash_entry *fde, u64 offset) 
{
    int err = 0;

    err = append_buf_flush(fde, ABUF_SYNC | ABUF_UNMAP);
    if (err) {
        gk_err(mds, "ABUF flush failed w/ %d\n", err);
        goto out;
    }

    /* The CALLER must be sure that we are the ONLY owner for this file, and
     * we try to adjust the offset now */
    switch (fde->state) {
    case FDE_ABUF_UNMAPPED:
        fde->abuf.falloc_offset = 
            fde->abuf.file_offset = PAGE_ROUNDUP(offset, getpagesize());
        err = ftruncate(fde->fd, fde->abuf.falloc_offset +
                        (fde->abuf.falloc_size << 1));
        if (err) {
            gk_err(mds, "ftruncate fd %d failed w/ %d\n",
                     fde->fd, err);
            goto out;
        }
        fde->abuf.falloc_offset += fde->abuf.falloc_size;
        gk_warning(mds, "ftruncate offset %lx len %ld\n",
                     fde->abuf.falloc_offset, fde->abuf.falloc_size);
        mds_aio_start();
        fde->abuf.addr = mmap(NULL, fde->abuf.len, PROT_WRITE | PROT_READ,
                              MAP_SHARED, fde->fd, fde->abuf.file_offset);
        if (fde->abuf.addr == MAP_FAILED) {
            gk_err(mds, "mmap fd %d in ragion [%ld,%ld] failed w/ %d\n",
                     fde->fd, fde->abuf.file_offset,
                     fde->abuf.file_offset + fde->abuf.len, errno);
            err = -errno;
            goto out;
        }
        fde->state = FDE_ABUF;
        fde->abuf.offset = 0;
        break;
    default:
        gk_err(mds, "Invalid FDE state %d\n", fde->state);
        err = -EINVAL;
        goto out;
    }

out:
    return err;
}
    
/*
 * Note: holding the fde->lock
 */
int append_buf_flush_remap(struct fdhash_entry *fde)
{
#ifdef MDS_ACC_SYNC
    int err = ABUF_ASYNC | ABUF_UNMAP | ABUF_XSYNC;

    if (fde->abuf.acclen >= aio_sync_length()) {
        err = ABUF_ASYNC | ABUF_UNMAP;
        fde->abuf.acclen = 0;
    }
#else
    int err = ABUF_ASYNC | ABUF_UNMAP;
    
#endif
    err = append_buf_flush(fde, err);
    if (err) {
        gk_err(mds, "ABUF flush failed w/ %d\n", err);
        goto out;
    }

    /* we try to remap another region now */
    switch (fde->state) {
    case FDE_ABUF:
        err = munmap(fde->abuf.addr, fde->abuf.len);
        if (err == -1) {
            gk_err(mds, "munmap ABUF failed w/ %d\n", errno);
            err = -errno;
            goto out;
        }
        fde->state = FDE_ABUF_UNMAPPED;
    case FDE_ABUF_UNMAPPED:
        fde->abuf.file_offset += fde->abuf.len;
        if (fde->abuf.file_offset + fde->abuf.len > fde->abuf.falloc_offset + 
            fde->abuf.falloc_size) {
            err = ftruncate(fde->fd, fde->abuf.falloc_offset + 
                            (fde->abuf.falloc_size << 1));
            if (err) {
                gk_err(mds, "ftruncate fd %d failed w/ %d\n",
                         fde->fd, err);
                goto out;
            }
            fde->abuf.falloc_offset += fde->abuf.falloc_size;
            gk_warning(mds, "ftruncate offset %lx len %ld\n", 
                         fde->abuf.falloc_offset, fde->abuf.falloc_size);
        }
        mds_aio_start();
        fde->abuf.addr = mmap(NULL, fde->abuf.len, PROT_WRITE | PROT_READ,
                              MAP_SHARED, fde->fd, fde->abuf.file_offset);
        if (fde->abuf.addr == MAP_FAILED) {
            gk_err(mds, "mmap fd %d in region [%ld,%ld] failed w/ %d\n",
                     fde->fd, fde->abuf.file_offset,
                     fde->abuf.file_offset + fde->abuf.len, errno);
            err = -errno;
            goto out;
        }
        fde->state = FDE_ABUF;
        fde->abuf.offset = 0;
        break;
    default:
        gk_err(mds, "ABUF flush remap w/ other state %x\n",
                 fde->state);
        err = -EINVAL;
    }

out:
    return err;
}

int append_buf_write(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    void *base;
    size_t len, wlen;
    off_t woffset = 0;
    int err = 0;

    if (!msa->iov_nr || !msa->iov || fde->state == FDE_FREE ||
        msa->iov->iov_len < 0) {
        return -EINVAL;
    }

    if (msa->iov_nr != 1) {
        gk_err(mds, "Append buffer do not support vary IOV write.\n");
        return -EINVAL;
    }

    base = msa->iov->iov_base;
    len = msa->iov->iov_len;
    
    xlock_lock(&fde->lock);
    /* check the remained length of the Append Buffer */
    do {
        wlen = min(fde->abuf.len - fde->abuf.offset, len);

        /* ok, we can copy the region to the abuf now */
        memcpy(fde->abuf.addr + fde->abuf.offset, 
               base + woffset, wlen);

        if (!woffset) {
            if (fde->type == MDS_STORAGE_ITB) {
                ((struct itb_info *)msa->arg)->location = 
                    fde->abuf.file_offset + fde->abuf.offset;
            } else if (fde->type == MDS_STORAGE_DATA) {
                *((u64 *)msa->arg) = fde->abuf.file_offset +
                    fde->abuf.offset;
            } else {
                gk_err(mds, "WHAT type? fde %d abuf (%d,%ld,%ld) @ L %ld\n",
                         fde->fd, fde->type, fde->abuf.file_offset,
                         fde->abuf.offset, msa->iov->iov_len);
            }
        }
        
        len -= wlen;
        woffset += wlen;
        fde->abuf.offset += wlen;
        atomic64_add(wlen, &hmo.prof.storage.cpbytes);
        
        if (fde->abuf.offset >= fde->abuf.len) {
            /* we should mmap another region */
            err = append_buf_flush_remap(fde);
            if (err) {
                /* FIXME: this might be a DATA CORRUPTION */
                gk_err(mds, "ABUFF flush remap failed w/ %d\n", err);
                goto out_unlock;
            }
        }
    } while (len > 0);
    fde->abuf.acclen += msa->iov->iov_len;

out_unlock:
    xlock_unlock(&fde->lock);
    
    return err;
}

int odirect_create(struct fdhash_entry *fde, char *name, int state)
{
    size_t buf_len;
    int err = 0;

    if (state == FDE_FREE)
        return 0;

    if (fde->type == MDS_STORAGE_ITB ||
        fde->type == MDS_STORAGE_ITB_ODIRECT) {
        buf_len = hmo.conf.itb_file_chunk;
    } else if (fde->type == MDS_STORAGE_DATA) {
        buf_len = hmo.conf.data_file_chunk;
    } else {
        buf_len = MDS_STORAGE_DEFAULT_CHUNK;
    }


    xlock_lock(&fde->lock);

    if (fde->state == FDE_FREE) {
        /* ok, we should open it */
        fde->odirect.wfd = open(name, O_RDWR | O_CREAT | O_DIRECT, 
                                S_IRUSR | S_IWUSR);
        if (fde->odirect.wfd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                     name, strerror(errno), -errno);
            xlock_unlock(&fde->lock);
            return -errno;
        }
        gk_err(mds, "open file %s w/ fd %d\n", name, fde->odirect.wfd);
        fde->odirect.rfd = open(name, O_RDONLY | O_CREAT,
                                S_IRUSR | S_IWUSR);
        if (fde->odirect.rfd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                     name, strerror(errno), -errno);
            close(fde->odirect.wfd);
            xlock_unlock(&fde->lock);
            return -errno;
        }
        fde->state = FDE_OPEN;
        fde->fd = fde->odirect.rfd;
    }
    if (state == FDE_ODIRECT && fde->state == FDE_OPEN) {
        /* get the file end offset */
        fde->odirect.file_offset = lseek(fde->odirect.wfd, 0, SEEK_END);
        if (fde->odirect.file_offset < 0) {
            gk_err(mds, "lseek to end of file %s failed w/ %d\n",
                     name, errno);
            err = -errno;
            goto out_close;
        }
        DISK_SEC_ROUND_UP(fde->odirect.file_offset);
        /* we create the buffer now */
        err = posix_memalign(&fde->odirect.addr, DISK_SEC, buf_len);
        if (err) {
            gk_err(mds, "posix memalign buffer region failed w/ %d\n",
                     err);
            err = -err;
            goto out_close;
        }
        fde->odirect.len = buf_len;
        if (!fde->odirect.file_offset)
            fde->odirect.offset = 1;
        fde->state = FDE_ODIRECT;
    }

    xlock_unlock(&fde->lock);

    return err;
out_close:
    xlock_unlock(&fde->lock);
    /* close the file */
    if (fde->odirect.wfd)
        close(fde->odirect.wfd);
    if (fde->odirect.rfd)
        close(fde->odirect.rfd);
    fde->state = FDE_FREE;

    return err;
}

int odirect_flush_reget(struct fdhash_entry *fde)
{
    int err = 0;
    
    /* we should submit the io request to the aio threads and do async io */
    err = mds_aio_submit_request(fde->odirect.addr, fde->odirect.offset,
                                  fde->odirect.len, fde->odirect.file_offset,
                                  MDS_AIO_ODIRECT, fde->odirect.wfd);
    if (err) {
        gk_err(mds, "Submit ODIRECT AIO request failed w/ %d\n", err);
        return err;
    }
    gk_info(mds, "ASYNC ODIRECT offset %lx %p, submitted.\n",
              fde->odirect.file_offset, fde->odirect.addr);

    /* release the resource and re-init the odirect buffer */
    fde->odirect.file_offset += fde->odirect.len;
    fde->odirect.offset = 0;
    err = posix_memalign(&fde->odirect.addr, DISK_SEC, fde->odirect.len);
    if (err) {
        gk_err(mds, "posix memalign buffer region failed w/ %d\n", err);
        err = -err;
        goto out;
    }
    mds_aio_start();
out:
    return err;
}

int odirect_write(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    void *base;
    size_t len, wlen;
    off_t woffset = 0;
    int err = 0;

    if (!msa->iov_nr || !msa->iov || fde->state == FDE_FREE) {
        return -EINVAL;
    }
    if (msa->iov_nr != 1) {
        gk_err(mds, "Odirect do not support vary IOV write.\n");
        return -EINVAL;
    }

    base = msa->iov->iov_base;
    len = msa->iov->iov_len;
    
    xlock_lock(&fde->lock);
    /* check the remained length of the ODIRECT buffer */
    do {
        wlen = min(fde->abuf.len - fde->abuf.offset, len);

        /* ok, we can copy the region to the odirect buffer now */
        memcpy(fde->odirect.addr + fde->odirect.offset, 
               base + woffset, wlen);

        if (!woffset) {
            if (fde->type == MDS_STORAGE_ITB_ODIRECT) {
                ((struct itb_info *)msa->arg)->location = 
                    fde->odirect.file_offset + fde->odirect.offset;
            }
        }
        len -= wlen;
        woffset += wlen;
        fde->odirect.offset += wlen;
        atomic64_add(wlen, &hmo.prof.storage.cpbytes);

        if (fde->odirect.offset >= fde->odirect.len) {
            /* we should submit the region to disk */
            err = odirect_flush_reget(fde);
            if (err) {
                gk_err(mds, "odirect region flush remap failed w/ %d\n", 
                         err);
                goto out_unlock;
            }
        }
    } while (len > 0);

out_unlock:
    xlock_unlock(&fde->lock);

    return err;
}

int __normal_read(struct fdhash_entry *fde, struct mds_storage_access *msa);
int odirect_read(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    u64 offset;
    int err = 0;

    if (fde->state < FDE_OPEN || !msa->iov_nr || !msa->iov)
        return -EINVAL;

    offset = msa->offset;
    /* check whether this request can be handled in the odirect buffer */
    if (offset < fde->odirect.file_offset) {
        /* this request should be handled by the normal read. But, the normal
         * read maybe failed to retrieve the correct result! Because the
         * corresponding aio writing is going on. */
        return __normal_read(fde, msa);
    } else if (offset < fde->odirect.file_offset + fde->odirect.len) {
        /* this request may be handled by the odirect buffer */
        /* Step 1: copy the data now */
        memcpy(msa->iov->iov_base, 
               fde->odirect.addr + offset - fde->odirect.file_offset,
               msa->iov->iov_len);
        /* Step 2: recheck */
        if (offset < fde->odirect.file_offset) {
            /* oh, we should fallback to the normal read */
            return __normal_read(fde, msa);
        }
    } else
        return __normal_read(fde, msa);

    return err;
}

void odirect_destroy(struct fdhash_entry *fde)
{
    int err;
    
    if (fde->state == FDE_ODIRECT) {
        gk_err(mds, "begin odirect destroy SYNC on fd %d.\n",
                 fde->fd);
        err = mds_aio_submit_request(fde->odirect.addr, fde->odirect.offset,
                                      fde->odirect.len, fde->odirect.file_offset,
                                      MDS_AIO_ODIRECT, fde->odirect.wfd);
        if (err) {
            gk_err(mds, "Submit final ODIRECT aio request failed w/ %d\n", err);
        } else {
            mds_aio_start();
        }
        gk_err(mds, "end odirect destroy SYNC on fd %d.\n",
                 fde->fd);
    }
    memset(&fde->odirect, 0, sizeof(fde->odirect));
    fde->state = FDE_OPEN;
}

int mds_storage_init(void)
{
    char path[256] = {0, };
    int err = 0;
    int i;

    /* init the deleted dir list */
    INIT_LIST_HEAD(&g_dd.dd);
    xlock_init(&g_dd.lock);
    
    /* set the fd limit firstly */
    struct rlimit rli = {
        .rlim_cur = 65536,
        .rlim_max = 70000,
    };
    err = setrlimit(RLIMIT_NOFILE, &rli);
    if (err) {
        gk_err(xnet, "setrlimit failed w/ %s\n", strerror(errno));
        gk_warning(xnet, "%sStorage Server has FD limit! To overcome "
                     "this limit, please use a powerful UID to run this"
                     " process.%s\n", 
                     GK_COLOR_RED, GK_COLOR_END);
    }

    if (!hmo.conf.storage_fdhash_size) {
        hmo.conf.storage_fdhash_size = MDS_STORAGE_FDHASH_SIZE;
    }

    hmo.storage.fdhash = xmalloc(hmo.conf.storage_fdhash_size *
                                 sizeof(struct regular_hash));
    if (!hmo.storage.fdhash) {
        gk_err(mds, "alloc fd hash table failed.\n");
        return -ENOMEM;
    }

    /* init the hash table */
    for (i = 0; i < hmo.conf.storage_fdhash_size; i++) {
        INIT_HLIST_HEAD(&(hmo.storage.fdhash + i)->h);
        xlock_init(&(hmo.storage.fdhash + i)->lock);
    }
    INIT_LIST_HEAD(&hmo.storage.lru);
    xlock_init(&hmo.storage.lru_lock);
    xlock_init(&hmo.storage.txg_fd_lock);
    xlock_init(&hmo.storage.tmp_fd_lock);

    /* init the global fds */
    err = mds_storage_dir_make_exist(hmo.conf.mds_home);
    if (err) {
        gk_err(mds, "dir %s do not exist %d.\n", path, err);
        return -ENOTEXIST;
    }

    /* check the MDS site directory */
    sprintf(path, "%s/%lx", hmo.conf.mds_home, hmo.site_id);
    err = mds_storage_dir_make_exist(path);
    if (err) {
        gk_err(mds, "dir %s do not exist.\n", path);
        return -ENOTEXIST;
    }

    /* open the txg file */
    sprintf(path, "%s/%lx/txg", hmo.conf.mds_home, hmo.site_id);
    hmo.storage.txg_fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (hmo.storage.txg_fd < 0) {
        gk_err(mds, "open file '%s' faield w/ %d\n", path, errno);
        return -errno;
    }

    /* check if we should set recovery flag */
    err = mds_txg_integrated();
    if (err) {
        hmo.aux_state = HMO_AUX_STATE_RECOVERY;
        gk_warning(mds, "mds_txg_integrated() failed w/ %d, "
                     "change state to RECOVERY\n",
                     err);
    }
    gk_info(mds, "MDS %lx log recovery return %s\n",
              hmo.site_id,
              (err == 0 ? "CLEAN" : "?DIRTY?"));
    
    return 0;
}

void mds_storage_destroy(void)
{
    struct fdhash_entry *fde;
    struct hlist_node *pos, *n;
    time_t begin, current;
    int i, notdone, force_close = 0;

    begin = time(NULL);
    do {
        current = time(NULL);
        if (current - begin > 30) {
            gk_err(mds, "30 seconds passed, we will close all pending "
                     "fils forcely.\n");
            force_close = 1;
        }
        notdone = 0;
        for (i = 0; i < hmo.conf.storage_fdhash_size; i++) {
            xlock_lock(&(hmo.storage.fdhash + i)->lock);
            hlist_for_each_entry_safe(fde, pos, n, 
                                      &(hmo.storage.fdhash + i)->h, list) {
                if (atomic_read(&fde->ref) == 0 || force_close) {
                    gk_debug(mds, "Final close fd %d.\n", fde->fd);
                    if (fde->type == MDS_STORAGE_ITB ||
                        fde->type == MDS_STORAGE_DATA) {
                        append_buf_destroy(fde);
                    } else if (fde->type == MDS_STORAGE_ITB_ODIRECT) {
                        odirect_destroy(fde);
                    } else if (fde->type == MDS_STORAGE_MD) {
                        __mdisk_write(fde, NULL);
                    } else
                        fsync(fde->fd);
                    close(fde->fd);
                    hlist_del(&fde->list);
                    list_del(&fde->lru);
                    if (fde->state == FDE_NORMAL)
                        xfree((void *)fde->arg);
                    xfree(fde);
                    atomic_dec(&hmo.storage.active);
                } else {
                    notdone = 1;
                }
            }
            xlock_unlock(&(hmo.storage.fdhash + i)->lock);
        }
    } while (notdone);
}


static inline
void mds_storage_fd_lru_update(struct fdhash_entry *fde)
{
    xlock_lock(&hmo.storage.lru_lock);
    list_del_init(&fde->lru);
    list_add(&fde->lru, &hmo.storage.lru);
    xlock_unlock(&hmo.storage.lru_lock);
}

static inline
struct fdhash_entry *mds_storage_fd_lookup(u64 duuid, int ftype, u64 arg)
{
    struct fdhash_entry *fde;
    struct hlist_node *pos;
    int idx;
    
    idx = gk_hash_fdht(duuid, ftype) % hmo.conf.storage_fdhash_size;
    xlock_lock(&(hmo.storage.fdhash + idx)->lock);
    hlist_for_each_entry(fde, pos, &(hmo.storage.fdhash + idx)->h, list) {
        if (duuid == fde->uuid && ftype == fde->type) {
            if (ftype == MDS_STORAGE_RANGE) {
                struct mmap_args *ma1 = (struct mmap_args *)arg;

                if (ma1->range_id == fde->mwin.arg && 
                    ma1->flag == fde->mwin.flag) {
                    atomic_inc(&fde->ref);
                    xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
                    /* lru update */
                    mds_storage_fd_lru_update(fde);
                    return fde;
                }
            } else if (ftype == MDS_STORAGE_NORMAL) {
                struct proxy_args *pa0, *pa1;

                pa0 = (struct proxy_args *)arg;
                pa1 = (struct proxy_args *)fde->arg;

                if (pa0->uuid == pa1->uuid && pa0->cno == pa1->cno) {
                    atomic_inc(&fde->ref);
                    xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
                    /* lru update */
                    mds_storage_fd_lru_update(fde);
                    return fde;
                }
            } else if (arg == fde->arg) {
                atomic_inc(&fde->ref);
                xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
                /* lru update */
                mds_storage_fd_lru_update(fde);
                return fde;
            }
        }
    }
    xlock_unlock(&(hmo.storage.fdhash + idx)->lock);

    return ERR_PTR(-EINVAL);
}

/* At this moment, we only support lockup the md file. Other ftype we will
 * return -EINVAL immediately.
 */
int mds_storage_fd_lockup(struct fdhash_entry *fde)
{
    int err = 0;

    if ((fde->type != MDS_STORAGE_MD &&
         fde->type != MDS_STORAGE_DATA) 
        || fde->state == FDE_OPEN)
        return -EINVAL;
    
    xcond_lock(&fde->cond);
    if (fde->type == MDS_STORAGE_MD && fde->state == FDE_MDISK)
        fde->state = FDE_LOCKED;
    else if (fde->type == MDS_STORAGE_DATA && 
             fde->aux_state != FDE_AUX_LOCKED)
        fde->aux_state = FDE_AUX_LOCKED;
    else
        err = -ELOCKED;
    xcond_unlock(&fde->cond);

    if (!err) {
        if (fde->state == FDE_LOCKED ||
            fde->aux_state == FDE_AUX_LOCKED)
            err = 0;
        else
            err = -EFAULT;
    }
    if (!err) {
        /* wait for the active references, then nobody can hold unsafe
         * reference */
        while (atomic_read(&fde->ref) > 1) {
            xsleep(1000);   /* wait 1ms */
        }
    }

    return err;
}

int mds_storage_fd_unlock(struct fdhash_entry *fde)
{
    int err = 0;

    if (fde->type != MDS_STORAGE_MD &&
        fde->type != MDS_STORAGE_DATA)
        return -EINVAL;

    xcond_lock(&fde->cond);
    if (fde->type == MDS_STORAGE_MD && fde->state == FDE_LOCKED)
        fde->state = FDE_MDISK;
    if (fde->type == MDS_STORAGE_DATA && 
        fde->aux_state == FDE_AUX_LOCKED)
        fde->aux_state = FDE_AUX_FREE;
    xcond_broadcast(&fde->cond);
    xcond_unlock(&fde->cond);

    return err;
}

struct fdhash_entry *mds_storage_fd_insert(struct fdhash_entry *new)
{
    struct fdhash_entry *fde;
    struct hlist_node *pos;
    int idx, found = 0;

    idx = gk_hash_fdht(new->uuid, new->type) % hmo.conf.storage_fdhash_size;
    xlock_lock(&(hmo.storage.fdhash + idx)->lock);
    hlist_for_each_entry(fde, pos, &(hmo.storage.fdhash + idx)->h, list) {
        if (new->uuid == fde->uuid && new->type == fde->type) {
            if (new->type == MDS_STORAGE_RANGE) {
                struct mmap_args *ma1 = (struct mmap_args *)new->arg;

                if (ma1->range_id == fde->mwin.arg &&
                    ma1->flag == fde->mwin.flag) {
                    atomic_inc(&fde->ref);
                    found = 1;
                    break;
                }
            } else if (new->type == MDS_STORAGE_NORMAL) {
                struct proxy_args *pa0, *pa1;

                pa0 = (struct proxy_args *)new->arg;
                pa1 = (struct proxy_args *)fde->arg;

                if (pa0->uuid == pa1->uuid && pa0->cno == pa1->cno) {
                    atomic_inc(&fde->ref);
                    found = 1;
                    break;
                }
            } else if (new->arg == fde->arg) {
                atomic_inc(&fde->ref);
                found = 1;
                break;
            }
        }
    }
    if (!found) {
        hlist_add_head(&new->list, &(hmo.storage.fdhash + idx)->h);
        atomic_inc(&hmo.storage.active);
    }
    xlock_unlock(&(hmo.storage.fdhash + idx)->lock);


    if (found) {
        /* lru update */
        mds_storage_fd_lru_update(fde);
        return fde;
    } else {
        /* lru update */
        mds_storage_fd_lru_update(new);
        return new;
    }
}

void mds_storage_fd_remove(struct fdhash_entry *new)
{
    int idx;

    idx = gk_hash_fdht(new->uuid, new->type) % hmo.conf.storage_fdhash_size;
    xlock_lock(&(hmo.storage.fdhash + idx)->lock);
    hlist_del_init(&new->list);
    xlock_lock(&hmo.storage.lru_lock);
    /* race with mds_storage_fd_limit_check() */
    if (list_empty(&new->lru)) {
        xlock_unlock(&hmo.storage.lru_lock);
        xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
        return;
    }
    list_del_init(&new->lru);
    xlock_unlock(&hmo.storage.lru_lock);
    xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
    atomic_dec(&hmo.storage.active);
}

int mds_storage_low_load(time_t cur)
{
    static u64 last_wbytes = 0;
    static u64 last_req = 0;
    static time_t last_probe = 0;
    static int shift = 0;       /* shift for fd_cleanup_N */

    static u64 wbytes = 0, rbytes = 0;
    static time_t old = 0;
    u64 this_wbytes, this_req;
    int res = 0;

    /* update the peace counter */
    if (cur > old) {
        if ((atomic64_read(&hmo.prof.storage.wbytes) > wbytes) ||  
            (atomic64_read(&hmo.prof.storage.rbytes) > rbytes)) {
            wbytes = atomic64_read(&hmo.prof.storage.wbytes);
            rbytes = atomic64_read(&hmo.prof.storage.rbytes);
            atomic_set(&hmo.storage.peace, 0);
        } else {
            atomic_inc(&hmo.storage.peace);
        }
        old = cur;
    }
    
    /* we recompute the wbytes rate every 10 seconds */
    if (!last_probe)
        last_probe = cur;
    if (cur - last_probe >= 10) {
        /* rate < 1MB/10s && wreq + rreq < 30/10 */
        this_wbytes = atomic64_read(&hmo.prof.storage.wbytes);
        this_req = atomic64_read(&hmo.prof.storage.wreq) +
            atomic64_read(&hmo.prof.storage.rreq);
        if (this_wbytes - last_wbytes < (hmo.conf.disk_low_load) &&
            this_req - last_req < 30) {
            res = 1;
            /* if there is no I/O, we should not adjust the cleanup_N :) */
            if (this_wbytes > last_wbytes) {
                /* we are gentel man, thus, only 8 times of original
                 * cleanup_N */
                if (shift < 3) {
                    hmo.conf.fd_cleanup_N = hmo.conf.fd_cleanup_N << 1;
                    ++shift;
                }
            }
        } else {
            if (shift > 0) {
                hmo.conf.fd_cleanup_N = hmo.conf.fd_cleanup_N >> shift;
                shift = 0;
            }
        }
        last_probe = cur;
        last_wbytes = this_wbytes;
        last_req = this_req;
    }

    return res;
}

void mds_storage_fd_limit_check(time_t cur)
{
    struct fdhash_entry *fde;
    u64 duuid;
    int idx, remove, i = 0;

    /* Step 1: check if we can free some fde entries */
    duuid = mds_storage_get_next_deleted_dir();
    if (duuid != -1UL) {
        int err = mds_storage_clean_dir(duuid);
        if (err) {
            gk_warning(mds, "storage clean dir %lx failed w/ %d\n",
                         duuid, err);
        }
    }
    
    /* Step 2: check the memcache */
    if (atomic64_read(&hmo.storage.memcache) > hmo.conf.mclimit ||
        mds_storage_low_load(cur)) {
        while (i++ < hmo.conf.fd_cleanup_N) {
            /* check if the opened fdes are not accessed for a long time, for
             * example 30s */
            if (atomic_read(&hmo.storage.peace) > 30) {
                /* if it is, we try to do some evicts without respect to
                 * fdlimit */
                goto do_evict;
            }
            if (atomic_read(&hmo.storage.active) <= hmo.conf.fdlimit)
                break;
        do_evict:
            /* try to evict some fd entry now */
            xlock_lock(&hmo.storage.lru_lock);
            if (list_empty(&hmo.storage.lru)) {
                xlock_unlock(&hmo.storage.lru_lock);
                return;
            }
            fde = list_entry(hmo.storage.lru.prev, struct fdhash_entry, lru);
            /* check if this fde is referenced */
            if (atomic_read(&fde->ref) > 0) {
                xlock_unlock(&hmo.storage.lru_lock);
                continue;
            }
            list_del_init(&fde->lru);
            xlock_unlock(&hmo.storage.lru_lock);
            /* do destroy on the file w/ a lock */
            idx = gk_hash_fdht(fde->uuid, fde->type) % 
                hmo.conf.storage_fdhash_size;
            xlock_lock(&(hmo.storage.fdhash + idx)->lock);
            /* race with mds_storage_fd_remove() */
            if (hlist_unhashed(&fde->list)) {
                xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
                continue;
            }
            remove = mds_storage_fd_cleanup(fde);
            if (remove) {
                /* wait for the last reference, while this broke the original
                 * storage unit test */
                while (atomic_read(&fde->ref) > 0)
                    sched_yield();
                hlist_del_init(&fde->list);
                close(fde->fd);
                if (fde->state == FDE_NORMAL)
                    xfree((void *)fde->arg);
                xfree(fde);
            }
            xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
            if (remove) {
                atomic_dec(&hmo.storage.active);
            } else {
                mds_storage_fd_lru_update(fde);
            }
        }
    }
}

/* mds_storage_clean_dir() do clean all the opened fds in the directory
 * 'duuid'.
 */
int mds_storage_clean_dir(u64 duuid)
{
    struct fdhash_entry *fde;
    struct hlist_node *pos, *n;
    int i, j = 0;
    
    /* Step 1: add this deleted dir to a memory list */
    mds_storage_deleted_dir(duuid);

    /* Step 2: try to clean the memcache now */
    for (i = 0; i < hmo.conf.storage_fdhash_size; i++) {
        xlock_lock(&(hmo.storage.fdhash + i)->lock);
        hlist_for_each_entry_safe(fde, pos, n,
                                  &(hmo.storage.fdhash + i)->h,
                                  list) {
            if (fde->uuid == duuid && 
                atomic_read(&fde->ref) == 0 &&
                (fde->type == MDS_STORAGE_DATA)) {
                
                xlock_lock(&hmo.storage.lru_lock);
                /* Caution: race with fd_limit_check! */
                if (list_empty(&fde->lru)) {
                    xlock_unlock(&hmo.storage.lru_lock);
                    continue;
                } else
                    list_del(&fde->lru);
                xlock_unlock(&hmo.storage.lru_lock);

                /* this drop function should close fde->fd */
                if (append_buf_destroy_drop(fde))
                    j++;

                hlist_del_init(&fde->list);
                if (fde->state == FDE_NORMAL)
                    xfree((void *)fde->arg);
                xfree(fde);
                atomic_dec(&hmo.storage.active);
                gk_debug(mds, "Clean the data file %d in dir %lx",
                           fde->fd, duuid);
            }
        }
        xlock_unlock(&(hmo.storage.fdhash + i)->lock);
    }

    atomic64_add(j, &hmo.pending_ios);

    return 0;
}

/* evict dir does hard to evict range files
 */
int mds_storage_evict_rangef(u64 duuid)
{
    struct fdhash_entry *fde;
    struct hlist_node *pos, *n;
    int i;

    for (i = 0; i < hmo.conf.storage_fdhash_size; i++) {
        xlock_lock(&(hmo.storage.fdhash + i)->lock);
        hlist_for_each_entry_safe(fde, pos, n,
                                  &(hmo.storage.fdhash + i)->h,
                                  list) {
            if (fde->uuid == duuid &&
                atomic_read(&fde->ref) == 0 &&
                (fde->type == MDS_STORAGE_RANGE)) {

                xlock_lock(&hmo.storage.lru_lock);
                /* Caution: reace with fd_limit_check! */
                if (list_empty(&fde->lru)) {
                    xlock_unlock(&hmo.storage.lru_lock);
                    continue;
                } else
                    list_del(&fde->lru);
                xlock_unlock(&hmo.storage.lru_lock);
                
                hlist_del_init(&fde->list);
                close(fde->fd);
                xfree(fde);
                atomic_dec(&hmo.storage.active);
            }
        }
        xlock_unlock(&(hmo.storage.fdhash + i)->lock);
    }

    return 0;
}

void mds_storage_pending_io(void)
{
    int i, nr;
    
    do {
        nr = min(MDS_STORAGE_IO_PEAK, 
                 (u64)atomic64_read(&hmo.pending_ios));
    
        if (mds_aio_queue_empty())
            atomic64_add(-nr, &hmo.pending_ios);
        else
            break;
    } while (nr > 0);
    
    for (i = 0; i < nr; i++) {
        mds_aio_start();
    }
    atomic64_add(-nr, &hmo.pending_ios);
}

void mds_storage_fd_pagecache_cleanup(void)
{
    static u64 last_rbytes = 0;
    struct fdhash_entry *fde;
    struct hlist_node *pos;
    int idx;

#ifdef MDS_DROP_CACHE
    if (atomic64_read(&hmo.prof.storage.rbytes) - last_rbytes 
        < hmo.conf.pcct) {
        return;
    }
    last_rbytes = atomic64_read(&hmo.prof.storage.rbytes);
#else
    if (atomic64_read(&hmo.prof.storage.rbytes) +
        atomic64_read(&hmo.prof.storage.wbytes) - last_rbytes
        < (hmo.conf.pcct << 1)) {
        return;
    }
    last_rbytes = atomic64_read(&hmo.prof.storage.rbytes) +
        atomic64_read(&hmo.prof.storage.wbytes);
#endif

    for (idx = 0; idx < hmo.conf.storage_fdhash_size; idx++) {
        xlock_lock(&(hmo.storage.fdhash + idx)->lock);
        hlist_for_each_entry(fde, pos, 
                             &(hmo.storage.fdhash + idx)->h, 
                             list) {
            if (fde->type == MDS_STORAGE_ITB ||
                fde->type == MDS_STORAGE_DATA) {
                posix_fadvise(fde->fd, 0, 0, POSIX_FADV_DONTNEED);
            }
        }
        xlock_unlock(&(hmo.storage.fdhash + idx)->lock);
    }
}

void __mdisk_range_sort(void *ranges, size_t size);

/* mds_stroage_fd_mdisk()
 *
 * do mdisk open.
 */
int mds_storage_fd_mdisk(struct fdhash_entry *fde, char *path)
{
    int err = 0;
    int size;
    
    xlock_lock(&fde->lock);
    if (fde->state == FDE_FREE) {
        /* ok, we should open it */
        fde->fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fde->fd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                     path, strerror(-errno), -errno);
            err = -errno;
            goto out_unlock;
        }
        gk_warning(mds, "open file %s w/ fd %d\n", path, fde->fd);
        fde->state = FDE_OPEN;
    }
    if (fde->state == FDE_OPEN) {
        /* we should load the disk struct to memory */
        long br, bl = 0;

        do {
            br = pread(fde->fd, (void *)(&fde->mdisk) + bl, 
                       sizeof(struct md_disk) - bl, bl);
            if (br < 0) {
                gk_err(mds, "pread failed w/ %d\n", errno);
                err = -errno;
                goto out_unlock;
            } else if (br == 0) {
                if (bl == 0) {
                    goto first_hit;
                }
                gk_err(mds, "pread to EOF w/ offset %ld\n", bl);
                err = -EINVAL;
                goto out_unlock;
            }
            bl += br;
        } while (bl < sizeof(struct md_disk));

        /* we alloc the region for the ranges */
        fde->mdisk.size = (fde->mdisk.range_nr);
        size = fde->mdisk.size * sizeof(range_t);
        
        if (!size) {
            /* there is no content to read, just return OK */
            goto ok;
        }
        
        fde->mdisk.ranges = xzalloc(size);
        if (!fde->mdisk.ranges) {
            gk_err(mds, "xzalloc ranges failed\n");
            err = -ENOMEM;
            goto out_unlock;
        }

        /* load the ranges to memory */
        bl = 0;
        do {
            br = pread(fde->fd, (void *)fde->mdisk.ranges + bl,
                       size - bl, sizeof(struct md_disk) + bl);
            if (br < 0) {
                gk_err(mds, "pread failed w/ %d\n", errno);
                err = -errno;
                goto out_unlock;
            } else if (br == 0) {
                gk_err(mds, "pread to EOF @ %ld vs %d\n",
                         sizeof(struct md_disk) + bl,
                         size);
                err = -EINVAL;
                goto out_unlock;
            }
            bl += br;
        } while (bl < size);
        /* we need to sort the range list */
        __mdisk_range_sort(fde->mdisk.ranges, fde->mdisk.size);
    ok:
        fde->mdisk.new_range = NULL;
        fde->mdisk.new_size = 0;
        fde->state = FDE_MDISK;
    }
out_unlock:
    xlock_unlock(&fde->lock);
    
    return err;
first_hit:
    /* init the mdisk memory structure */
    fde->mdisk.winsize = MDS_STORAGE_DEFAULT_RANGE_SIZE;
    fde->state = FDE_MDISK;
    
    goto out_unlock;
}

/* __normal_open() do normal file open
 */
int __normal_open(struct fdhash_entry *fde, char *path)
{
    int err = 0;

    xlock_lock(&fde->lock);
    if (fde->state == FDE_FREE) {
        /* ok, we should open it */
        fde->fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fde->fd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                     path, strerror(errno), -errno);
            err = -errno;
            goto out_unlock;
        }
        gk_warning(mds, "open file %s w/ fd %d\n", path, fde->fd);
        fde->state = FDE_OPEN;
    }
    if (fde->state == FDE_OPEN) {
        /* we should lseek to the tail of the file */
        fde->proxy.foffset = lseek(fde->fd, 0, SEEK_END);
        if (fde->proxy.foffset == -1UL) {
            gk_err(mds, "lseek to tail of fd(%d) failed w/ %d\n",
                     fde->fd, -errno);
            err = -errno;
            goto out_unlock;
        }
        fde->state = FDE_NORMAL;
    }
out_unlock:
    xlock_unlock(&fde->lock);

    return err;
}

int __normal_write(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    loff_t offset;
    long bl, bw;
    int err = 0, i;
    
    xlock_lock(&fde->lock);
    if (msa->offset == -1) {
        /* do lseek() now */
        fde->proxy.foffset = lseek(fde->fd, 0, SEEK_END);
        if (fde->proxy.foffset == -1UL) {
            gk_err(mds, "do lseek on fd(%d) failed w/ %d\n",
                     fde->fd, -errno);
            err = -errno;
            goto out;
        }
        offset = *((u64 *)msa->arg) = fde->proxy.foffset;
    } else
        offset = *((u64 *)msa->arg) = msa->offset;

    for (i = 0; i < msa->iov_nr; i++) {
        bl = 0;
        do {
            bw = pwrite(fde->fd, (msa->iov + i)->iov_base + bl,
                        (msa->iov + i)->iov_len - bl, offset + bl);
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
        } while (bl < (msa->iov + i)->iov_len);
        atomic64_add(bl, &hmo.prof.storage.wbytes);
    }
out:    
    xlock_unlock(&fde->lock);

    return err;
}

/* Note: I find an issue that when I run the client.ut test with 16 threads,
 * the performance of mds read is VERY bad (as low as <1MB/s). While, using
 * fio to simulate the io pattern, I got about 15MB/s. This is a huge gap!
 *
 * After some more investments, I finally realise that the reason for this low
 * performance is we issue two many large random I/O requests. Thus, we have
 * this I/O read controler.
 */
static inline
void random_read_control_down(size_t len)
{
    if (len < GK_TINY_FILE_LEN)
        return;
    xcond_lock(&hmo.storage.cond);
    while (atomic_read(&hmo.prof.storage.pread) >= 2) {
        /* we should wait now */
        xcond_wait(&hmo.storage.cond);
    }

    /* ok to issue */
    atomic_inc(&hmo.prof.storage.pread);
    xcond_unlock(&hmo.storage.cond);
}

static inline
void random_read_control_up(size_t len)
{
    if (len < GK_TINY_FILE_LEN)
        return;
    atomic_dec(&hmo.prof.storage.pread);
    xcond_lock(&hmo.storage.cond);
    xcond_broadcast(&hmo.storage.cond);
    xcond_unlock(&hmo.storage.cond);
}

/* __normal_read()
 *
 * Note: we just relay the I/O to the file, nothing should be changed
 */
int __normal_read(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    loff_t offset;
    long bl, br;
    int err = 0, i;

    if (unlikely(fde->state < FDE_OPEN || !msa->iov_nr || !msa->iov))
        return -EINVAL;
    
    offset = msa->offset;
    if (offset == -1UL) {
        offset = lseek(fde->fd, 0, SEEK_CUR);
    }
    
    gk_debug(mds, "read offset %ld len %ld\n", offset, msa->iov->iov_len);
    random_read_control_down(msa->iov->iov_len);
    for (i = 0; i < msa->iov_nr; i++) {
        bl = 0;
        do {
            br = pread(fde->fd, (msa->iov + i)->iov_base + bl, 
                       (msa->iov + i)->iov_len - bl, offset + bl);
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
        } while (bl < (msa->iov + i)->iov_len);
        atomic64_add(bl, &hmo.prof.storage.rbytes);
    }

out:
    random_read_control_up(msa->iov->iov_len);
    
    return err;
}

/* Read the content and drop it. Use the buffer and length in msa
 */
int __normal_read_drop(struct fdhash_entry *fde, 
                       struct mds_storage_access *msa)
{
    loff_t offset;
    long bl, br;
    int err = 0, i;

    if (unlikely(fde->state < FDE_OPEN || !msa->iov_nr || !msa->iov))
        return -EINVAL;
    
    /* reset offset to ZERO */
    offset = 0;

    do {
        random_read_control_down(msa->iov->iov_len);
        for (i = 0; i < msa->iov_nr; i++) {
            bl = 0;
            do {
                br = pread(fde->fd, (msa->iov + i)->iov_base + bl, 
                           (msa->iov + i)->iov_len - bl, offset + bl);
                if (br < 0) {
                    gk_err(mds, "pread failed w/ %d\n", errno);
                    err = -errno;
                    goto out;
                } else if (br == 0) {
                    gk_warning(mds, "reach EOF, exit.\n");
                    goto out;
                }
                bl += br;
            } while (bl < (msa->iov + i)->iov_len);
            atomic64_add(bl, &hmo.prof.storage.rbytes);
            offset += (msa->iov + i)->iov_len;
        }
        random_read_control_up(msa->iov->iov_len);
    } while (offset < fde->abuf.file_offset + fde->abuf.offset);

    return err;
out:
    random_read_control_up(msa->iov->iov_len);
    return err;
}

int __mmap_write(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    return 0;
}

int __mmap_read(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    return 0;
}

int __mdisk_write(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    loff_t offset = 0;
    long bw, bl = 0;
    u32 crc = ~(u32)0;
    int err = 0;
    
    if (fde->state != FDE_MDISK && fde->state != FDE_LOCKED)
        return 0;
    
    /* calculate the crc32c checksum */
    fde->mdisk.crc = 0;
    fde->mdisk.crc = crc32c(crc, (const u8 *)(&fde->mdisk), 
                            sizeof(struct md_disk));
    
    /* we should write the fde.mdisk to disk file */
    xlock_lock(&fde->lock);
    do {
        bw = pwrite(fde->fd, (void *)(&fde->mdisk) + bl, 
                    sizeof(struct md_disk) - bl, offset + bl);
        if (bw < 0) {
            gk_err(mds, "pwrite md_disk failed w/ %d\n", errno);
            err = -errno;
            goto out;
        }
        bl += bw;
    } while (bl < sizeof(struct md_disk));
    offset += sizeof(struct md_disk);

    /* write the original ranges to disk */
    if (fde->mdisk.size) {
        bl = 0;
        do {
            bw = pwrite(fde->fd, (void *)fde->mdisk.ranges + bl,
                        sizeof(range_t) * fde->mdisk.size - bl, 
                        offset + bl);
            if (bw < 0) {
                gk_err(mds, "pwrite mdisk.range faield w/ %d\n", errno);
                err = -errno;
                goto out;
            }
            bl += bw;
        } while (bl < sizeof(range_t) * fde->mdisk.size);
        offset += sizeof(range_t) * fde->mdisk.size;
    }

    /* write the new ranges to disk */
    if (fde->mdisk.new_size) {
        bl = 0;
        do {
            bw = pwrite(fde->fd, (void *)fde->mdisk.new_range + bl,
                        sizeof(range_t) * fde->mdisk.new_size - bl,
                        offset + bl);
            if (bw < 0) {
                gk_err(mds, "pwrite mdisk.new_range failed w/ %d\n", errno);
                err = -errno;
                goto out;
            }
            bl += bw;
        } while (bl < sizeof(range_t) * fde->mdisk.new_size);
    }
    gk_debug(mds, "mdisk write len %ld\n", sizeof(range_t) * 
               fde->mdisk.new_size);

    atomic64_add(sizeof(struct md_disk) + 
                 fde->mdisk.size + fde->mdisk.new_size, 
                 &hmo.prof.storage.wbytes);
out:            
    xlock_unlock(&fde->lock);
    
    return err;
}

int __mdisk_add_range_nolock(struct fdhash_entry *fde, u64 begin, u64 end, 
                             u64 range_id)
{
    range_t *ptr;
    size_t size = fde->mdisk.new_size;
    int err = 0;
    
    if (fde->state != FDE_MDISK && fde->state != FDE_LOCKED) {
        err = -EINVAL;
        goto out_unlock;
    }

    size++;
    ptr = xrealloc(fde->mdisk.new_range, size * sizeof(range_t));
    if (!ptr) {
        gk_err(mds, "xrealloc failed to extend\n");
        err = -ENOMEM;
        goto out_unlock;
    }

    fde->mdisk.new_range = ptr;
    ptr = (fde->mdisk.new_range + fde->mdisk.new_size);
    ptr->begin = begin;
    ptr->end = end;
    ptr->range_id = range_id;
    fde->mdisk.new_size = size;

    fde->mdisk.range_nr++;
    
out_unlock:
    
    return err;
}

int __mdisk_add_range(struct fdhash_entry *fde, u64 begin, u64 end, 
                      u64 range_id)
{
    int err = 0;

    xlock_lock(&fde->lock);
    err = __mdisk_add_range_nolock(fde, begin, end, range_id);
    xlock_unlock(&fde->lock);
    
    return err;
}

int __mdisk_lookup_nolock(struct fdhash_entry *fde, int op, u64 arg, 
                          range_t **out)
{
    int i = 0, found = 0;
    int err = 0;
    
    if (fde->state != FDE_MDISK && fde->state != FDE_LOCKED) {
        err = -EINVAL;
        goto out_unlock;
    }

    if (op == MDS_MDISK_RANGE) {
        if (fde->mdisk.ranges) {
            for (i = 0; i < fde->mdisk.size; i++) {
                if ((fde->mdisk.ranges + i)->begin <= arg && 
                    (fde->mdisk.ranges + i)->end > arg) {
                    found = 1;
                    break;
                }
                if ((fde->mdisk.ranges + i)->begin > arg)
                    break;
            }
            if (found) {
                *out = (fde->mdisk.ranges + i);
                goto out_unlock;
            }
        }
        if (fde->mdisk.new_range) {
            found = 0;

            for (i = 0; i < fde->mdisk.new_size; i++) {
                if ((fde->mdisk.new_range + i)->begin <= arg &&
                    (fde->mdisk.new_range + i)->end > arg) {
                    found = 1;
                    break;
                }
                if ((fde->mdisk.new_range + i)->begin > arg)
                    break;
            }
            if (found) {
                *out = (fde->mdisk.new_range + i);
                goto out_unlock;
            }
        }
    }

    err = -ENOENT;
out_unlock:
    
    return err;
}

int __mdisk_lookup(struct fdhash_entry *fde, int op, u64 arg,
                   range_t **out)
{
    int err;
    
    xlock_lock(&fde->lock);
    err = __mdisk_lookup_nolock(fde, op, arg, out);
    xlock_unlock(&fde->lock);

    return err;
}

int __mdisk_range_compare(const void *left, const void *right)
{
    range_t *a = (range_t *)left;
    range_t *b = (range_t *)right;

    if (a->begin < b->begin)
        return -1;
    else if (a->begin > b->begin)
        return 1;
    else
        return 0;
}

void __mdisk_range_sort(void *ranges, size_t size)
{
    qsort(ranges, size, sizeof(range_t), __mdisk_range_compare);
}

int __range_lookup(u64 duuid, u64 itbid, struct mmap_args *ma, u64 *location)
{
    struct fdhash_entry *fde;
    int err = 0;

    fde = mds_storage_fd_lookup_create(duuid, MDS_STORAGE_RANGE, (u64)ma);
    if (IS_ERR(fde)) {
        gk_err(mds, "lookup create %lx/%ld range %ld faield\n",
                 duuid, itbid, ma->range_id);
        err = PTR_ERR(fde);
        goto out;
    }
    *location = *((u64 *)(fde->mwin.addr) + (itbid - fde->mwin.offset));

    mds_storage_fd_put(fde);
out:
    return err;
}

int __range_write(u64 duuid, u64 itbid, struct mmap_args *ma, u64 location)
{
    struct fdhash_entry *fde;
    int err = 0;

    fde = mds_storage_fd_lookup_create(duuid, MDS_STORAGE_RANGE, (u64)ma);
    if (IS_ERR(fde)) {
        gk_err(mds, "lookup create %lx/%ld range %ld failed\n",
                 duuid, itbid, ma->range_id);
        err = PTR_ERR(fde);
        goto out;
    }
    *((u64 *)(fde->mwin.addr) + (itbid - fde->mwin.offset)) = location;

    mds_storage_fd_put(fde);
out:
    return err;
}

int __range_write_conditional(u64 duuid, u64 itbid, struct mmap_args *ma, 
                              u64 location)
{
    struct fdhash_entry *fde;
    int err = 0;

    fde = mds_storage_fd_lookup_create(duuid, MDS_STORAGE_RANGE, (u64)ma);
    if (IS_ERR(fde)) {
        gk_err(mds, "lookup create %lx/%ld range %ld failed\n",
                 duuid, itbid, ma->range_id);
        err = PTR_ERR(fde);
        goto out;
    }
    if (*((u64 *)(fde->mwin.addr) + (itbid - fde->mwin.offset)) == 0)
        *((u64 *)(fde->mwin.addr) + (itbid - fde->mwin.offset)) = location;

    mds_storage_fd_put(fde);
out:
    return err;
}

/* mds_storage_fd_mmap()
 *
 * @win: the window size of the mmap region
 */
int mds_storage_fd_mmap(struct fdhash_entry *fde, char *path, 
                         struct mmap_args *ma)
{
    int err = 0;
    
    xlock_lock(&fde->lock);
    if (fde->state == FDE_FREE) {
        /* ok, we should open it */
        fde->fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fde->fd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                     path, strerror(errno), -errno);
            err = -errno;
            goto out_unlock;
        }
        gk_warning(mds, "open file %s w/ fd %d\n", path, fde->fd);
        fde->state = FDE_OPEN;
    }
    if (fde->state == FDE_OPEN) {
        /* check the file size, do not mmap the range file if ma->winsize
         * unmatched */
        struct stat stat;

        err = fstat(fde->fd, &stat);
        if (err < 0) {
            gk_err(mds, "fd %d fstat failed w/ %d.\n",
                     fde->fd, errno);
            err = -errno;
            goto out_unlock;
        }
        if (stat.st_size != ma->win) {
            if (!stat.st_size) {
                /* ok, we just create the range file now */
                err = ftruncate(fde->fd, ma->win);
                if (err) {
                    gk_err(mds, "ftruncate fd %d to %ld failed w/ %d\n",
                             fde->fd, ma->win, errno);
                    err = -errno;
                    goto out_unlock;
                }
            } else {
                gk_err(mds, "winsize mismatch st_size %ld vs winsize %ld\n",
                         stat.st_size, ma->win);
                err = -EINVAL;
                goto out_unlock;
            }
        }
        
        /* do mmap on the region */
        err = lseek(fde->fd, 0, SEEK_SET);
        if (err < 0) {
            gk_err(mds, "fd %d mmap win created failed w/ %d.\n",
                     fde->fd, errno);
            err = -errno;
            goto out_unlock;
        }
        fde->mwin.addr = mmap(NULL, ma->win, PROT_WRITE | PROT_READ,
                              MAP_SHARED, fde->fd, ma->foffset);
        if (fde->mwin.addr == MAP_FAILED) {
            gk_err(mds, "mmap fd %d in region [%ld,%ld] failed w/ %d\n",
                     fde->fd, 0UL, ma->win, errno);
            err = -errno;
            goto out_unlock;
        }
        fde->mwin.offset = ma->range_begin;
        fde->mwin.file_offset = ma->foffset;
        fde->mwin.len = ma->win;
        fde->mwin.arg = ma->range_id;
        fde->mwin.flag = ma->flag;
        fde->state = FDE_MEMWIN;
    }
out_unlock:            
    xlock_unlock(&fde->lock);

    return err;
}

int mds_storage_fd_bitmap(struct fdhash_entry *fde, char *path)
{
    u64 offset;
    int err = 0;

    xlock_lock(&fde->lock);
    if (fde->state == FDE_FREE) {
        /* ok, we should open it */
        fde->fd = open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        if (fde->fd < 0) {
            gk_err(mds, "open file '%s' failed w/ %s(%d)\n", 
                     path, strerror(errno), -errno);
            err = -errno;
            goto out_unlock;
        }
        /* check to see if this is the first access */
        offset = lseek(fde->fd, 0, SEEK_END);
        if (offset == 0) {
            /* we need to write the default block now */
            err = ftruncate(fde->fd, XTABLE_BITMAP_BYTES);
            if (err < 0) {
                gk_err(mds, "ftruncate file to one slice failed w/ %d\n",
                         errno);
                err = -errno;
                goto out_unlock;
            }
            {
                u8 data = 0xff;
                long bw = 0;
                
                do {
                    bw = pwrite(fde->fd, &data, 1, 0);
                    if (bw < 0) {
                        gk_err(mds, "pwrite the default region in the first slice "
                                 "failed w/ %d\n", errno);
                        err = -errno;
                        goto out_unlock;
                    }
                } while (bw < 1);
            }
        } else if (offset < 0) {
            gk_err(mds, "lseek failed w/ %d\n", errno);
            err = -errno;
            goto out_unlock;
        }
        gk_err(mds, "open file %s w/ fd %d\n", path, fde->fd);
        xlock_init(&fde->bmmap.lock);
        fde->state = FDE_OPEN;
    }
    if (fde->state == FDE_OPEN) {
        /* we should get a 128KB buffer to mmap/mremap the bitmap slice */
        fde->bmmap.len = (XTABLE_BITMAP_SIZE / 8);
        fde->bmmap.addr = NULL;
        fde->bmmap.file_offset = 0;
        fde->state = FDE_BITMAP;
    }
out_unlock:
    xlock_unlock(&fde->lock);

    return err;
}

/* this function only do one-bit write on the final localtion(bit nr) */
/* Note:
 *
 * if the offset we want to write is higher than the allocated slice, we
 * should do read/copy/enlarge operations together.
 */
int __bitmap_write(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    /* Note that: msa->arg is just the bit offset; if the iov is not null,
     * we have troubles. we must write the entire iov region to the end of the
     * file and return new start address. */
    u64 offset = (u64)msa->arg;
    u64 new_offset;
    u64 snr, bl;
    long bw;
    int err = 0;
    
    if (msa->iov) {
        /* we have trouble! */

        /* the new location should be return by @msa->arg */
        xlock_lock(&fde->bmmap.lock);
        /* append the iov to the file */
        new_offset = lseek(fde->fd, 0, SEEK_END);
        if (new_offset < 0) {
            xlock_unlock(&fde->bmmap.lock);
            gk_err(mds, "lseek to bitmap fd %d failed w/ %d\n",
                     fde->fd, errno);
            err = -errno;
            goto out;
        }
        *((u64 *)msa->arg) = new_offset;

        ASSERT(msa->iov_nr == 2, mds);
        bl = 0;
        do {
            bw = pwrite(fde->fd, msa->iov[0].iov_base + bl,
                        msa->iov[0].iov_len - bl, new_offset + bl);
            if (bw < 0) {
                xlock_unlock(&fde->bmmap.lock);
                gk_err(mds, "pwrite bitmap fd %d offset %ld bl %ld "
                         "failed w/ %s\n",
                         fde->fd, new_offset + bl, bl, strerror(errno));
                err = -errno;
                goto out;
            }
            bl += bw;
        } while (bl < msa->iov[0].iov_len);

        /* do lseek now */
        new_offset = lseek(fde->fd, msa->offset * fde->bmmap.len, SEEK_END);
        if (new_offset < 0) {
            xlock_unlock(&fde->bmmap.lock);
            gk_err(mds, "lseek failed w/ %s\n", strerror(errno));
            err = -errno;
            goto out;
        }
        /* write the last bitmap slice */
        bl = 0;
        do {
            bw = pwrite(fde->fd, msa->iov[1].iov_base + bl,
                        msa->iov[1].iov_len - bl, new_offset + bl);
            if (bw < 0) {
                xlock_unlock(&fde->bmmap.lock);
                gk_err(mds, "pwrite bitmap fd %d offset %ld bl %ld "
                         "failed w/ %s\n",
                         fde->fd, new_offset + bl, bl, strerror(errno));
                err = -errno;
                goto out;
            }
            bl += bw;
        } while (bl < msa->iov[1].iov_len);
        xlock_unlock(&fde->bmmap.lock);

        atomic64_add(bl, &hmo.prof.storage.wbytes);
    } else {
        /* what a nice day! */

        /* find the byte offset */
        snr = BITMAP_ROUNDDOWN(offset) >> XTABLE_BITMAP_SHIFT >> 3;

        xlock_lock(&fde->bmmap.lock);
        fde->bmmap.addr = mmap(NULL, fde->bmmap.len, PROT_READ | PROT_WRITE,
                               MAP_SHARED, fde->fd, 
                               msa->offset + snr * fde->bmmap.len);
        if (fde->bmmap.addr == MAP_FAILED) {
            xlock_unlock(&fde->bmmap.lock);
            gk_err(mds, "mmap bitmap file @ %lx failed w/ %d\n",
                     msa->offset, errno);
            err = -errno;
            goto out;
        }
        /* flip the bit now */
        offset -= snr * (fde->bmmap.len << 3);
        __set_bit(offset, fde->bmmap.addr);
        /* unmap the region now */
        err = munmap(fde->bmmap.addr, fde->bmmap.len);
        if (err) {
            xlock_unlock(&fde->bmmap.lock);
            gk_err(mds, "munmap failed w/ %d\n", errno);
            err = -errno;
            goto out;
        }
        xlock_unlock(&fde->bmmap.lock);
        gk_debug(mds, "map fd %d offset %ld and update bit %ld snr %ld\n",
                   fde->fd, msa->offset + snr * fde->bmmap.len, offset, snr);
    }
out:
    return err;
}

/* It has been fde locked yet!
 */
int __bitmap_write_v2(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    /* Note that: msa->arg is just the bit offset; if the iov is not null,
     * we have troubles. we must write the entire iov region to the end of the
     * file and return new start address. */
    u64 offset = (u64)msa->arg;
    u64 new_offset;
    u64 snr, bl;
    long bw;
    int err = 0;

    if (msa->iov) {
        /* write the total iov to disk sequentially and return the file
         * location */
        new_offset = lseek(fde->fd, 0, SEEK_END);
        if (new_offset < 0) {
            gk_err(mds, "lseek to bitmap fd %d failed w/ %d\n",
                     fde->fd, errno);
            err = -errno;
            goto out;
        }
        *((u64 *)msa->arg) = new_offset;

        ASSERT(msa->iov_nr == 2, mds);
        bl = 0;
        do {
            bw = pwrite(fde->fd, msa->iov[0].iov_base + bl,
                        msa->iov[0].iov_len - bl, new_offset + bl);
            if (bw < 0) {
                gk_err(mds, "pwrite bitmap fd %d offset %ld bl %ld "
                         "failed w/ %s\n",
                         fde->fd, new_offset + bl, bl, strerror(errno));
                err = -errno;
                goto out;
            }
            bl += bw;
        } while (bl < msa->iov[0].iov_len);

        new_offset += msa->iov[0].iov_len;
        bl = 0;
        do {
            bw = pwrite(fde->fd, msa->iov[1].iov_base + bl,
                        msa->iov[1].iov_len - bl, new_offset + bl);
            if (bw < 0) {
                gk_err(mds, "pwrite bitmap fd %d offset %ld bl %ld "
                         "failed w/ %s\n",
                         fde->fd, new_offset + bl, bl, strerror(errno));
                err = -errno;
                goto out;
            }
            bl += bw;
        } while (bl < msa->iov[1].iov_len);

        atomic64_add(bl, &hmo.prof.storage.wbytes);
    } else {
        /* ok, this means we should update the bit */

        snr = BITMAP_ROUNDDOWN(offset) >> XTABLE_BITMAP_SHIFT >> 3;

        fde->bmmap.addr = mmap(NULL, fde->bmmap.len, PROT_READ | PROT_WRITE,
                               MAP_SHARED, fde->fd,
                               msa->offset);
        if (fde->bmmap.addr == MAP_FAILED) {
            gk_err(mds, "mmap bitmap file @ %lx failed w/ %d\n",
                     msa->offset, errno);
            err = -errno;
            goto out;
        }
        /* flip the bit now */
        offset -= snr * (fde->bmmap.len << 3);
        __set_bit(offset, fde->bmmap.addr);
        /* unmap the region now */
        err = munmap(fde->bmmap.addr, fde->bmmap.len);
        if (err) {
            gk_err(mds, "munmap failed w/ %d\n", errno);
            err = -errno;
            goto out;
        }
        gk_debug(mds, "map fd %d offset %ld and update bit %ld snr %ld\n",
                   fde->fd, msa->offset, offset, snr);
    }
out:
    return err;
}

/* this function only read the region(s) from the bitmap file */
int __bitmap_read(struct fdhash_entry *fde, struct mds_storage_access *msa)
{
    loff_t offset;
    long bl, br;
    int err = 0, i;
    
    if (fde->state < FDE_OPEN || !msa->iov_nr || !msa->iov) {
        return -EINVAL;
    }

    /* msa->offset is the real file location! */
    offset = msa->offset;
    for (i = 0; i < msa->iov_nr; i++) {
        if ((msa->iov + i)->iov_len == 0)
            continue;
        bl = 0;
        do {
            br = pread(fde->fd, (msa->iov + i)->iov_base + bl,
                       (msa->iov + i)->iov_len - bl, offset + bl);
            if (br < 0) {
                gk_err(mds, "pread failed w/ %d\n", errno);
                err = -errno;
                goto out;
            } else if (br == 0) {
                if (bl == (msa->iov + i)->iov_len)
                    break;
                gk_err(mds, "reach EOF left %ld.\n",
                         (msa->iov + i)->iov_len - bl);
                err = -EINVAL;
                goto out;
            }
            bl += br;
        } while (bl < (msa->iov + i)->iov_len);
        atomic64_add(bl, &hmo.prof.storage.rbytes);
    }

out:
    return err;
}

static inline
int mds_storage_fd_init(struct fdhash_entry *fde)
{
    char path[GK_MAX_NAME_LEN] = {0, };
    int err = 0;
    
    /* NOTE:
     *
     * 1. itb/data file should be written with self buffering through the mem
     *    window or not
     *
     * 2. itb/data file should be read through the mem window or direct read.
     *
     * 3. md/range file should be read/written with mem window
     */

    /* NOTE2: you should consider the concurrent access here!
     */

    /* make sure the duuid dir exist */
    sprintf(path, "%s/%lx/%lx", hmo.conf.mds_home, hmo.site_id, fde->uuid);
    err = mds_storage_dir_make_exist(path);
    if (err) {
        gk_err(mds, "duuid dir %s do not exist %d.\n", path, err);
        goto out;
    }

    /* please consider the concurrent access here in your subroutine! */

    switch (fde->type) {
    case MDS_STORAGE_MD:
        sprintf(path, "%s/%lx/%lx/md-%ld", hmo.conf.mds_home, hmo.site_id, 
                fde->uuid, fde->arg);
        err = mds_storage_fd_mdisk(fde, path);
        if (err) {
            gk_err(mds, "change state to open failed w/ %d\n", err);
            goto out;
        }
        break;
    case MDS_STORAGE_ITB:
        sprintf(path, "%s/%lx/%lx/itb-%ld", hmo.conf.mds_home, hmo.site_id, 
                fde->uuid, fde->arg);
        err = append_buf_create(fde, path, FDE_ABUF);
        if (err) {
            gk_err(mds, "append buf create failed w/ %d\n", err);
            goto out;
        }
        break;
    case MDS_STORAGE_ITB_ODIRECT:
        sprintf(path, "%s/%lx/%lx/itb-%ld", hmo.conf.mds_home, hmo.site_id,
                fde->uuid, fde->arg);
        err = odirect_create(fde, path, FDE_ODIRECT);
        if (err) {
            gk_err(mds, "odirect buf create failed w/ %d\n", err);
            goto out;
        }
        break;
    case MDS_STORAGE_RANGE:
    {
        struct mmap_args *ma = (struct mmap_args *)fde->arg;
        
        sprintf(path, "%s/%lx/%lx/%srange-%ld", hmo.conf.mds_home, hmo.site_id, 
                fde->uuid, ((ma->flag & MA_GC) ? "G" : ""), ma->range_id);
        err = mds_storage_fd_mmap(fde, path, ma);
        if (err) {
            gk_err(mds, "mmap window created failed w/ %d\n", err);
            goto out;
        }
        break;
    }
    case MDS_STORAGE_DATA:
        sprintf(path, "%s/%lx/%lx/data-%ld", hmo.conf.mds_home, hmo.site_id, 
                fde->uuid, fde->arg);
        err = append_buf_create(fde, path, FDE_ABUF);
        if (err) {
            gk_err(mds, "append buf create failed w/ %d\n", err);
            goto out;
        }
        /* ok, we should set the file to random access */
        posix_fadvise(fde->fd, 0, 0, POSIX_FADV_RANDOM);
        break;
    case MDS_STORAGE_BITMAP:
        sprintf(path, "%s/%lx/%lx/data-%ld", hmo.conf.mds_home, hmo.site_id, 
                fde->uuid, fde->arg);
        err = mds_storage_fd_bitmap(fde, path);
        if (err) {
            gk_err(mds, "bitmap file created failed w/ %d\n", err);
            goto out;
        }
        break;
    case MDS_STORAGE_NORMAL:
    {
        /* 
         * the file name is constructed as following:
         * duuid/.proxy.uuid.cno
         */
        struct proxy_args *pa = (struct proxy_args *)fde->arg;
        
        sprintf(path, "%s/%lx/%lx/.proxy.%lx.%lx", hmo.conf.mds_home,
                hmo.site_id, fde->uuid, pa->uuid, pa->cno);
        err = __normal_open(fde, path);
        if (err) {
            gk_err(mds, "normal file create failed w/ %d\n", err);
            goto out;
        }
        break;
    }
    case MDS_STORAGE_DIRECTW:
        sprintf(path, "%s/%lx/%lx/directw", hmo.conf.mds_home, hmo.site_id, 
                fde->uuid);
        break;
    case MDS_STORAGE_LOG:
        sprintf(path, "%s/%lx/log", hmo.conf.mds_home, hmo.site_id);
        break;
    case MDS_STORAGE_SPLIT_LOG:
        sprintf(path, "%s/%lx/split_log", hmo.conf.mds_home, hmo.site_id);
        break;
    case MDS_STORAGE_TXG:
        sprintf(path, "%s/%lx/txg", hmo.conf.mds_home, hmo.site_id);
        break;
    case MDS_STORAGE_TMP_TXG:
        sprintf(path, "%s/%lx/tmp_txg", hmo.conf.mds_home, hmo.site_id);
        break;
    default:
        gk_err(mds, "Invalid file type provided, check your codes.\n");
        err = -EINVAL;
        goto out;
    }

out:
    return err;
}

struct fdhash_entry *mds_storage_fd_lookup_create(u64 duuid, int fdtype, 
                                                   u64 arg)
{
    struct fdhash_entry *fde;
    int err = 0;
    
    fde = mds_storage_fd_lookup(duuid, fdtype, arg);
    if (!IS_ERR(fde)) {
    recheck_state:
        if (fde->state <= FDE_OPEN) {
            goto reinit;
        } else if (fde->state == FDE_LOCKED ||
                   fde->aux_state == FDE_AUX_LOCKED) {
            /* this FDE has already been locked, we should wait it.(Note that
             * we have already got the reference) */
            xcond_lock(&fde->cond);
            do {
                xcond_wait(&fde->cond);
            } while (fde->state == FDE_LOCKED ||
                     fde->aux_state == FDE_AUX_LOCKED);
            xcond_unlock(&fde->cond);
            /* ok, we should return this fde to user */
            goto recheck_state;
        } else 
            return fde;
    }

    /* Step 1: create a new fdhash_entry */
    fde = xzalloc(sizeof(*fde));
    if (!fde) {
        gk_err(mds, "xzalloc struct fdhash_entry failed.\n");
        return ERR_PTR(-ENOMEM);
    } else {
        struct fdhash_entry *inserted;
        
        /* init it */
        INIT_HLIST_NODE(&fde->list);
        INIT_LIST_HEAD(&fde->lru);
        xlock_init(&fde->lock);
        xcond_init(&fde->cond);
        atomic_set(&fde->ref, 1);
        fde->uuid = duuid;
        fde->arg = arg;
        fde->type = fdtype;
        fde->state = FDE_FREE;
        /* insert into the fdhash table */
        inserted = mds_storage_fd_insert(fde);
        if (inserted != fde) {
            gk_warning(mds, "someone insert this fde before us.\n");
            xfree(fde);
            fde = inserted;
        }
    }

    /* Step 2: we should open the file now */
reinit:
    err = mds_storage_fd_init(fde);
    if (err) {
        goto out_clean;
    }
    
    return fde;
out_clean:
    /* we should release the fde on error */
    if (atomic_dec_return(&fde->ref) == 0) {
        mds_storage_fd_remove(fde);
        xfree(fde);
    }
    
    return ERR_PTR(err);
}

int mds_storage_fd_write(struct fdhash_entry *fde, 
                          struct mds_storage_access *msa)
{
    int err = 0;

retry:
    switch (fde->state) {
    case FDE_ABUF:
        err = append_buf_write(fde, msa);
        if (err) {
            gk_err(mds, "append_buf_write failed w/ %d\n", err);
            goto out_failed;
        }
        break;
    case FDE_ABUF_UNMAPPED:
        /* this is surely a transient state, we should try to remap the abuf,
         * and retry */
        xlock_lock(&fde->lock);
        if (fde->state == FDE_ABUF_UNMAPPED) {
            err = append_buf_flush_remap(fde);
            if (err) {
                gk_err(mds, "append_buf_flush_remap() failed w/ %d\n",
                         err);
            }
        }
        xlock_unlock(&fde->lock);
        goto retry;
        break;
    case FDE_ODIRECT:
        err = odirect_write(fde, msa);
        if (err) {
            gk_err(mds, "odirect_write failed w/ %d\n", err);
            goto out_failed;
        }
        break;
    case FDE_MEMWIN:
        err = __mmap_write(fde, msa);
        if (err) {
            gk_err(mds, "__mmap_write failed w/ %d\n", err);
            goto out_failed;
        }
        break;
    case FDE_NORMAL:
        err = __normal_write(fde, msa);
        if (err) {
            gk_err(mds, "__normal_write faield w/ %d\n", err);
            goto out_failed;
        }
        break;
    case FDE_MDISK:
    case FDE_LOCKED:
        err = __mdisk_write(fde, msa);
        if (err) {
            gk_err(mds, "__mdisk_write failed w/ %d\n", err);
            goto out_failed;
        }
        break;
    case FDE_BITMAP:
        err = __bitmap_write_v2(fde, msa);
        if (err) {
            gk_err(mds, "__bitmap_write failed w/ %d\n", err);
            goto out_failed;
        }
        break;
    case FDE_OPEN:
        /* we should change to ABUF or MEMWIN or NORMAL access mode */
        err = mds_storage_fd_init(fde);
        if (err) {
            gk_err(mds, "try to change state failed w/ %d\n", err);
            goto out_failed;
        }
        goto retry;
        break;
    default:
        /* we should (re-)open the file */
        ;
    }

    atomic64_inc(&hmo.prof.storage.wreq);
out_failed:
    return err;
}

int mds_storage_fd_read(struct fdhash_entry *fde,
                         struct mds_storage_access *msa)
{
    int err = 0;

retry:
    if (fde->state == FDE_ABUF || fde->state == FDE_NORMAL ||
        fde->state == FDE_ABUF_UNMAPPED) {
        err = __normal_read(fde, msa);
        if (err) {
            gk_err(mds, "__normal_read failed w/ %d\n", err);
            goto out_failed;
        }
    } else if (fde->state == FDE_MEMWIN) {
        err = __mmap_read(fde, msa);
        if (err) {
            gk_err(mds, "__mmap_read failed w/ %d\n", err);
            goto out_failed;
        }
    } else if (fde->state == FDE_MDISK || fde->state == FDE_LOCKED) {
        gk_err(mds, "hoo, you should not call this function on this FDE\n");
    } else if (fde->state == FDE_ODIRECT) {
        err = odirect_read(fde, msa);
        if (err) {
            gk_err(mds, "__odirect_read failed w/ %d\n", err);
            goto out_failed;
        }
    } else if (fde->state == FDE_BITMAP) {
        err = __bitmap_read(fde, msa);
        if (err) {
            gk_err(mds, "__bitmap_read failed w/ %d\n", err);
            goto out_failed;
        }
    } else if (fde->state == FDE_OPEN) {
        /* we should change to ABUF or MEMWIN or NORMAL access mode */
        err = mds_storage_fd_init(fde);
        if (err) {
            gk_err(mds, "try to change state failed w/ %d\n", err);
            goto out_failed;
        }
        goto retry;
    } else {
        /* we should (re-)open the file */
        gk_warning(mds, "Invalid read operation: state %d\n",
                     fde->state);
    }

    atomic64_inc(&hmo.prof.storage.rreq);
out_failed:
    return err;
}

/* Return value:
 *
 * 1: means that we should remove the entry
 * 0: means that we should leave the fd entry in hash table
 */
int mds_storage_fd_cleanup(struct fdhash_entry *fde)
{
    /* For append buf and odirect buffer, we do not release the fde entry. We
     * just release the attached memory resources. Change the fde state to
     * FDE_OPEN! */
    int err = 0;

    switch (fde->state) {
    case FDE_ABUF:
        if (fde->type == MDS_STORAGE_DATA &&
            fde->aux_state == FDE_AUX_LOCKED)
            err = 0;
        else
            append_buf_destroy(fde);
        break;
    case FDE_ODIRECT:
        odirect_destroy(fde);
        break;
    case FDE_MDISK:
        err = __mdisk_write(fde, NULL);
        if (err)
            err = 0;
        else
            err = 1;
        break;
    case FDE_LOCKED:
        err = 0;
        break;
    case FDE_MEMWIN:
        err = munmap(fde->mwin.addr, fde->mwin.len);
        if (err)
            err = 0;
        else {
            fde->state = FDE_OPEN;
            err = 1;
        }
        break;
    default:
        err = 1;
    }

    gk_debug(mds, "cleanup fd %d to state %d w/ %d\n", 
               fde->fd, fde->state, err);
    
    return err;
}

int mds_storage_dir_make_exist(char *path)
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

int mds_storage_toe_commit(struct txg_open_entry *toe, struct txg_end *te)
{
    struct itb_info *pos;
    loff_t offset;
    long bw, bl;
    int err = 0;

    xlock_lock(&hmo.storage.txg_fd_lock);
    offset = lseek(hmo.storage.txg_fd, 0, SEEK_END);
    if (offset < 0) {
        gk_err(mds, "lseek to end of fd %d failed w/ %d\n",
                 hmo.storage.txg_fd, errno);
        err = -errno;
        goto out_unlock;
    }
    /* write the TXG_BEGIN */
    bl = 0;
    do {
        bw = pwrite(hmo.storage.txg_fd, (void *)&toe->begin + bl, 
                    sizeof(struct txg_begin) - bl, offset + bl);
        if (bw <= 0) {
            gk_err(mds, "pwrite to fd %d failed w/ %d\n",
                     hmo.storage.txg_fd, errno);
            err = -errno;
            goto out_unlock;
        }
        bl += bw;
    } while (bl < sizeof(struct txg_begin));
    offset += sizeof(struct txg_begin);

    /* write the itb info to disk */
    if (atomic_read(&toe->itb_nr)) {
        list_for_each_entry(pos, &toe->itb, list) {
            bl = 0;
            do {
                bw = pwrite(hmo.storage.txg_fd, (void *)&pos->duuid + bl,
                            ITB_INFO_DISK_SIZE - bl, offset + bl);
                if (bw <= 0) {
                    gk_err(mds, "pwrite to fd %d failed w/ %d\n",
                             hmo.storage.txg_fd, errno);
                    err = -errno;
                    goto out_unlock;
                }
                bl += bw;
            } while (bl < ITB_INFO_DISK_SIZE);
            offset += ITB_INFO_DISK_SIZE;
        }
    }

    /* write the other deltas to disk */
    if (toe->other_region) {
        bl = 0;
        do {
            bw = pwrite(hmo.storage.txg_fd, (void *)toe->other_region + bl,
                        toe->osize - bl, offset + bl);
            if (bw <= 0) {
                gk_err(mds, "pwrite to fd %d (bw %ld osize %d) "
                         "failed w/ %d\n",
                         hmo.storage.txg_fd, bw, toe->osize, errno);
                err = -errno;
                goto out_unlock;
            }
            bl += bw;
        } while (bl < toe->osize);
        offset += toe->osize;
    }

    /* write the txg_end to disk */
    bl = 0;
    do {
        bw = pwrite(hmo.storage.txg_fd, (void *)te + bl, 
                    sizeof(*te) - bl, offset + bl);
        if (bw <= 0) {
            gk_err(mds, "pwrite to fd %d failed w/ %d\n",
                     hmo.storage.txg_fd, errno);
            err = -errno;
            goto out_unlock;
        }
        bl += bw;
    } while (bl < sizeof(*te));

out_unlock:
    xlock_unlock(&hmo.storage.txg_fd_lock);
    
    return err;
}

int mds_storage_update_range(struct txg_open_entry *toe)
{
    struct itb_info *pos, *n;
    struct fdhash_entry *fde;
    struct mmap_args ma = {0, };
    range_t *range;
    int err = 0;

    list_for_each_entry_safe(pos, n, &toe->itb, list) {
        fde = mds_storage_fd_lookup_create(pos->duuid, MDS_STORAGE_MD, 0);
        if (IS_ERR(fde)) {
            gk_err(mds, "lookup create MD file faield w/ %ld\n",
                     PTR_ERR(fde));
            err = PTR_ERR(fde);
            continue;
        }
        if (pos->master < fde->mdisk.itb_master)
            goto put_fde;
        ma.win = MDS_STORAGE_DEFAULT_RANGE_SIZE;
    relookup:
        xlock_lock(&fde->lock);
        err = __mdisk_lookup_nolock(fde, MDS_MDISK_RANGE, pos->itbid, &range);
        /* FIXME: here is a RACE point, no lock protect fde from concurrent
         * __mdisk_add_range */
        if (err == -ENOENT) {
            /* create a new range now */
            u64 i;
            
            i = MDS_STORAGE_idx2range(pos->itbid);
            __mdisk_add_range_nolock(fde, i * MDS_STORAGE_RANGE_SLOTS,
                                     (i + 1) * MDS_STORAGE_RANGE_SLOTS - 1,
                                     fde->mdisk.range_aid++);
            __mdisk_range_sort(fde->mdisk.new_range, fde->mdisk.new_size);
            xlock_unlock(&fde->lock);
            goto relookup;
        } else if (err) {
            gk_err(mds, "mdisk_lookup_nolock failed w/ %d\n", err);
            xlock_unlock(&fde->lock);
            goto put_fde;
        }
        xlock_unlock(&fde->lock);

        ma.foffset = 0;
        ma.range_id = range->range_id;
        ma.range_begin = range->begin;
        ma.flag = MA_OFFICIAL;

        gk_debug(mds, "write II %lx %ld to location %ld\n",
                   pos->duuid, pos->itbid, pos->location);

        if (pos->overwrite)
            err = __range_write(pos->duuid, pos->itbid, &ma, pos->location);
        else
            err = __range_write_conditional(pos->duuid, pos->itbid, &ma, 
                                            pos->location);
        if (err) {
            gk_err(mds, "range write failed w/ %d\n", err);
            goto put_fde;
        }
        err = __mdisk_write(fde, NULL);
        if (err) {
            gk_err(mds, "sync md file failed w/ %d\n", err);
        }
    put_fde:
        mds_storage_fd_put(fde);

        /* free the resources */
        list_del(&pos->list);
        xfree(pos);
    }
    return err;
}

/* Got the current max offset of this file
 */
u64 mds_storage_fd_max_offset(struct fdhash_entry *fde)
{
    u64 offset;
    
    switch (fde->state) {
    case FDE_ABUF_UNMAPPED:
    case FDE_ABUF:
        offset = fde->abuf.file_offset + fde->abuf.offset;
        break;
    case FDE_ODIRECT:
        offset = fde->odirect.file_offset;
        break;
    default:
        /* use lseek to detect */
        offset = lseek(fde->fd, 0, SEEK_END);
        if (offset == -1UL) {
            gk_err(mds, "find the max file %d offset failed w/ %s(%d)\n",
                     fde->fd, strerror(errno), errno);
        }
    }

    return offset;
}

/* Get the max txg from txg log file
 */
int mds_storage_find_max_txg(u64 site, u64 *txg, int fd)
{
    struct txg_begin tb;
    off_t offset = 0;
    int err = 0, bl, br;

    /* read in the content from last checkpoint position */
    do {
        /* get the txg_begin entry */
        bl = 0;
        do {
            br = pread(fd, (void *)&tb + bl,
                       sizeof(tb) - bl, offset + bl);
            if (br < 0) {
                gk_err(mds, "read txg log file failed w/ %d\n", errno);
                err = -errno;
                goto out;
            } else if (br == 0) {
                /* it is ok to break here */
                goto out;
            }
            bl += br;
        } while (bl < sizeof(tb));

        gk_debug(mds, "Find site %lx, got %lx offset %ld, TB(%d,%ld)\n", 
                   site, tb.site_id, offset, tb.itb_nr, tb.txg);
    
        if (tb.magic == TXG_BEGIN_MAGIC &&
            tb.site_id == site) {
            if (*txg < tb.txg)
                *txg = tb.txg;
            else {
                /* this means we lost some txg in execution, fallback to last
                 * txg number */
                gk_warning(mds, "TXG from site %lx is decreasing from "
                             "%ld to %ld\n",
                             site, *txg, tb.txg);
                *txg = tb.txg;
            }
        }
        offset += sizeof(tb);
        
        /* skip itb info region */
        offset += ITB_INFO_DISK_SIZE * tb.itb_nr;
        /* skip other region */
        offset += tb.dir_delta_nr * 
            sizeof(struct gk_dir_delta) +
            tb.rdd_nr *
            sizeof(struct gk_dir_delta) +
            tb.bitmap_delta_nr * 
            sizeof(struct bitmap_delta) +
            tb.ckpt_nr * 
            sizeof(struct checkpoint) +
            tb.rd_nr * sizeof(u64);
        /* skip txg_end */
        offset += sizeof(struct txg_end);
    } while (1);

out:
    if (err) {
        gk_warning(mds, "Read from txg log file failed w/ %d, corrupted?\n",
                     err);
    }
    /* ignore any error */

    return 0;
}

void __mds_txg_pair_write(struct txg_begin *tb, struct txg_end *te)
{
    loff_t offset;
    long bw, bl;
    
    xlock_lock(&hmo.storage.txg_fd_lock);
    offset = lseek(hmo.storage.txg_fd, 0, SEEK_END);
    if (offset < 0) {
        gk_err(mds, "lseek to end of fd %d failed w/ %d\n",
                 hmo.storage.txg_fd, errno);
        goto out_unlock;
    }
    /* write the TXG_BEGIN */
    bl = 0;
    do {
        bw = pwrite(hmo.storage.txg_fd, (void *)tb + bl,
                    sizeof(*tb) - bl, offset + bl);
        if (bw <= 0) {
            gk_err(mds, "pwrite to fd %d failed w/ %d\n",
                     hmo.storage.txg_fd, errno);
            goto out_unlock;
        }
        bl += bw;
    } while (bl < sizeof(*tb));
    offset += sizeof(*tb);

    /* write the TXG_END */
    bl = 0;
    do {
        bw = pwrite(hmo.storage.txg_fd, (void *)te + bl,
                    sizeof(*te) - bl, offset + bl);
        if (bw <= 0) {
            gk_err(mds, "pwrite to fd %d failed w/ %d\n",
                     hmo.storage.txg_fd, errno);
            goto out_unlock;
        }
        bl += bw;
    } while (bl < sizeof(*te));

out_unlock:    
    xlock_unlock(&hmo.storage.txg_fd_lock);
}

void __mds_txg_rename(void)
{
    char opath[256], npath[256];
    int err = 0;

    sprintf(opath, "%s/%lx/txg", hmo.conf.mds_home, hmo.site_id);
    sprintf(npath, "%s/%lx/last-txg", hmo.conf.mds_home, hmo.site_id);

    err = rename(opath, npath);
    if (err) {
        gk_err(mds, "rename txg to last-txg failed w/ %d\n",
                 errno);
        goto out;
    }

    /* close old file and open new file */
    err = open(opath, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
    if (err < 0) {
        gk_err(mds, "open file '%s' failed w/ %d\n", opath, errno);
    }
    close(hmo.storage.txg_fd);
    hmo.storage.txg_fd = err;
    
out:
    return;
}

/* mds_startup_normal() write a TXG_BEGIN/TXG_END pair to txg file
 */
void mds_startup_normal(void)
{
    struct txg_begin tb;
    struct txg_end te;

    memset(&tb, 0, sizeof(tb));
    tb.magic = TXG_BEGIN_MAGIC;
    memset(&te, 0, sizeof(te));
    te.magic = TXG_END_MAGIC;

    /* set up specific info */
    tb.site_id = hmo.site_id;
    tb.session_id = hmo.session | HMO_SESSION_BEGIN;
    te.site_id = hmo.site_id;
    te.session_id = hmo.session | HMO_SESSION_BEGIN;

    /* change to a new txg file */
    __mds_txg_rename();
    
    /* do write */
    __mds_txg_pair_write(&tb, &te);
}

/* mds_exit_normal() write a TXG_BEGIN/TXG_END pair to txg file
 */
void mds_exit_normal(void)
{
    struct txg_begin tb;
    struct txg_end te;

    memset(&tb, 0, sizeof(tb));
    tb.magic = TXG_BEGIN_MAGIC;
    memset(&te, 0, sizeof(te));
    te.magic = TXG_END_MAGIC;

    /* set up specific info */
    tb.site_id = hmo.site_id;
    tb.session_id = hmo.session | HMO_SESSION_END;
    te.site_id = hmo.site_id;
    te.session_id = hmo.session | HMO_SESSION_END;

    __mds_txg_pair_write(&tb, &te);
}

/* Return value: 0 => ok; <0 => error;
 */
int mds_txg_integrated(void)
{
    struct txg_begin tb;
    loff_t offset = 0;
    u64 begin_session = 0, end_session = 0;
    int err = -ENOENT, bl, br;

    /* read in the content from last checkpoint position */
    do {
        /* get the txg_begin entry */
        bl = 0;
        do {
            br = pread(hmo.storage.txg_fd, (void *)&tb + bl,
                       sizeof(tb) - bl, offset + bl);
            if (br < 0) {
                gk_err(mds, "read txg log file failed w/ %d offset %ld\n", 
                         errno, offset + bl);
                err = -errno;
                goto out;
            } else if (br == 0) {
                /* it is ok to break here */
                goto out_check;
            }
            bl += br;
        } while (bl < sizeof(tb));

        if (tb.magic == TXG_BEGIN_MAGIC &&
            tb.site_id == hmo.site_id) {
            if (tb.session_id & HMO_SESSION_BEGIN)
                begin_session = tb.session_id;
            else if (tb.session_id & HMO_SESSION_END)
                end_session = tb.session_id;
        }
        offset += sizeof(tb);

        /* skip itb info region */
        offset += ITB_INFO_DISK_SIZE * tb.itb_nr;
        /* skip other region */
        offset += tb.dir_delta_nr *
            sizeof(struct gk_dir_delta) +
            tb.rdd_nr *
            sizeof(struct gk_dir_delta) +
            tb.bitmap_delta_nr * 
            sizeof(struct bitmap_delta) +
            tb.ckpt_nr * 
            sizeof(struct checkpoint) +
            tb.rd_nr * sizeof(u64);
        /* skip txg_end */
        offset += sizeof(struct txg_end);
    } while (1);

out_check:
    if ((begin_session & (~HMO_SESSION_MASK)) ==
        (end_session & (~HMO_SESSION_MASK))) {
        if (end_session) 
            err = 0;
        else if (!begin_session) {
            /* there is no session pair */
            err = 0;
        } else {
            err = -ENOENT;
        }
    }
out:
    if (err) {
        gk_warning(mds, "MDS txg integrated check failed w/ %d(%s)\n",
                     err, strerror(-err));
    }

    return err;
}

/* if msa->arg is MDS_FILE_BULK_LOAD_DROP, then we read in the file content,
 * and then drop it.
 */
int mds_storage_bulk_load(struct fdhash_entry *fde,
                           struct mds_storage_access *msa)
{
    int err = 0;

retry:
    if (fde->state == FDE_ABUF || fde->state == FDE_NORMAL ||
        fde->state == FDE_ABUF_UNMAPPED) {
        if (((u64)msa->arg) == MDS_FILE_BULK_LOAD_DROP) {
            err = __normal_read_drop(fde, msa);
        } else {
            err = __normal_read(fde, msa);
        }
        if (err) {
            gk_err(mds, "__normal_read failed w/ %d\n", 
                     err);
            goto out_failed;
        }
    } else if (fde->state == FDE_OPEN) {
        /* we should init the fd and retry */
        err = mds_storage_fd_init(fde);
        if (err) {
            gk_err(mds, "try to change state failed w/ %d\n", err);
            goto out_failed;
        }
        goto retry;
    } else {
        gk_warning(mds, "Invalid read operation: state %d\n",
                     fde->state);
        err = -EINVAL;
    }

    atomic64_inc(&hmo.prof.storage.rreq);

out_failed:
    return err;
}

/* syncer helpers */
int __sync_md(u64 duuid)
{
    return 0;
}
