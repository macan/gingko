/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-08-19 14:18:03 macan>
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

#ifndef __TRACING_H__
#define __TRACING_H__

#include <sys/timeb.h>
#include <time.h>

/* gk tracing flags */
#define GK_INFO       0x80000000
#define GK_WARN       0x40000000
#define GK_ERR        0x20000000
#define GK_PLAIN      0x10000000

#define GK_ENTRY      0x00000008
#define GK_VERBOSE    0x00000004 /* more infos than debug mode */
#define GK_PRECISE    0x00000002
#define GK_DEBUG      0x00000001

#define GK_DEFAULT_LEVEL      0xf0000000
#define GK_DEBUG_ALL          0x0000000f

#ifdef __KERNEL__
#define PRINTK printk
#define FFLUSH
#else  /* !__KERNEL__ */
#define PRINTK printf
#define FPRINTK fprintf
#define FFLUSH(f) fflush(f)
#define KERN_INFO       "[INFO] "
#define KERN_ERR        "[ERR ] "
#define KERN_WARNING    "[WARN] "
#define KERN_DEBUG      "[DBG ] "
#define KERN_VERB       "[VERB] "
#define KERN_PLAIN      ""
#endif

#ifdef GK_TRACING
#define gk_tracing(mask, flag, lvl, f, a...) do {                     \
        if (unlikely(mask & flag)) {                                    \
            struct timeval __cur;                                       \
            struct tm __tmp;                                            \
            char __ct[32];                                              \
                                                                        \
            gettimeofday(&__cur, NULL);                                 \
            if (!localtime_r(&__cur.tv_sec, &__tmp)) {                  \
                PRINTK(KERN_ERR f, ## a);                               \
                FFLUSH(stdout);                                         \
                break;                                                  \
            }                                                           \
            strftime(__ct, 64, "%G-%m-%d %H:%M:%S", &__tmp);            \
            if (mask & GK_PRECISE) {                                  \
                PRINTK("%s.%03ld " lvl "GK (%16s, %5d): %s[%lx]: " f, \
                       __ct, (long)(__cur.tv_usec / 1000),              \
                       __FILE__, __LINE__, __func__,                    \
                       pthread_self(), ## a);                           \
                FFLUSH(stdout);                                         \
            } else {                                                    \
                PRINTK("%s.%03ld " lvl f,                               \
                       __ct, (long)(__cur.tv_usec / 1000), ## a);       \
                FFLUSH(stdout);                                         \
            }                                                           \
        }                                                               \
    } while (0)
#else
#define gk_tracing(mask, flag, lvl, f, a...)
#endif  /* !GK_TRACING */

#define IS_GK_DEBUG(module) ({                            \
            int ret;                                        \
            if (gk_##module##_tracing_flags & GK_DEBUG) \
                ret = 1;                                    \
            else                                            \
                ret = 0;                                    \
            ret;                                            \
        })

#define IS_GK_VERBOSE(module) ({                              \
            int ret;                                            \
            if (gk_##module##_tracing_flags & GK_VERBOSE)   \
                ret = 1;                                        \
            else                                                \
                ret = 0;                                        \
            ret;                                                \
        })

#define gk_info(module, f, a...)                          \
    gk_tracing(GK_INFO, gk_##module##_tracing_flags,  \
                 KERN_INFO, f, ## a)

#define gk_plain(module, f, a...)                         \
    gk_tracing(GK_PLAIN, gk_##module##_tracing_flags, \
                 KERN_PLAIN, f, ## a)

#define gk_verbose(module, f, a...)           \
    gk_tracing((GK_VERBOSE | GK_PRECISE), \
                 gk_##module##_tracing_flags, \
                 KERN_VERB, f, ## a)

#define gk_debug(module, f, a...)             \
    gk_tracing((GK_DEBUG | GK_PRECISE),   \
                 gk_##module##_tracing_flags, \
                 KERN_DEBUG, f, ## a)

#define gk_entry(module, f, a...)             \
    gk_tracing((GK_ENTRY | GK_PRECISE),   \
                 gk_##module##_tracing_flags, \
                 KERN_DEBUG, "entry: " f, ## a)

#define gk_exit(module, f, a...)              \
    gk_tracing((GK_ENTRY | GK_PRECISE),   \
                 gk_##module##_tracing_flags, \
                 KERN_DEBUG, "exit: " f, ## a)

#define gk_warning(module, f, a...)           \
    gk_tracing((GK_WARN | GK_PRECISE),    \
                 gk_##module##_tracing_flags, \
                 KERN_WARNING, f, ##a)

#define gk_err(module, f, a...)               \
    gk_tracing((GK_ERR | GK_PRECISE),     \
                 gk_##module##_tracing_flags, \
                 KERN_ERR, f, ##a)

#define SET_TRACING_FLAG(module, flag) do {     \
        gk_##module##_tracing_flags |= flag;  \
    } while (0)
#define CLR_TRACING_FLAG(module, flag) do {     \
        gk_##module##_tracing_flags &= ~flag; \
    } while (0)

#define TRACING_FLAG(name, v) u32 gk_##name##_tracing_flags = v
#define TRACING_FLAG_DEF(name) extern u32 gk_##name##_tracing_flags

#ifdef __KERNEL__
#define ASSERT(i, m) BUG_ON(!(i))
#else  /* !__KERNEL__ */
#define ASSERT(i, m) do {                               \
        if (!(i)) {                                     \
            gk_err(m, "Assertion " #i " failed!\n");  \
            exit(-EINVAL);                              \
        }                                               \
    } while (0)
#endif

#define GK_VV PRINTK
/* Use GK_BUG() to get the SIGSEGV signal to debug in the GDB */
#define GK_BUG() do {                         \
        GK_VV(KERN_PLAIN "GK BUG :(\n");    \
        (*((int *)0) = 1);                      \
    } while (0)
#define GK_BUGON(str) do {                        \
        GK_VV(KERN_PLAIN "Bug on '" #str "'\n");  \
        GK_BUG();                                 \
    } while (0)

#define gk_pf(f, a...) do {                   \
        if (hmo.conf.pf_file) {                 \
            FPRINTK(hmo.conf.pf_file, f, ## a); \
            FFLUSH(hmo.conf.pf_file);           \
        } else {                                \
            PRINTK(f, ## a);                    \
            FFLUSH(stdout);                     \
        }                                       \
    } while (0)

#endif  /* !__TRACING_H__ */
