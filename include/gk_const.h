/**
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2019-09-04 09:59:32 macan>
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

#ifndef __GK_CONST_H__
#define __GK_CONST_H__

#define GK_MAX_NAME_LEN         256
#define MDS_DCONF_MAX_NAME_LEN  63
#define ROOT_DCONF_MAX_NAME_LEN MDS_DCONF_MAX_NAME_LEN
#define MDSL_DCONF_MAX_NAME_LEN MDS_DCONF_MAX_NAME_LEN

static char *gk_ccolor[] __attribute__((unused)) = 
{
    "\033[0;40;31m",            /* red */
    "\033[0;40;32m",            /* green */
    "\033[0;40;33m",            /* yellow */
    "\033[0;40;34m",            /* blue */
    "\033[0;40;35m",            /* pink */
    "\033[0;40;36m",            /* yank */
    "\033[0;40;37m",            /* white */
    "\033[0m",                  /* end */
};

#define GK_COLOR(x)   (gk_ccolor[x])
#define GK_COLOR_RED  (gk_ccolor[0])
#define GK_COLOR_GREEN        (gk_ccolor[1])
#define GK_COLOR_YELLOW       (gk_ccolor[2])
#define GK_COLOR_BLUE         (gk_ccolor[3])
#define GK_COLOR_PINK         (gk_ccolor[4])
#define GK_COLOR_YANK         (gk_ccolor[5])
#define GK_COLOR_WHITE        (gk_ccolor[6])
#define GK_COLOR_END          (gk_ccolor[7])

#define ENOTEXIST       1025    /* Not Exist */
#define EHSTOP          1026    /* Stop */

#define EHWAIT          1033    /* wait a few seconds and retry */
#define ERECOVER        1034    /* notify a recover process to the caller */

#define EOFFLINE        1044    /* Site is offline */
#define ECLOSING        1045    /* site is closing */
#define ELAUNCH         1046    /* site is launching */
#define ERETRY          1047    /* retry yourself */
#define EBADV           1048    /* Bad version */

#endif
