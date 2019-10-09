/*
 **************************************************************************
 *                                                                        *
 *          General Purpose Hash Function Algorithms Library              *
 *                                                                        *
 * Author: Arash Partow - 2002                                            *
 * URL: http://www.partow.net                                             *
 * URL: http://www.partow.net/programming/hashfunctions/index.html        *
 *                                                                        *
 * Copyright notice:                                                      *
 * Free use of the General Purpose Hash Function Algorithms Library is    *
 * permitted under the guidelines and in accordance with the most current *
 * version of the Common Public License.                                  *
 * http://www.opensource.org/licenses/cpl.php                             *
 *                                                                        *
 **************************************************************************
*/

/* Murmurhash from http://sites.google.com/site/murmurhash/
 *
 * All code is released to public domain. For business purposes, Murmurhash is
 * under the MIT license.
 */

/**
 * Other codes written by Ma Can following the GPL
 *
 * Copyright (c) 2009 Ma Can <ml.macana@gmail.com>
 *                           <macan@ncic.ac.cn>
 *
 * Armed with EMACS.
 * Time-stamp: <2010-07-10 14:04:11 macan>
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

/* BEGIN OF General Hash Functions */
static inline unsigned int RSHash(char* str, unsigned int len)
{
   unsigned int b    = 378551;
   unsigned int a    = 63689;
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = hash * a + (*str);
      a    = a * b;
   }

   return hash;
}
/* End Of RS Hash Function */


static inline unsigned int JSHash(char* str, unsigned int len)
{
   unsigned int hash = 1315423911;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash ^= ((hash << 5) + (*str) + (hash >> 2));
   }

   return hash;
}
/* End Of JS Hash Function */


static inline unsigned int PJWHash(char* str, unsigned int len)
{
   const unsigned int BitsInUnsignedInt = (unsigned int)(sizeof(unsigned int) * 8);
   const unsigned int ThreeQuarters     = (unsigned int)((BitsInUnsignedInt  * 3) / 4);
   const unsigned int OneEighth         = (unsigned int)(BitsInUnsignedInt / 8);
   const unsigned int HighBits          = (unsigned int)(0xFFFFFFFF) << (BitsInUnsignedInt - OneEighth);
   unsigned int hash              = 0;
   unsigned int test              = 0;
   unsigned int i                 = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash << OneEighth) + (*str);

      if((test = hash & HighBits)  != 0)
      {
         hash = (( hash ^ (test >> ThreeQuarters)) & (~HighBits));
      }
   }

   return hash;
}
/* End Of  P. J. Weinberger Hash Function */


static inline unsigned int ELFHash(char* str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int x    = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash << 4) + (*str);
      if((x = hash & 0xF0000000L) != 0)
      {
         hash ^= (x >> 24);
      }
      hash &= ~x;
   }

   return hash;
}
/* End Of ELF Hash Function */


static inline unsigned int BKDRHash(char* str, unsigned int len)
{
   unsigned int seed = 131; /* 31 131 1313 13131 131313 etc.. */
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (hash * seed) + (*str);
   }

   return hash;
}
/* End Of BKDR Hash Function */


static inline unsigned int SDBMHash(char* str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = (*str) + (hash << 6) + (hash << 16) - hash;
   }

   return hash;
}
/* End Of SDBM Hash Function */


static inline unsigned int DJBHash(char* str, unsigned int len)
{
   unsigned int hash = 5381;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) + hash) + (*str);
   }

   return hash;
}
/* End Of DJB Hash Function */


static inline unsigned int DEKHash(char* str, unsigned int len)
{
   unsigned int hash = len;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash = ((hash << 5) ^ (hash >> 27)) ^ (*str);
   }
   return hash;
}
/* End Of DEK Hash Function */


static inline unsigned int BPHash(char* str, unsigned int len)
{
   unsigned int hash = 0;
   unsigned int i    = 0;
   for(i = 0; i < len; str++, i++)
   {
      hash = hash << 7 ^ (*str);
   }

   return hash;
}
/* End Of BP Hash Function */


static inline unsigned int FNVHash(char* str, unsigned int len)
{
   const unsigned int fnv_prime = 0x811C9DC5;
   unsigned int hash      = 0;
   unsigned int i         = 0;

   for(i = 0; i < len; str++, i++)
   {
      hash *= fnv_prime;
      hash ^= (*str);
   }

   return hash;
}
/* End Of FNV Hash Function */


static inline unsigned int APHash(char* str, unsigned int len)
{
   unsigned int hash = 0xAAAAAAAA;
   unsigned int i    = 0;

   for(i = 0; i < len; str++, i++)
   {
       hash ^= ((i & 1) == 0) ? (  (hash <<  7) ^ ((*str) * (hash >> 3))) :
           (~(((hash << 11) + (*str)) ^ (hash >> 5)));
   }

   return hash;
}
/* End Of AP Hash Function */
/* END OF General Hash Functions */

static inline
u64 __murmurhash64a(const void *key, int len, u64 seed)
{
    const u64 m = 0xc6a4a7935bd1e995;
    const int r = 47;

    u64 h = seed ^ (len * m);

    const u64 *data = (const u64 *)key;
    const u64 *end = data + (len/8);

    while (data != end) {
        u64 k = *data++;

        k *= m;
        k ^= k >> r;
        k *= m;

        h ^= k;
        h *= m;
    }

    const unsigned char *data2 = (const unsigned char *)data;

    switch (len & 7) {
    case 7: h ^= ((u64)data2[6]) << 48;
    case 6: h ^= ((u64)data2[5]) << 40;
    case 5: h ^= ((u64)data2[4]) << 32;
    case 4: h ^= ((u64)data2[3]) << 24;
    case 3: h ^= ((u64)data2[2]) << 16;
    case 2: h ^= ((u64)data2[1]) << 8;
    case 1: h ^= ((u64)data2[0]);
        h *= m;
    }

    h ^= h >> r;
    h *= m;
    h ^= h >> r;

    return h;
}

static inline
u32 __murmurhash32(const void * key, int len, u32 seed)
{
  // 'm' and 'r' are mixing constants generated offline.
  // They're not really 'magic', they just happen to work well.

  const u32 m = 0x5bd1e995;
  const int r = 24;

  // Initialize the hash to a 'random' value

  u32 h = seed ^ len;

  // Mix 4 bytes at a time into the hash

  const unsigned char * data = (const unsigned char *)key;

  while (len >= 4) {
      u32 k = *(u32 *)data;

      k *= m;
      k ^= k >> r;
      k *= m;
      
      h *= m;
      h ^= k;
      
      data += 4;
      len -= 4;
  }
  
  // Handle the last few bytes of the input array

  switch (len) {
  case 3: h ^= data[2] << 16;
  case 2: h ^= data[1] << 8;
  case 1: h ^= data[0];
      h *= m;
  };

  // Do a few final mixes of the hash to ensure the last few
  // bytes are well-incorporated.

  h ^= h >> 13;
  h *= m;
  h ^= h >> 15;

  return h;
} 

static inline u64 gk_hash_nsht(void *key, int keylen)
{
    return __murmurhash64a((const void *)key, keylen, 0xab90175de9084);
}

static inline u64 gk_hash_ns(void *key, int keylen)
{
#if 0
    return __murmurhash32((const void *)key, keylen, 0xfe887f94);
#else
    //return __murmurhash64a((const void *)key, keylen, 0x98dafe887f940eab);
    return __murmurhash64a((const void *)key, keylen, 0);
#endif
}

static inline u64 gk_hash_eh(u64 key1, u64 key2, u64 key2len)
{
#if 1
    u64 hash;

    hash = hash_64(key1, 64);
    hash ^= JSHash((char *)key2, key2len);
    return hash;
#else
    return __murmurhash64a((const void *)key2, key2len, key1);
#endif
}

static inline u64 gk_hash_cbht(u64 key1, u64 key2, u64 key2len)
{
#if 1
    u64 hash;

    hash = hash_64(key1, 64);
    hash ^= APHash((char *)&key2, sizeof(u64));
    return hash;
#else
    u64 val1, val2;
    
    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
#endif
}

static inline u64 gk_hash_ring(u64 key1, u64 key2, u64 key2len)
{
    u64 val1, val2;
    
    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
}

static inline u32 gk_hash_dh(u64 key)
{
    return RSHash((char *)(&key), sizeof(u64));
}

static inline u64 gk_hash_gdt(u64 key1, u64 key2)
{
    u64 val1, val2;
    
    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
}

static inline u64 gk_hash_vsite(u64 key1, u64 key2, u64 key2len)
{
    u64 val1;

    val1 = APHash((char *)key2, key2len);
    val1 <<= 32;
    val1 |= RSHash((char *)&key1, sizeof(u64));
    val1 ^= hash_64(key1, 64);
    return val1;
}

static inline u64 gk_hash_kvs(u64 key, u64 keylen)
{
#if 0
    u64 val;

    val = APHash((char *)key, keylen);
    val <<= 32;
    val |= RSHash((char *)key, keylen);
    return val;
#else
    return __murmurhash64a((const void *)key, keylen, 0xf87211939);
#endif
}

static inline u32 gk_hash_tws(u64 key)
{
    return APHash((char *)(&key), sizeof(key));
}

/* for storage fd hash table */
static inline u32 gk_hash_fdht(u64 key1, u64 key2)
{
    u64 val1, val2;

    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
}

static inline u32 gk_hash_ddht(u64 key1, u64 key2)
{
    u64 val1, val2;

    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
}

static inline u32 gk_hash_site_mgr(u64 key1, u64 key2)
{
    u64 val1, val2;

    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
}

static inline u32 gk_hash_ring_mgr(u64 key1, u64 key2)
{
    u64 val1, val2;

    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
}

static inline u32 gk_hash_root_mgr(u64 key1, u64 key2)
{
    u64 val1, val2;

    val1 = hash_64(key2, 64);
    val2 = hash_64(key1, 64);
    val1 = val1 ^ (val2 ^ GOLDEN_RATIO_PRIME);

    return val1;
}

static inline
u64 gk_hash(u64 key1, u64 key2, u64 key2len, u32 sel)
{
    switch (sel) {
    case HASH_SEL_EH:
        return gk_hash_eh(key1, key2, key2len);
        break;
    case HASH_SEL_CBHT:
        return gk_hash_cbht(key1, key2, key2len);
        break;
    case HASH_SEL_RING:
        return gk_hash_ring(key1, key2, key2len);
    case HASH_SEL_DH:
        return gk_hash_dh(key1);
        break;
    case HASH_SEL_GDT:
        return gk_hash_gdt(key1, key2);
        break;
    case HASH_SEL_VSITE:
        return gk_hash_vsite(key1, key2, key2len);
        break;
    case HASH_SEL_KVS:
        return gk_hash_kvs(key2, key2len);
    default:
        /* we just fall through to zero */
        GK_VV("Invalid hash selector %d\n", sel);
        ;
    }
    return 0;
}
