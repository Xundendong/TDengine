
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
#ifndef _TD_BSE_TABLE_MANAGER_H_
#define _TD_BSE_TABLE_MANAGER_H_

#include "bse.h"
#include "bseCache.h"
#include "bseTable.h"
#include "bseUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  STableBuilder *p[2];
  int8_t         inUse;

  int8_t inited;
  SBse  *pBse;

  TdThreadMutex mutex;
} STableBuilderMgt;

typedef struct {
  SArray       *pFileList;
  STableCache  *pTableCache;
  SBlockCache  *pBlockCache;
  TdThreadMutex mutex;
  SBse         *pBse;
} STableReaderMgt;

typedef struct {
  void            *pBse;
  STableBuilderMgt pBuilderMgt[1];
  STableReaderMgt  pReaderMgt[1];
} STableMgt;

int32_t bseTableMgtCreate(SBse *pBse, void **pMgt);

int32_t bseTableMgtGet(STableMgt *p, int64_t seq, uint8_t **pValue, int32_t *len);

int32_t bseTableMgtCleanup(void *p);

int32_t bseTableMgtCommit(STableMgt *pMgt, SArray **pLiveFileList);

int32_t bseTableMgtUpdateLiveFileSet(STableMgt *pMgt, SArray *pLiveFileList);

int32_t bseTableMgtAppend(STableMgt *pMgt, SBseBatch *pBatch);

int32_t bseTableMgtGetLiveFileSet(STableMgt *pMgt, SArray **pList);
#ifdef __cplusplus
}
#endif

#endif