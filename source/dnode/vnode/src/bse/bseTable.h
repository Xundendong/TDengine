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

#ifndef BSE_TABLE_H_
#define BSE_TABLE_H_

#include "bse.h"
#include "bseUtil.h"
#include "cJSON.h"
#include "os.h"
#include "tchecksum.h"
#include "tcompare.h"
#include "tlog.h"
#include "tmsg.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

#define kMaxEncodeLen 64
#define kEncodeLen    (2 * (kMaxEncodeLen) + 8)

#define kMagicNumber 0xdb4775248b80fb57ull;
#define kMagicNum    0x123456

enum {
  BSE_TABLE_DATA_TYPE = 0x1,
  BSE_TABLE_META_TYPE = 0x2,
  BSE_TABLE_INDEX_TYPE = 0x4,
  BSE_TABLE_FOOTER_TYPE = 0x8,
};

typedef struct {
  uint64_t  offset;
  uint64_t  size;
  SSeqRange range;
} SBlkHandle;
typedef struct {
  SBlkHandle metaHandle[1];
  SBlkHandle indexHandle[1];
} STableFooter;

typedef struct {
  int64_t seq;

} SSeqToBlk;
typedef struct {
  int32_t type;
  int32_t len;
  char    data[0];
} SBlock;

int8_t inSeqRange(SSeqRange *p, int64_t seq);
int8_t isGreaterSeqRange(SSeqRange *p, int64_t seq);

typedef struct {
  SBlock *data;
  int32_t cap;
  int8_t  type;
} SBlockWrapper;
typedef struct {
  char          name[TSDB_FILENAME_LEN];
  TdFilePtr     pDataFile;
  STableFooter  footer;
  SArray       *pSeqToBlock;
  SArray       *pMetaHandle;
  SBlockWrapper pBlockWrapper;
  int32_t       blockCap;
  int8_t        compressType;
  int32_t       offset;
  int32_t       blockId;
  SSeqRange     tableRange;
  SSeqRange     blockRange;

  SBse   *pBse;
  int32_t nRef;

} STableBuilder;

typedef struct {
  char         name[TSDB_FILENAME_LEN];
  TdFilePtr    pDataFile;
  STableFooter footer;
  SArray      *pSeqToBlock;
  SArray      *pMetaHandle;

  int32_t blockCap;
  int32_t fileSize;
  void   *pReaderMgt;
} STableReader;

typedef struct {
  int64_t sseq;
  int64_t eseq;
  int64_t size;
  int32_t level;
  char    name[TSDB_FILENAME_LEN];
} SBseLiveFileInfo;

int32_t tableBuildOpen(char *path, STableBuilder **pBuilder, SBse *pBse);
int32_t tableBuildPut(STableBuilder *p, int64_t *seq, uint8_t *value, int32_t len);
int32_t tableBuildPutBatch(STableBuilder *p, SBseBatch *pBatch);
int32_t tableBuildGet(STableBuilder *p, int64_t seq, uint8_t **value, int32_t *len);
int32_t tableBuildFlush(STableBuilder *p, int8_t type);
int32_t tableBuildCommit(STableBuilder *p, SBseLiveFileInfo *pInfo);
int32_t tableBuildClose(STableBuilder *p, int8_t commited);
void    tableBuildClear(STableBuilder *p);
int32_t tableBuildOpenFile(STableBuilder *p);

int32_t tableReadOpen(char *name, STableReader **pReader, void *pReaderMgt);
int32_t tableReadGet(STableReader *p, int64_t seq, uint8_t **pValue, int32_t *len);
int32_t tableReadClose(STableReader *p);

#ifdef __cplusplus
}
#endif
#endif