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
#ifdef USE_STREAM
#ifndef TDENGINE_STREAM_INT_H
#define TDENGINE_STREAM_INT_H

#include "executor.h"
#include "query.h"
#include "trpc.h"
#include "stream.h"
#include "tref.h"

#ifdef __cplusplus
extern "C" {
#endif

#define STREAM_HB_INTERVAL_MS             1000
#define STREAM_GRP_DEFAULT_STREAM_NUM     20

typedef void (*taskUndeplyCallback)(void*);

typedef struct SStreamHbInfo {
  tmr_h        hbTmr;
  SStreamHbMsg hbMsg;
} SStreamHbInfo;

typedef struct SStreamTasksInfo {
  SArray* readerTaskList;        // SArray<SStreamReaderTask>
  SArray* triggerTaskList;       // SArray<SStreamTriggerTask>
  SArray* runnerTaskList;        // SArray<SStreamRunnerTask>
} SStreamTasksInfo;

typedef struct SStreamVgReaderTasks {
  SRWLatch lock;
  int64_t  streamVer;
  SArray*  taskList;       // SArray<SStreamTask*>
} SStreamVgReaderTasks;


typedef struct SStreamMgmtInfo {
  void*                  timer;
  void*                  dnode;
  int32_t                dnodeId;
  int32_t                snodeId;
  SStorageAPI            api;
  getMnodeEpsetFromDnode cb;
  SStreamHbInfo          hb;

  SRWLatch               vgroupLeadersLock;
  SArray*                vgroupLeaders;

  int8_t                 streamGrpIdx;
  SHashObj*              streamGrp[STREAM_MAX_GROUP_NUM]; // streamId => SStreamTasksInfo
  SHashObj*              taskMap;                         // streamId + taskId => SStreamTask*
  SHashObj*              vgroupMap;                       // vgId => SStreamVgReaderTasks

  SRWLatch               snodeLock;
  SArray*                snodeTasks;                      // SArray<SStreamTask*>
} SStreamMgmtInfo;


#ifdef __cplusplus
}
#endif

#endif /* ifndef TDENGINE_STREAM_INT_H */
#endif /* USE_STREAM */
