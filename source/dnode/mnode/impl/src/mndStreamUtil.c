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

#include "mndDb.h"
#include "mndStb.h"
#include "mndStream.h"
#include "mndTrans.h"
#include "mndVgroup.h"
#include "taoserror.h"
#include "tmisce.h"

int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId);

static int32_t mndAddSnodeInfo(SMnode *pMnode, SArray *pVgroupList) {
  SSnodeObj *pObj = NULL;
  void      *pIter = NULL;
  int32_t    code = 0;

  while (1) {
    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = SNODE_HANDLE};
    code = addEpIntoEpSet(&entry.epset, pObj->pDnode->fqdn, pObj->pDnode->port);
    if (code) {
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      mError("failed to extract epset for fqdn:%s during task vgroup snapshot", pObj->pDnode->fqdn);
      return code;
    }

    char buf[256] = {0};
    code = epsetToStr(&entry.epset, buf, tListLen(buf));
    if (code != 0) {  // print error and continue
      mError("failed to convert epset to str, code:%s", tstrerror(code));
    }

    void *p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      code = terrno;
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      mError("failed to put entry in vgroup list, nodeId:%d code:%s", entry.nodeId, tstrerror(code));
      return code;
    } else {
      mDebug("take snode snapshot, nodeId:%d %s", entry.nodeId, buf);
    }

    sdbRelease(pMnode->pSdb, pObj);
  }

  return code;
}

static int32_t mndCheckMnodeStatus(SMnode* pMnode) {
  int32_t    code = 0;
  ESdbStatus objStatus;
  void      *pIter = NULL;
  SMnodeObj *pObj = NULL;

  while (1) {
    pIter = sdbFetchAll(pMnode->pSdb, SDB_MNODE, pIter, (void **)&pObj, &objStatus, true);
    if (pIter == NULL) {
      break;
    }

    if (pObj->syncState != TAOS_SYNC_STATE_LEADER && pObj->syncState != TAOS_SYNC_STATE_FOLLOWER) {
      mDebug("mnode sync state:%d not leader/follower", pObj->syncState);
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      return TSDB_CODE_FAILED;
    }

    if (objStatus != SDB_STATUS_READY) {
      mWarn("mnode status:%d not ready", objStatus);
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      return TSDB_CODE_FAILED;
    }

    sdbRelease(pMnode->pSdb, pObj);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t mndCheckAndAddVgroupsInfo(SMnode *pMnode, SArray *pVgroupList, bool* allReady, SHashObj* pTermMap) {
  SSdb     *pSdb = pMnode->pSdb;
  void     *pIter = NULL;
  SVgObj   *pVgroup = NULL;
  int32_t   code = 0;
  SHashObj *pHash = NULL;

  pHash = taosHashInit(10, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), false, HASH_NO_LOCK);
  if (pHash == NULL) {
    mError("failed to prepare hashmap during take vgroup snapshot, code:%s", tstrerror(terrno));
    return terrno;
  }

  while (1) {
    pIter = sdbFetch(pSdb, SDB_VGROUP, pIter, (void **)&pVgroup);
    if (pIter == NULL) {
      break;
    }

    SNodeEntry entry = {.nodeId = pVgroup->vgId, .hbTimestamp = pVgroup->updateTime};
    entry.epset = mndGetVgroupEpset(pMnode, pVgroup);

    int8_t *pReplica = taosHashGet(pHash, &pVgroup->dbUid, sizeof(pVgroup->dbUid));
    if (pReplica == NULL) {  // not exist, add it into hash map
      code = taosHashPut(pHash, &pVgroup->dbUid, sizeof(pVgroup->dbUid), &pVgroup->replica, sizeof(pVgroup->replica));
      if (code) {
        mError("failed to put info into hashmap during task vgroup snapshot, code:%s", tstrerror(code));
        sdbRelease(pSdb, pVgroup);
        sdbCancelFetch(pSdb, pIter);
        goto _end;  // take snapshot failed, and not all ready
      }
    } else {
      if (*pReplica != pVgroup->replica) {
        mInfo("vgId:%d replica:%d inconsistent with other vgroups replica:%d, not ready for stream operations",
              pVgroup->vgId, pVgroup->replica, *pReplica);
        *allReady = false;  // task snap success, but not all ready
      }
    }

    // if not all ready till now, no need to check the remaining vgroups,
    // but still we need to put the info of the existed vgroups into the snapshot list
    if (*allReady) {
      *allReady = checkStatusForEachReplica(pVgroup);
    }

    char buf[256] = {0};
    code = epsetToStr(&entry.epset, buf, tListLen(buf));
    if (code != 0) {  // print error and continue
      mError("failed to convert epset to str, code:%s", tstrerror(code));
    }

    void *p = taosArrayPush(pVgroupList, &entry);
    if (p == NULL) {
      mError("failed to put entry in vgroup list, nodeId:%d code:out of memory", entry.nodeId);
      code = terrno;
      sdbRelease(pSdb, pVgroup);
      sdbCancelFetch(pSdb, pIter);
      goto _end;
    } else {
      mDebug("take node snapshot, nodeId:%d %s", entry.nodeId, buf);
    }

    if (pTermMap != NULL) {
      int64_t term = pVgroup->vnodeGid[0].syncTerm;
      code = taosHashPut(pTermMap, &pVgroup->vgId, sizeof(pVgroup->vgId), &term, sizeof(term));
      if (code) {
        mError("failed to put vnode:%d term into hashMap, code:%s", pVgroup->vgId, tstrerror(code));
      }
    }

    sdbRelease(pSdb, pVgroup);
  }

_end:
  taosHashCleanup(pHash);
  return code;
}

int32_t mndTakeVgroupSnapshot(SMnode *pMnode, bool *allReady, SArray **pList, SHashObj* pTermMap) {
  int32_t   code = 0;
  SArray   *pVgroupList = NULL;

  *pList = NULL;
  *allReady = true;

  pVgroupList = taosArrayInit(4, sizeof(SNodeEntry));
  if (pVgroupList == NULL) {
    mError("failed to prepare arraylist during take vgroup snapshot, code:%s", tstrerror(terrno));
    code = terrno;
    goto _err;
  }

  // 1. check for all vnodes status
  code = mndCheckAndAddVgroupsInfo(pMnode, pVgroupList, allReady, pTermMap);
  if (code) {
    goto _err;
  }

  // 2. add snode info
  code = mndAddSnodeInfo(pMnode, pVgroupList);
  if (code) {
    goto _err;
  }

  // 3. check for mnode status
  code = mndCheckMnodeStatus(pMnode);
  if (code != TSDB_CODE_SUCCESS) {
    *allReady = false;
  }

  *pList = pVgroupList;
  return code;

_err:
  *allReady = false;
  taosArrayDestroy(pVgroupList);
  return code;
}

int32_t mndGetStreamObj(SMnode *pMnode, int64_t streamId, SStreamObj **pStream) {
  void *pIter = NULL;
  SSdb *pSdb = pMnode->pSdb;
  *pStream = NULL;

  SStreamObj *p = NULL;
  while ((pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&p)) != NULL) {
    if (p->uid == streamId) {
      sdbCancelFetch(pSdb, pIter);
      *pStream = p;
      return TSDB_CODE_SUCCESS;
    }
    sdbRelease(pSdb, p);
  }

  return TSDB_CODE_STREAM_TASK_NOT_EXIST;
}

int32_t extractNodeEpset(SMnode *pMnode, SEpSet *pEpSet, bool *hasEpset, int32_t taskId, int32_t nodeId) {
  *hasEpset = false;

  pEpSet->numOfEps = 0;
  if (nodeId == SNODE_HANDLE) {
    SSnodeObj *pObj = NULL;
    void      *pIter = NULL;

    pIter = sdbFetch(pMnode->pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter != NULL) {
      int32_t code = addEpIntoEpSet(pEpSet, pObj->pDnode->fqdn, pObj->pDnode->port);
      sdbRelease(pMnode->pSdb, pObj);
      sdbCancelFetch(pMnode->pSdb, pIter);
      if (code) {
        *hasEpset = false;
        mError("failed to set epset");
      } else {
        *hasEpset = true;
      }
      return code;
    } else {
      mError("failed to acquire snode epset");
      return TSDB_CODE_INVALID_PARA;
    }
  } else {
    SVgObj *pVgObj = mndAcquireVgroup(pMnode, nodeId);
    if (pVgObj != NULL) {
      SEpSet epset = mndGetVgroupEpset(pMnode, pVgObj);
      mndReleaseVgroup(pMnode, pVgObj);

      epsetAssign(pEpSet, &epset);
      *hasEpset = true;
      return TSDB_CODE_SUCCESS;
    } else {
      mDebug("orphaned task:0x%x need to be dropped, nodeId:%d, no redo action", taskId, nodeId);
      return TSDB_CODE_SUCCESS;
    }
  }
}

int32_t mndGetStreamTask(STaskId *pId, SStreamObj *pStream, SStreamTask **pTask) {
  *pTask = NULL;

  SStreamTask     *p = NULL;
  SStreamTaskIter *pIter = NULL;
  int32_t          code = createStreamTaskIter(pStream, &pIter);
  if (code) {
    mError("failed to create stream task iter:%s", pStream->name);
    return code;
  }

  while (streamTaskIterNextTask(pIter)) {
    code = streamTaskIterGetCurrent(pIter, &p);
    if (code) {
      continue;
    }

    if (p->id.taskId == pId->taskId) {
      destroyStreamTaskIter(pIter);
      *pTask = p;
      return 0;
    }
  }

  destroyStreamTaskIter(pIter);
  return TSDB_CODE_FAILED;
}

int32_t mndGetNumOfStreamTasks(const SStreamObj *pStream) {
  int32_t num = 0;
  for (int32_t i = 0; i < taosArrayGetSize(pStream->pTaskList); ++i) {
    SArray *pLevel = taosArrayGetP(pStream->pTaskList, i);
    num += taosArrayGetSize(pLevel);
  }

  return num;
}

int32_t mndGetNumOfStreams(SMnode *pMnode, char *dbName, int32_t *pNumOfStreams) {
  SSdb   *pSdb = pMnode->pSdb;
  SDbObj *pDb = mndAcquireDb(pMnode, dbName);
  if (pDb == NULL) {
    TAOS_RETURN(TSDB_CODE_MND_DB_NOT_SELECTED);
  }

  int32_t numOfStreams = 0;
  void   *pIter = NULL;
  while (1) {
    SStreamObj *pStream = NULL;
    pIter = sdbFetch(pSdb, SDB_STREAM, pIter, (void **)&pStream);
    if (pIter == NULL) break;

    if (pStream->sourceDbUid == pDb->uid) {
      numOfStreams++;
    }

    sdbRelease(pSdb, pStream);
  }

  *pNumOfStreams = numOfStreams;
  mndReleaseDb(pMnode, pDb);
  return 0;
}

static void freeTaskList(void *param) {
  SArray **pList = (SArray **)param;
  taosArrayDestroy(*pList);
}

void removeExpiredNodeInfo(const SArray *pNodeSnapshot) {
  SArray *pValidList = taosArrayInit(4, sizeof(SNodeEntry));
  if (pValidList == NULL) {  // not continue
    return;
  }

  int32_t size = taosArrayGetSize(pNodeSnapshot);
  int32_t oldSize = taosArrayGetSize(execInfo.pNodeList);

  for (int32_t i = 0; i < oldSize; ++i) {
    SNodeEntry *p = taosArrayGet(execInfo.pNodeList, i);
    if (p == NULL) {
      continue;
    }

    for (int32_t j = 0; j < size; ++j) {
      SNodeEntry *pEntry = taosArrayGet(pNodeSnapshot, j);
      if (pEntry == NULL) {
        continue;
      }

      if (pEntry->nodeId == p->nodeId) {
        p->hbTimestamp = pEntry->hbTimestamp;

        void *px = taosArrayPush(pValidList, p);
        if (px == NULL) {
          mError("failed to put node into list, nodeId:%d", p->nodeId);
        } else {
          mDebug("vgId:%d ts:%" PRId64 " HbMsgId:%d is valid", p->nodeId, p->hbTimestamp, p->lastHbMsgId);
        }
        break;
      }
    }
  }

  taosArrayDestroy(execInfo.pNodeList);
  execInfo.pNodeList = pValidList;

  mDebug("remain %d valid node entries after clean expired nodes info, prev size:%d",
         (int32_t)taosArrayGetSize(pValidList), oldSize);
}

int32_t doRemoveTasks(SStreamExecInfo *pExecNode, STaskId *pRemovedId) {
  void *p = taosHashGet(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (p == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = taosHashRemove(pExecNode->pTaskMap, pRemovedId, sizeof(*pRemovedId));
  if (code) {
    return code;
  }

  for (int32_t k = 0; k < taosArrayGetSize(pExecNode->pTaskList); ++k) {
    STaskId *pId = taosArrayGet(pExecNode->pTaskList, k);
    if (pId == NULL) {
      continue;
    }

    if (pId->taskId == pRemovedId->taskId && pId->streamId == pRemovedId->streamId) {
      taosArrayRemove(pExecNode->pTaskList, k);

      int32_t num = taosArrayGetSize(pExecNode->pTaskList);
      mInfo("s-task:0x%x removed from buffer, remain:%d in buffer list", (int32_t)pRemovedId->taskId, num);
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

void removeTasksInBuf(SArray *pTaskIds, SStreamExecInfo *pExecInfo) {
  for (int32_t i = 0; i < taosArrayGetSize(pTaskIds); ++i) {
    STaskId *pId = taosArrayGet(pTaskIds, i);
    if (pId == NULL) {
      continue;
    }

    int32_t code = doRemoveTasks(pExecInfo, pId);
    if (code) {
      mError("failed to remove task in buffer list, 0x%" PRIx64, pId->taskId);
    }
  }
}

static bool taskNodeExists(SArray *pList, int32_t nodeId) {
  size_t num = taosArrayGetSize(pList);

  for (int32_t i = 0; i < num; ++i) {
    SNodeEntry *pEntry = taosArrayGet(pList, i);
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->nodeId == nodeId) {
      return true;
    }
  }

  return false;
}

int32_t removeExpiredNodeEntryAndTaskInBuf(SArray *pNodeSnapshot) {
  SArray *pRemovedTasks = taosArrayInit(4, sizeof(STaskId));
  if (pRemovedTasks == NULL) {
    return terrno;
  }

  int32_t numOfTask = taosArrayGetSize(execInfo.pTaskList);
  for (int32_t i = 0; i < numOfTask; ++i) {
    STaskId *pId = taosArrayGet(execInfo.pTaskList, i);
    if (pId == NULL) {
      continue;
    }

    STaskStatusEntry *pEntry = taosHashGet(execInfo.pTaskMap, pId, sizeof(*pId));
    if (pEntry == NULL) {
      continue;
    }

    if (pEntry->nodeId == SNODE_HANDLE) {
      continue;
    }

    bool existed = taskNodeExists(pNodeSnapshot, pEntry->nodeId);
    if (!existed) {
      void *p = taosArrayPush(pRemovedTasks, pId);
      if (p == NULL) {
        mError("failed to put task entry into remove list, taskId:0x%" PRIx64, pId->taskId);
      }
    }
  }

  removeTasksInBuf(pRemovedTasks, &execInfo);

  mDebug("remove invalid stream tasks:%d, remain:%d", (int32_t)taosArrayGetSize(pRemovedTasks),
         (int32_t)taosArrayGetSize(execInfo.pTaskList));

  removeExpiredNodeInfo(pNodeSnapshot);

  taosArrayDestroy(pRemovedTasks);
  return 0;
}

int32_t mndClearConsensusCheckpointId(SHashObj *pHash, int64_t streamId) {
  int32_t code = 0;
  int32_t numOfStreams = taosHashGetSize(pHash);
  if (numOfStreams == 0) {
    return code;
  }

  code = taosHashRemove(pHash, &streamId, sizeof(streamId));
  if (code == 0) {
    numOfStreams = taosHashGetSize(pHash);
    mDebug("drop stream:0x%" PRIx64 " in consensus-checkpointId list, remain:%d", streamId, numOfStreams);
  } else {
    mError("failed to remove stream:0x%" PRIx64 " in consensus-checkpointId list, remain:%d", streamId, numOfStreams);
  }

  return code;
}

int32_t mndClearChkptReportInfo(SHashObj *pHash, int64_t streamId) {
  int32_t code = 0;
  int32_t numOfStreams = taosHashGetSize(pHash);
  if (numOfStreams == 0) {
    return code;
  }

  code = taosHashRemove(pHash, &streamId, sizeof(streamId));
  if (code == 0) {
    mDebug("drop stream:0x%" PRIx64 " in chkpt-report list, remain:%d", streamId, numOfStreams);
  } else {
    mError("failed to remove stream:0x%" PRIx64 " in chkpt-report list, remain:%d", streamId, numOfStreams);
  }

  return code;
}

static void mndShowStreamStatus(char *dst, int8_t status) {
  if (status == STREAM_STATUS__NORMAL) {
    tstrncpy(dst, "ready", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__STOP) {
    tstrncpy(dst, "stop", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__FAILED) {
    tstrncpy(dst, "failed", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__RECOVER) {
    tstrncpy(dst, "recover", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__PAUSE) {
    tstrncpy(dst, "paused", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (status == STREAM_STATUS__INIT) {
    tstrncpy(dst, "init", MND_STREAM_TRIGGER_NAME_SIZE);
  }
}

static void mndShowStreamTrigger(char *dst, SStreamObj *pStream) {
  int8_t trigger = pStream->conf.trigger;
  if (trigger == STREAM_TRIGGER_AT_ONCE) {
    tstrncpy(dst, "at once", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (trigger == STREAM_TRIGGER_WINDOW_CLOSE) {
    tstrncpy(dst, "window close", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (trigger == STREAM_TRIGGER_MAX_DELAY) {
    tstrncpy(dst, "max delay", MND_STREAM_TRIGGER_NAME_SIZE);
  } else if (trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE) {
    tstrncpy(dst, "force window close", MND_STREAM_TRIGGER_NAME_SIZE);
  }
}

static void int64ToHexStr(int64_t id, char *pBuf, int32_t bufLen) {
  memset(pBuf, 0, bufLen);
  pBuf[2] = '0';
  pBuf[3] = 'x';

  int32_t len = tintToHex(id, &pBuf[4]);
  varDataSetLen(pBuf, len + 2);
}

static int32_t isAllTaskPaused(SStreamObj *pStream, bool *pRes) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SStreamTaskIter *pIter = NULL;
  bool             isPaused =  true;

  taosRLockLatch(&pStream->lock);
  code = createStreamTaskIter(pStream, &pIter);
  TSDB_CHECK_CODE(code, lino, _end);

  while (streamTaskIterNextTask(pIter)) {
    SStreamTask *pTask = NULL;
    code = streamTaskIterGetCurrent(pIter, &pTask);
    TSDB_CHECK_CODE(code, lino, _end);

    STaskId           id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};
    STaskStatusEntry *pe = taosHashGet(execInfo.pTaskMap, &id, sizeof(id));
    if (pe == NULL) {
      continue;
    }
    if (pe->status != TASK_STATUS__PAUSE) {
      isPaused = false;
    }
  }
  (*pRes) = isPaused;

_end:
  destroyStreamTaskIter(pIter);
  taosRUnLockLatch(&pStream->lock);
  if (code != TSDB_CODE_SUCCESS) {
    mError("error happens when get stream status, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}

int32_t setStreamAttrInResBlock(SStreamObj *pStream, SSDataBlock *pBlock, int32_t numOfRows) {
  int32_t code = 0;
  int32_t cols = 0;
  int32_t lino = 0;

  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));
  SColumnInfoData *pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // create time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->createTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stream id
  char buf[128] = {0};
  int64ToHexStr(pStream->uid, buf, tListLen(buf));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, buf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  if (pStream->hTaskUid != 0) {
    int64ToHexStr(pStream->hTaskUid, buf, tListLen(buf));
    code = colDataSetVal(pColInfo, numOfRows, buf, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, buf, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // related fill-history stream id
  char sql[TSDB_SHOW_SQL_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sql, pStream->sql, sizeof(sql));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)sql, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char status[20 + VARSTR_HEADER_SIZE] = {0};
  char status2[MND_STREAM_TRIGGER_NAME_SIZE] = {0};
  bool isPaused = false;
  code = isAllTaskPaused(pStream, &isPaused);
  TSDB_CHECK_CODE(code, lino, _end);

  int8_t streamStatus = atomic_load_8(&pStream->status);
  if (isPaused && pStream->pTaskList != NULL) {
    streamStatus = STREAM_STATUS__PAUSE;
  }
  mndShowStreamStatus(status2, streamStatus);
  STR_WITH_MAXSIZE_TO_VARSTR(status, status2, sizeof(status));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char sourceDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(sourceDB, mndGetDbStr(pStream->sourceDb), sizeof(sourceDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&sourceDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char targetDB[TSDB_DB_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(targetDB, mndGetDbStr(pStream->targetDb), sizeof(targetDB));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetDB, false);
  TSDB_CHECK_CODE(code, lino, _end);

  if (pStream->targetSTbName[0] == 0) {
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    code = colDataSetVal(pColInfo, numOfRows, NULL, true);
  } else {
    char targetSTB[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
    STR_WITH_MAXSIZE_TO_VARSTR(targetSTB, mndGetStbStr(pStream->targetSTbName), sizeof(targetSTB));
    pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
    TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)&targetSTB, false);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pStream->conf.watermark, false);
  TSDB_CHECK_CODE(code, lino, _end);

  char trigger[20 + VARSTR_HEADER_SIZE] = {0};
  char trigger2[MND_STREAM_TRIGGER_NAME_SIZE] = {0};
  mndShowStreamTrigger(trigger2, pStream);
  STR_WITH_MAXSIZE_TO_VARSTR(trigger, trigger2, sizeof(trigger));
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&trigger, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // sink_quota
  char sinkQuota[20 + VARSTR_HEADER_SIZE] = {0};
  sinkQuota[0] = '0';
  char dstStr[20] = {0};
  STR_TO_VARSTR(dstStr, sinkQuota)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);
  TSDB_CHECK_CODE(code, lino, _end);


  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  // checkpoint backup type
  char backup[20 + VARSTR_HEADER_SIZE] = {0};
  STR_TO_VARSTR(backup, "none")
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)backup, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // history scan idle
  char scanHistoryIdle[20 + VARSTR_HEADER_SIZE] = {0};
  tstrncpy(scanHistoryIdle, "100a", sizeof(scanHistoryIdle));

  memset(dstStr, 0, tListLen(dstStr));
  STR_TO_VARSTR(dstStr, scanHistoryIdle)
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)dstStr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);
  char msg[TSDB_RESERVE_VALUE_LEN + VARSTR_HEADER_SIZE] = {0};
  if (streamStatus == STREAM_STATUS__FAILED){
    STR_TO_VARSTR(msg, pStream->reserve)
  } else {
    STR_TO_VARSTR(msg, " ")
  }
  code = colDataSetVal(pColInfo, numOfRows, (const char *)msg, false);

_end:
  if (code) {
    mError("error happens when build stream attr result block, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}

int32_t setTaskAttrInResBlock(SStreamObj *pStream, SStreamTask *pTask, SSDataBlock *pBlock, int32_t numOfRows,
                              int32_t precision) {
  SColumnInfoData *pColInfo = NULL;
  int32_t          cols = 0;
  int32_t          code = 0;
  int32_t          lino = 0;

  STaskId id = {.streamId = pTask->id.streamId, .taskId = pTask->id.taskId};

  STaskStatusEntry *pe = taosHashGet(execInfo.pTaskMap, &id, sizeof(id));
  if (pe == NULL) {
    mError("task:0x%" PRIx64 " not exists in any vnodes, streamName:%s, streamId:0x%" PRIx64 " createTs:%" PRId64
           " no valid status/stage info",
           id.taskId, pStream->name, pStream->uid, pStream->createTime);
    return TSDB_CODE_STREAM_TASK_NOT_EXIST;
  }

  // stream name
  char streamName[TSDB_TABLE_NAME_LEN + VARSTR_HEADER_SIZE] = {0};
  STR_WITH_MAXSIZE_TO_VARSTR(streamName, mndGetDbStr(pStream->name), sizeof(streamName));

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)streamName, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // task id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  char idstr[128] = {0};
  int64ToHexStr(pTask->id.taskId, idstr, tListLen(idstr));
  code = colDataSetVal(pColInfo, numOfRows, idstr, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node type
  char nodeType[20 + VARSTR_HEADER_SIZE] = {0};
  varDataSetLen(nodeType, 5);
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pTask->info.nodeId > 0) {
    memcpy(varDataVal(nodeType), "vnode", 5);
  } else {
    memcpy(varDataVal(nodeType), "snode", 5);
  }
  code = colDataSetVal(pColInfo, numOfRows, nodeType, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // node id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  int64_t nodeId = TMAX(pTask->info.nodeId, 0);
  code = colDataSetVal(pColInfo, numOfRows, (const char *)&nodeId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // level
  char level[20 + VARSTR_HEADER_SIZE] = {0};
  if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {
    STR_WITH_SIZE_TO_VARSTR(level, "source", 6);
  } else if (pTask->info.taskLevel == TASK_LEVEL__AGG) {
    STR_WITH_SIZE_TO_VARSTR(level, "agg", 3);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    STR_WITH_SIZE_TO_VARSTR(level, "sink", 4);
  } else if (pTask->info.taskLevel == TASK_LEVEL__MERGE) {
    STR_WITH_SIZE_TO_VARSTR(level, "merge", 5);
  }

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)level, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // status
  char status[20 + VARSTR_HEADER_SIZE] = {0};

  const char *pStatus = streamTaskGetStatusStr(pe->status);
  STR_TO_VARSTR(status, pStatus);

  // status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)status, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // stage
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->stage, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // input queue
  char        vbuf[TSDB_STREAM_NOTIFY_STAT_LEN + 2] = {0};
  char        buf[TSDB_STREAM_NOTIFY_STAT_LEN] = {0};
  const char *queueInfoStr = "%4.2f MiB (%6.2f%)";
  snprintf(buf, tListLen(buf), queueInfoStr, pe->inputQUsed, pe->inputRate);
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // input total
  const char *formatTotalMb = "%7.2f MiB";
  const char *formatTotalGb = "%7.2f GiB";
  if (pe->procsTotal < 1024) {
    snprintf(buf, tListLen(buf), formatTotalMb, pe->procsTotal);
  } else {
    snprintf(buf, tListLen(buf), formatTotalGb, pe->procsTotal / 1024);
  }

  memset(vbuf, 0, tListLen(vbuf));
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // process throughput
  const char *formatKb = "%7.2f KiB/s";
  const char *formatMb = "%7.2f MiB/s";
  if (pe->procsThroughput < 1024) {
    snprintf(buf, tListLen(buf), formatKb, pe->procsThroughput);
  } else {
    snprintf(buf, tListLen(buf), formatMb, pe->procsThroughput / 1024);
  }

  memset(vbuf, 0, tListLen(vbuf));
  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // output total
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    colDataSetNULL(pColInfo, numOfRows);
  } else {
    (void)tsnprintf(buf, sizeof(buf), formatTotalMb, pe->outputTotal);
    memset(vbuf, 0, tListLen(vbuf));
    STR_TO_VARSTR(vbuf, buf);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
    TSDB_CHECK_CODE(code, lino, _end);
  }

  // output throughput
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    colDataSetNULL(pColInfo, numOfRows);
  } else {
    if (pe->outputThroughput < 1024) {
      snprintf(buf, tListLen(buf), formatKb, pe->outputThroughput);
    } else {
      snprintf(buf, tListLen(buf), formatMb, pe->outputThroughput / 1024);
    }

    memset(vbuf, 0, tListLen(vbuf));
    STR_TO_VARSTR(vbuf, buf);

    code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
    TSDB_CHECK_CODE(code, lino, _end);
  }
  // info
  if (pTask->info.taskLevel == TASK_LEVEL__SINK) {
    const char *sinkStr = "%.2f MiB";
    snprintf(buf, tListLen(buf), sinkStr, pe->sinkDataSize);
  } else if (pTask->info.taskLevel == TASK_LEVEL__SOURCE) {  // offset info
    if (pTask->info.trigger == STREAM_TRIGGER_FORCE_WINDOW_CLOSE) {
      int32_t ret = taosFormatUtcTime(buf, tListLen(buf), pe->processedVer, precision);
      if (ret != 0) {
        mError("failed to format processed timewindow, skey:%" PRId64, pe->processedVer);
        memset(buf, 0, tListLen(buf));
      }
    } else {
      const char *offsetStr = "%" PRId64 " [%" PRId64 ", %" PRId64 "]";
      snprintf(buf, tListLen(buf), offsetStr, pe->processedVer, pe->verRange.minVer, pe->verRange.maxVer);
    }
  } else {
    memset(buf, 0, tListLen(buf));
  }

  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start_time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startTime, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startCheckpointId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // start ver
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->startCheckpointVer, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint time
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pe->checkpointInfo.latestTime != 0) {
    code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestTime, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, 0, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint_id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestId, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint version
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, (const char *)&pe->checkpointInfo.latestVer, false);
  TSDB_CHECK_CODE(code, lino, _end);

  // checkpoint size
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  colDataSetNULL(pColInfo, numOfRows);

  // checkpoint backup status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  // ds_err_info
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  // history_task_id
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (pe->hTaskId != 0) {
    int64ToHexStr(pe->hTaskId, idstr, tListLen(idstr));
    code = colDataSetVal(pColInfo, numOfRows, idstr, false);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, 0, true);
  }
  TSDB_CHECK_CODE(code, lino, _end);

  // history_task_status
  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  code = colDataSetVal(pColInfo, numOfRows, 0, true);
  TSDB_CHECK_CODE(code, lino, _end);

  // notify_event_stat
  int32_t offset =0;
  if (pe->notifyEventStat.notifyEventAddTimes > 0) {
    offset += tsnprintf(buf + offset, sizeof(buf) - offset, "Add %" PRId64 "x, %" PRId64 " elems in %lfs; ",
                        pe->notifyEventStat.notifyEventAddTimes, pe->notifyEventStat.notifyEventAddElems,
                        pe->notifyEventStat.notifyEventAddCostSec);
  }
  if (pe->notifyEventStat.notifyEventPushTimes > 0) {
    offset += tsnprintf(buf + offset, sizeof(buf) - offset, "Push %" PRId64 "x, %" PRId64 " elems in %lfs; ",
                        pe->notifyEventStat.notifyEventPushTimes, pe->notifyEventStat.notifyEventPushElems,
                        pe->notifyEventStat.notifyEventPushCostSec);
  }
  if (pe->notifyEventStat.notifyEventPackTimes > 0) {
    offset += tsnprintf(buf + offset, sizeof(buf) - offset, "Pack %" PRId64 "x, %" PRId64 " elems in %lfs; ",
                        pe->notifyEventStat.notifyEventPackTimes, pe->notifyEventStat.notifyEventPackElems,
                        pe->notifyEventStat.notifyEventPackCostSec);
  }
  if (pe->notifyEventStat.notifyEventSendTimes > 0) {
    offset += tsnprintf(buf + offset, sizeof(buf) - offset, "Send %" PRId64 "x, %" PRId64 " elems in %lfs; ",
                        pe->notifyEventStat.notifyEventSendTimes, pe->notifyEventStat.notifyEventSendElems,
                        pe->notifyEventStat.notifyEventSendCostSec);
  }
  if (pe->notifyEventStat.notifyEventHoldElems > 0) {
    offset += tsnprintf(buf + offset, sizeof(buf) - offset, "[Hold %" PRId64 " elems] ",
                        pe->notifyEventStat.notifyEventHoldElems);
  }
  TSDB_CHECK_CONDITION(offset < sizeof(buf), code, lino, _end, TSDB_CODE_INTERNAL_ERROR);
  buf[offset] = '\0';

  STR_TO_VARSTR(vbuf, buf);

  pColInfo = taosArrayGet(pBlock->pDataBlock, cols++);
  TSDB_CHECK_NULL(pColInfo, code, lino, _end, terrno);

  if (offset == 0) {
    colDataSetNULL(pColInfo, numOfRows);
  } else {
    code = colDataSetVal(pColInfo, numOfRows, (const char *)vbuf, false);
    TSDB_CHECK_CODE(code, lino, _end);
  }

_end:
  if (code) {
    mError("error happens during build task attr result blocks, lino:%d, code:%s", lino, tstrerror(code));
  }
  return code;
}

static bool isNodeEpsetChanged(const SEpSet *pPrevEpset, const SEpSet *pCurrent) {
  const SEp *pEp = GET_ACTIVE_EP(pPrevEpset);
  const SEp *p = GET_ACTIVE_EP(pCurrent);

  if (pEp->port == p->port && strncmp(pEp->fqdn, p->fqdn, TSDB_FQDN_LEN) == 0) {
    return false;
  }
  return true;
}

void mndDestroyVgroupChangeInfo(SVgroupChangeInfo *pInfo) {
  if (pInfo != NULL) {
    taosArrayDestroy(pInfo->pUpdateNodeList);
    taosHashCleanup(pInfo->pDBMap);
  }
}

// 1. increase the replica does not affect the stream process.
// 2. decreasing the replica may affect the stream task execution in the way that there is one or more running stream
// tasks on the will be removed replica.
// 3. vgroup redistribution is an combination operation of first increase replica and then decrease replica. So we
// will handle it as mentioned in 1 & 2 items.
int32_t mndFindChangedNodeInfo(SMnode *pMnode, const SArray *pPrevNodeList, const SArray *pNodeList,
                               SVgroupChangeInfo *pInfo) {
  int32_t code = 0;
  int32_t lino = 0;

  if (pInfo == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  pInfo->pUpdateNodeList = taosArrayInit(4, sizeof(SNodeUpdateInfo));
  pInfo->pDBMap = taosHashInit(32, taosGetDefaultHashFunction(TSDB_DATA_TYPE_VARCHAR), true, HASH_NO_LOCK);

  if (pInfo->pUpdateNodeList == NULL || pInfo->pDBMap == NULL) {
    mndDestroyVgroupChangeInfo(pInfo);
    TSDB_CHECK_NULL(NULL, code, lino, _err, terrno);
  }

  int32_t numOfNodes = taosArrayGetSize(pPrevNodeList);
  for (int32_t i = 0; i < numOfNodes; ++i) {
    SNodeEntry *pPrevEntry = taosArrayGet(pPrevNodeList, i);
    if (pPrevEntry == NULL) {
      continue;
    }

    int32_t num = taosArrayGetSize(pNodeList);
    for (int32_t j = 0; j < num; ++j) {
      SNodeEntry *pCurrent = taosArrayGet(pNodeList, j);
      if (pCurrent == NULL) {
        continue;
      }

      if (pCurrent->nodeId == pPrevEntry->nodeId) {
        if (pPrevEntry->stageUpdated || isNodeEpsetChanged(&pPrevEntry->epset, &pCurrent->epset)) {
          const SEp *pPrevEp = GET_ACTIVE_EP(&pPrevEntry->epset);

          char buf[256] = {0};
          code = epsetToStr(&pCurrent->epset, buf, tListLen(buf));  // ignore this error
          if (code) {
            mError("failed to convert epset string, code:%s", tstrerror(code));
            TSDB_CHECK_CODE(code, lino, _err);
          }

          mDebug("nodeId:%d restart/epset changed detected, old:%s:%d -> new:%s, stageUpdate:%d", pCurrent->nodeId,
                 pPrevEp->fqdn, pPrevEp->port, buf, pPrevEntry->stageUpdated);

          SNodeUpdateInfo updateInfo = {.nodeId = pPrevEntry->nodeId};
          epsetAssign(&updateInfo.prevEp, &pPrevEntry->epset);
          epsetAssign(&updateInfo.newEp, &pCurrent->epset);

          void *p = taosArrayPush(pInfo->pUpdateNodeList, &updateInfo);
          TSDB_CHECK_NULL(p, code, lino, _err, terrno);
        }

        // todo handle the snode info
        if (pCurrent->nodeId != SNODE_HANDLE) {
          SVgObj *pVgroup = mndAcquireVgroup(pMnode, pCurrent->nodeId);
          code = taosHashPut(pInfo->pDBMap, pVgroup->dbName, strlen(pVgroup->dbName), NULL, 0);
          mndReleaseVgroup(pMnode, pVgroup);
          TSDB_CHECK_CODE(code, lino, _err);
        }

        break;
      }
    }
  }

  return code;

_err:
  mError("failed to find node change info, code:%s at %s line:%d", tstrerror(code), __func__, lino);
  mndDestroyVgroupChangeInfo(pInfo);
  return code;
}


int32_t mndStreamCheckSnodeExists(SMnode *pMnode) {
  SSdb      *pSdb = pMnode->pSdb;
  void      *pIter = NULL;
  SSnodeObj *pObj = NULL;

  while (1) {
    pIter = sdbFetch(pSdb, SDB_SNODE, pIter, (void **)&pObj);
    if (pIter == NULL) {
      break;
    }

    sdbRelease(pSdb, pObj);
    sdbCancelFetch(pSdb, pIter);
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SNODE_NOT_DEPLOYED;
}
