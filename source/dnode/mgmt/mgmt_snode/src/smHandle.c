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
 * along with this program. If not, see <http:www.gnu.org/licenses/>.
 */

#define _DEFAULT_SOURCE
#include "smInt.h"

void smGetMonitorInfo(SSnodeMgmt *pMgmt, SMonSmInfo *smInfo) {}

int32_t smProcessCreateReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t          code = 0;
  SDCreateSnodeReq createReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &createReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;
    return code;
  }

  if (pInput->pData->dnodeId != 0 && createReq.dnodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to create snode since %s", tstrerror(code));
    tFreeSMCreateQnodeReq(&createReq);
    return code;
  }

  bool deployed = true;
  if ((code = dmWriteFile(pInput->path, pInput->name, deployed)) != 0) {
    dError("failed to write snode file since %s", tstrerror(code));
    tFreeSMCreateQnodeReq(&createReq);
    return code;
  }

  tFreeSMCreateQnodeReq(&createReq);
  return 0;
}

int32_t smProcessDropReq(const SMgmtInputOpt *pInput, SRpcMsg *pMsg) {
  int32_t        code = 0;
  SDDropSnodeReq dropReq = {0};
  if (tDeserializeSCreateDropMQSNodeReq(pMsg->pCont, pMsg->contLen, &dropReq) != 0) {
    code = TSDB_CODE_INVALID_MSG;

    return code;
  }

  if (pInput->pData->dnodeId != 0 && dropReq.dnodeId != pInput->pData->dnodeId) {
    code = TSDB_CODE_INVALID_OPTION;
    dError("failed to drop snode since %s", tstrerror(code));
    tFreeSMCreateQnodeReq(&dropReq);
    return code;
  }

  bool deployed = false;
  if ((code = dmWriteFile(pInput->path, pInput->name, deployed)) != 0) {
    dError("failed to write snode file since %s", tstrerror(code));
    tFreeSMCreateQnodeReq(&dropReq);
    return code;
  }

  tFreeSMCreateQnodeReq(&dropReq);
  return 0;
}

SArray *smGetMsgHandles() {
  int32_t code = -1;
  SArray *pArray = taosArrayInit(4, sizeof(SMgmtHandle));
  if (pArray == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_CALC, smPutMsgToRunnerQueue, 1) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_FETCH, smPutMsgToRunnerQueue, 0) == NULL) goto _OVER;

  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_PULL_RSP, smPutMsgToTriggerQueue, 0) == NULL) goto _OVER;
  if (dmSetMgmtHandle(pArray, TDMT_STREAM_TRIGGER_CALC_RSP, smPutMsgToTriggerQueue, 0) == NULL) goto _OVER;

  code = 0;
  
_OVER:
  if (code != 0) {
    taosArrayDestroy(pArray);
    return NULL;
  } else {
    return pArray;
  }
}
