###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from collections import defaultdict
import random
import string
import threading
import requests
import time
import taos

from .log import *
from .sql import *
from .server.dnodes import *
from .common import *
from datetime import datetime
from enum import Enum
from new_test_framework.utils import clusterComCheck

class StreamTableType(Enum):
    TYPE_SUP_TABLE = "SUP_TABLE"
    TYPE_SUB_TABLE = "SUB_TABLE"
    TYPE_NORMAL_TABLE = "NORMAL_TABLE"
    TYPE_VIRTUAL_TABLE = "VIRTUAL_TABLE"
    TYPE_SYSTEM_TABLE = "SYSTEM_TABLE"


class StreamTable:
    def __init__(self):
        self.streamTableType = StreamTableType.TYPE_SUP_TABLE
        self.precision = "ms"
        self.start="2025-01-01 00.00.00"
        self.interval=30

class StreamUtil:
    def __init__(self):
        self.streamTableType = StreamTableType.TYPE_SUP_TABLE
        self.calTableType = StreamTableType.TYPE_SUP_TABLE
        self.qdb = "qdb"
        self.tdb = "tdb"
        self.rdb = "rdb"
        self.qdbPrecision = "ms"
        self.tdbPrecision = "ms"
        self.rdbPrecision = "ms"
        self.vgroups = 1
        self.start="2025-01-01 00.00.00"
        self.interval=30
        
        self.tbBatch=int(2)
        self.tbPerBatch=100
        self.rowBatch=2
        self.rowsPerBatch=500
        self.initRow = self.rowBatch * self.rowsPerBatch
        
        self.triggerSTB = "trigger_st"
        self.triggerNTB = "tntb"
        self.calSTB = "meters"
        self.calNTB = "cntb"
        
        self.streamTableCols = (f"("
            "  cts timestamp"
            ", cint int"
            ", cuint int unsigned"
            ", cbigint bigint"
            ", cubigint bigint unsigned"
            ", cfloat float"
            ", cdouble double"
            ", cvarchar varchar(32)"
            ", csmallint smallint"
            ", cusmallint smallint unsigned"
            ", ctinyint tinyint"
            ", cutinyint tinyint unsigned"
            ", cbool bool"
            ", cnchar nchar(32)"
            ", cvarbinary varbinary(32)"
            ", cdecimal8 decimal(8)"
            ", cdecimal16 decimal(16)"
            ", cgeometry geometry(32)"
            ")")

    def setTriggerTableType(self, streamTableType):
        self.streamTableType = streamTableType
    def setCalTableType(self, calTableType):
        self.calTableType = calTableType
        
    def setQdb(self, qdb, precision="ms"):
        self.qdb = qdb
        self.qdbPrecision = precision
    def setTdb(self, tdb, precision="ms"):
        self.tdb = tdb
        self.tdbPrecision = precision
    def setRdb(self, rdb, precision="ms"):
        self.rdb = rdb
        self.rdbPrecision = precision
    def setInitRow(self, initRow):
        self.initRow = initRow
    def setSubTableCount(self, count):
        self.tbBatch = int(count / 100 if count % 100 == 0 else count / 100 + 1)
        self.tbPerBatch = 100 if count >= 100 else count
        
    def init_databases(self):
        tdLog.info(f"create databases")
        unique_dbs = {self.qdb, self.tdb, self.rdb}

        for db in unique_dbs:
            tdSql.prepare(dbname=db, vgroups=self.vgroups)
            clusterComCheck.checkDbReady(db)
            
    def init_databases(self, db):
        tdLog.info(f"create databases {db}")
        tdSql.prepare(dbname=db, vgroups=self.vgroups)
        clusterComCheck.checkDbReady(db)

    def createTable(self, db, tb, type, subTableCount=200):
        if type == StreamTableType.TYPE_SUP_TABLE or type == StreamTableType.TYPE_SUB_TABLE:
            tdLog.info(f"create super table {db}.{tb}")
            self.tbBatch= int(subTableCount / 100 if subTableCount % 100 == 0 else subTableCount / 100 + 1)
            self.tbPerBatch= 100 if subTableCount >= 100 else subTableCount
            self.createDefaultSupTable(db, tb)
            self.createDefaultSubTables(db, tb)
        elif type == StreamTableType.TYPE_NORMAL_TABLE:
            tdLog.info(f"create normal table {db}.{tb}")
            self.createDBDeafaultNormalTable(db, tb)       
            
    def createDefaultSubTables(self, db, stb):        
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        if self.tdbPrecision == "us":
            prec = 1000 * 1000 * 1000
        elif self.tdbPrecision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec

        totalTables = self.tbBatch * self.tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        for batch in range(self.tbBatch):
            sql = "create table "
            for tb in range(self.tbPerBatch):
                table = batch * self.tbPerBatch + tb
                tts = tsStart if table % 3 == 1 else tsNext
                tint = table % 3 if table % 20 != 1 else "NULL"
                tuint = table % 4
                tbigint = table % 5
                tubigint = table % 6
                tfloat = table % 7
                tdouble = table % 8
                tvarchar = "SanFrancisco" if table % 3 == 1 else "LosAngeles"
                tsmallint = table % 9
                tusmallint = table % 10
                ttinyint = table % 11
                tutinyint = table % 12
                tbool = table % 2
                tnchar = tvarchar
                tvarbinary = tvarchar
                tgeometry = "POINT(1.0 1.0)" if table % 3 == 1 else "POINT(2.0 2.0)"
                sql += f"{db}.{stb}_{table} using {db}.{stb} tags({tts}, '{tint}', {tuint}, {tbigint}, {tubigint}, {tfloat}, {tdouble}, '{tvarchar}', {tsmallint}, {tusmallint}, {ttinyint}, {tutinyint}, {tbool}, '{tnchar}', '{tvarbinary}', '{tgeometry}') "
            tdSql.execute(sql)
            
    def createDefaultSupTable(
        self,
        db="qdb",
        stb="meters"
        ):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create stable {db}.{stb} {self.streamTableCols} tags("
            "  tts timestamp"
            ", tint int"
            ", tuint int unsigned"
            ", tbigint bigint"
            ", tubigint bigint unsigned"
            ", tfloat float"
            ", tdouble double"
            ", tvarchar varchar(32)"
            ", tsmallint smallint"
            ", tusmallint smallint unsigned"
            ", ttinyint tinyint"
            ", tutinyint tinyint unsigned"
            ", tbool bool"
            ", tnchar nchar(16)"
            ", tvarbinary varbinary(32)"
            ", tgeometry geometry(32)"
            ")"
        )
    
    def createDBDeafaultNormalTable(self, db, tb):
        tdLog.info(f"create normal table")
        tdSql.execute(
            f"create table {db}.{tb} {self.streamTableCols}"
        )

    def appendTableData(self, fullTbName, startRow, endRows):
        if self.tdbPrecision == "us":
            prec = 1000 * 1000 * 1000
        elif self.tdbPrecision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000
                        
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        tsStart = int(dt.timestamp() * prec)
        tsInterval = self.interval * prec
        
        start = startRow
        end = endRows
        while start < endRows:
            end = endRows if start + self.rowsPerBatch > endRows else start + self.rowsPerBatch
            sql = f"insert into {fullTbName} values "
            for rows in range(startRow, endRows):
                ts = tsStart + rows * tsInterval
                cint = rows % 10
                cuint = rows
                cbigint = rows
                cubigint = rows
                cfloat = rows
                cdouble = rows
                cvarchar = f"cvar_{rows}"
                csmallint = rows
                cusmallint = rows
                ctinyint = rows % 128
                cutinyint = rows % 256
                cbool = rows % 2 if rows % 20 != 1 else "NULL"
                cnchar = cvarchar
                cvarbinary = cvarchar
                cdecimal8 = "0" if rows % 3 == 1 else "8"
                cdecimal16 = "4" if rows % 3 == 1 else "16"
                cgeometry = "POINT(1.0 1.0)" if rows % 3 == 1 else "POINT(2.0 2.0)"
                sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}') "
            tdSql.execute(sql)
            start = end
        tdLog.info(f"append {endRows - startRow} rows to table {fullTbName} from {startRow} to {endRows}")
  
    def updateTableRows(self, fullTbName, updateStart, updateEnd):
        for oldDataRow in range(updateStart, updateEnd):
            self.updateTableData(fullTbName, oldDataRow)

    def updateTableData(self, fullTbName, oldDataRow):
        newDataRow = oldDataRow + 10000
        if self.tdbPrecision == "us":
            prec = 1000 * 1000 * 1000
        elif self.tdbPrecision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000
                        
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        tsStart = int(dt.timestamp() * prec)
        tsInterval = self.interval * prec
        
        sql = f"insert into {fullTbName} values "
        ts = tsStart + oldDataRow * tsInterval
        cint = newDataRow % 10
        cuint = newDataRow
        cbigint = newDataRow
        cubigint = newDataRow
        cfloat = newDataRow
        cdouble = newDataRow
        cvarchar = f"cvar_{newDataRow}"
        csmallint = newDataRow
        cusmallint = newDataRow
        ctinyint = newDataRow % 128
        cutinyint = newDataRow % 256
        cbool = newDataRow % 2 if newDataRow % 20 != 1 else "NULL"
        cnchar = cvarchar
        cvarbinary = cvarchar
        cdecimal8 = "0" if newDataRow % 3 == 1 else "8"
        cdecimal16 = "4" if newDataRow % 3 == 1 else "16"
        cgeometry = "POINT(1.0 1.0)" if newDataRow % 3 == 1 else "POINT(2.0 2.0)"
        sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}') "
        tdSql.execute(sql)

        tdLog.info(f"update table {fullTbName} row {oldDataRow} data to row {newDataRow} data")

    def deleteTableData(self, fullTbName, deleteRow):
        if self.tdbPrecision == "us":
            prec = 1000 * 1000 * 1000
        elif self.tdbPrecision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000
                        
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        tsStart = int(dt.timestamp() * prec)
        tsInterval = self.interval * prec
        
        sql = f"delete from {fullTbName} where cts = {tsStart + deleteRow * tsInterval}"
        tdSql.execute(sql)

        tdLog.info(f"delete table {fullTbName} row {deleteRow}.")
        
    def deleteTableData(self, fullTbName, deleteStart, deleteEnd):
        if self.tdbPrecision == "us":
            prec = 1000 * 1000 * 1000
        elif self.tdbPrecision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000
                        
        dt = datetime.strptime(self.start, "%Y-%m-%d %H.%M.%S")
        tsStart = int(dt.timestamp() * prec)
        tsInterval = self.interval * prec
        
        sql = f"delete from {fullTbName} where cts >= {tsStart + deleteStart * tsInterval} and cts < {tsStart + deleteEnd * tsInterval}"
        tdSql.execute(sql)

        tdLog.info(f"delete table {fullTbName} row from {deleteStart} to {deleteEnd}.")

    def appendTriggerSubTableData(self, tbIndex, startRow, endRows):
        tbName = f"{self.tdb}.{self.triggerSTB}_{tbIndex}"
        self.appendTableData(tbName, startRow, endRows)
        
    def appendCalSubTableData(self, tbIndex, startRow, endRows):
        tbName = f"{self.rdb}.{self.calSTB}_{tbIndex}"
        self.appendTableData(tbName, startRow, endRows)

    def updateCalSubTableData(self, tbIndex, oldDataRow):
        tbName = f"{self.rdb}.{self.calSTB}_{tbIndex}"
        self.updateTableData(tbName, oldDataRow)
        
    def updateTriggerSubTableData(self, tbIndex, oldDataRow):
        tbName = f"{self.tdb}.{self.triggerSTB}_{tbIndex}"
        self.updateTableData(tbName, oldDataRow)
        
    def deleteCalSubTableData(self, tbIndex, deleteRow):
        tbName = f"{self.rdb}.{self.calSTB}_{tbIndex}"
        self.deleteTableData(tbName, deleteRow)
        
    def deleteTriggerSubTableData(self, tbIndex, deleteRow):
        tbName = f"{self.tdb}.{self.triggerSTB}_{tbIndex}"
        self.deleteTableData(tbName, deleteRow)
        
    def appendTriggerDefaultSubTablesData(self, startRow, endRow):
        totalTables = self.tbBatch * self.tbPerBatch
        if(endRow <= startRow or totalTables <= 0):
            tdLog.info(f"no data to append, startRow:{startRow}, endRow:{endRow}, totalTables:{totalTables}")
            return

        for tbIndex in range(totalTables):
            self.appendTriggerSubTableData(tbIndex, startRow, endRow)
            
    def appendCalDefaultSubTablesData(self, startRow, endRow):
        totalTables = self.tbBatch * self.tbPerBatch
        if(endRow <= startRow or totalTables <= 0):
            tdLog.info(f"no data to append, startRow:{startRow}, endRow:{endRow}, totalTables:{totalTables}")
            return

        for tbIndex in range(totalTables):
            self.appendCalSubTableData(tbIndex, startRow, endRow)
     
    def appendCalNormalTableData(self, startRow, endRows):
        tbName = f"{self.rdb}.{self.calNTB}"
        self.appendTableData(tbName, startRow, endRows)

    def appendTriggerNormalTableData(self, startRow, endRows):
        tbName = f"{self.tdb}.{self.triggerNTB}"
        self.appendTableData(tbName, startRow, endRows)
          
    def updateCalNormalTableData(self, oldDataRow):
        tbName = f"{self.rdb}.{self.calNTB}"
        self.updateTableData(tbName, oldDataRow)
        
    def updateTriggerNormalTableData(self, oldDataRow):
        tbName = f"{self.tdb}.{self.triggerNTB}"
        self.updateTableData(tbName, oldDataRow)
        
    def deleteCalNormalTableData(self, deleteRow):
        tbName = f"{self.rdb}.{self.calNTB}"
        self.deleteTableData(tbName, deleteRow)
        
    def deleteTriggerNormalTableData(self, deleteRow):
        tbName = f"{self.tdb}.{self.triggerNTB}"
        self.deleteTableData(tbName, deleteRow)     
     
    def prepareTriggerTable(self):
        if self.streamTableType == StreamTableType.TYPE_SUP_TABLE or self.streamTableType == StreamTableType.TYPE_SUB_TABLE:
            tdLog.info(f"create super table:{self.triggerSTB} for trigger")
            self.createDefaultSupTable(self.tdb, self.triggerSTB)
            self.createDefaultSubTables(self.tdb, self.triggerSTB)
            self.appendTriggerDefaultSubTablesData(0, self.initRow)
        elif self.streamTableType == StreamTableType.TYPE_NORMAL_TABLE:
            tdLog.info(f"create normal table:{self.triggerNTB} for trigger")
            self.createDBDeafaultNormalTable(self.tdb, self.triggerNTB)
            self.appendTriggerNormalTableData(0, self.initRow)
        
    def prepareCalTable(self):
        if self.calTableType == StreamTableType.TYPE_SUP_TABLE or self.calTableType == StreamTableType.TYPE_SUB_TABLE:
            tdLog.info(f"create super table for calculation:{self.calSTB}")
            self.createDefaultSupTable(self.rdb,  self.calSTB)
            self.createDefaultSubTables(self.rdb, self.calSTB)
            self.appendCalDefaultSubTablesData(0, self.initRow)
        elif self.calTableType == StreamTableType.TYPE_NORMAL_TABLE:
            tdLog.info(f"create normal table for calculation:{self.calNTB}")
            self.createDBDeafaultNormalTable(self.rdb, self.calNTB)
            self.appendCalNormalTableData(0, self.initRow)
        
    def clean(self):
        self.dropAllStreamsAndDbs() 
        
    def prepareData(self):
        self.init_databases()
        
        self.prepareTriggerTable()
        self.prepareCalTable()
               
    def triggerTable(self):
        if self.streamTableType == StreamTableType.TYPE_SUP_TABLE or self.streamTableType == StreamTableType.TYPE_SUB_TABLE:
            return f"{self.tdb}.{self.triggerSTB}"
        elif self.streamTableType == StreamTableType.TYPE_NORMAL_TABLE:
            return f"{self.tdb}.{self.triggerNTB}"
        return ""
    
    def calTable(self):
        if self.calTableType == StreamTableType.TYPE_SUP_TABLE or self.calTableType == StreamTableType.TYPE_SUB_TABLE:
            return f"{self.rdb}.{self.calSTB}"
        elif self.calTableType == StreamTableType.TYPE_NORMAL_TABLE:
            return f"{self.rdb}.{self.calNTB}"
        return ""
        
    def createSnode(self, index=1):
        sql = f"create snode on dnode {index}"
        tdSql.execute(sql)

        tdSql.query("show snodes")
        tdSql.checkKeyExist(index)

    def dropSnode(self, index=1):
        sql = f"drop snode on dnode {index}"
        tdSql.query(sql)

    def checkStreamStatus(self, stream_name=""):
        return

        for loop in range(60):
            if stream_name == "":
                tdSql.query(f"select * from information_schema.ins_stream_tasks")
                if tdSql.getRows() == 0:
                    continue
                tdSql.query(
                    f'select * from information_schema.ins_stream_tasks where status != "ready"'
                )
                if tdSql.getRows() == 0:
                    return
            else:
                tdSql.query(
                    f'select stream_name, status from information_schema.ins_stream_tasks where stream_name = "{stream_name}" and status == "ready"'
                )
                if tdSql.getRows() == 1:
                    return
            time.sleep(1)

        tdLog.exit(f"stream task status not ready in {loop} seconds")

    def dropAllStreamsAndDbs(self):
        streamNum = 0
        dbList = tdSql.query("show databases", row_tag=True)
        for r in range(len(dbList)):
            dbname = dbList[r][0]
            if dbname != "information_schema" and dbname != "performance_schema":
                streamList = tdSql.query(f"show {dbname}.streams", row_tag=True)
                for r in range(len(streamList)):
                    streamNum = streamNum + 1
                    streamName = streamList[r][0]
                    tdSql.execute(f"drop stream {dbname}.{streamList[r][0]}")
                tdLog.info(f"drop database {dbname}")
                tdSql.execute(f"drop database {dbname}")

        tdLog.info(f"drop {len(dbList)} databases, {streamNum} streams")

    def prepareChildTables(
        self,
        db="qdb",
        stb="meters",
        precision="ms",
        start="2025-01-01 00.00.00",
        interval=30,
        tbBatch=2,
        tbPerBatch=100,
        rowBatch=2,
        rowsPerBatch=500,
    ):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create stable {db}.{stb} ("
            "  cts timestamp"
            ", cint int"
            ", cuint int unsigned"
            ", cbigint bigint"
            ", cubigint bigint unsigned"
            ", cfloat float"
            ", cdouble double"
            ", cvarchar varchar(32)"
            ", csmallint smallint"
            ", cusmallint smallint unsigned"
            ", ctinyint tinyint"
            ", cutinyint tinyint unsigned"
            ", cbool bool"
            ", cnchar nchar(32)"
            ", cvarbinary varbinary(32)"
            ", cdecimal8 decimal(8)"
            ", cdecimal16 decimal(16)"
            ", cgeometry geometry(32)"
            ") tags("
            "  tts timestamp"
            ", tint int"
            ", tuint int unsigned"
            ", tbigint bigint"
            ", tubigint bigint unsigned"
            ", tfloat float"
            ", tdouble double"
            ", tvarchar varchar(32)"
            ", tsmallint smallint"
            ", tusmallint smallint unsigned"
            ", ttinyint tinyint"
            ", tutinyint tinyint unsigned"
            ", tbool bool"
            ", tnchar nchar(16)"
            ", tvarbinary varbinary(32)"
            ", tgeometry geometry(32)"
            ")"
        )

        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec
        tsInterval = interval * prec

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                tts = tsStart if table % 3 == 1 else tsNext
                tint = table % 3 if table % 20 != 1 else "NULL"
                tuint = table % 4
                tbigint = table % 5
                tubigint = table % 6
                tfloat = table % 7
                tdouble = table % 8
                tvarchar = "SanFrancisco" if table % 3 == 1 else "LosAngeles"
                tsmallint = table % 9
                tusmallint = table % 10
                ttinyint = table % 11
                tutinyint = table % 12
                tbool = table % 2
                tnchar = tvarchar
                tvarbinary = tvarchar
                tgeometry = "POINT(1.0 1.0)" if table % 3 == 1 else "POINT(2.0 2.0)"
                sql += f"{db}.t{table} using {db}.{stb} tags({tts}, '{tint}', {tuint}, {tbigint}, {tubigint}, {tfloat}, {tdouble}, '{tvarchar}', {tsmallint}, {tusmallint}, {ttinyint}, {tutinyint}, {tbool}, '{tnchar}', '{tvarbinary}', '{tgeometry}') "
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.t{table} values "
                for row in range(rowsPerBatch):
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    cint = rows
                    cuint = rows % 4
                    cbigint = rows % 5
                    cubigint = rows % 6
                    cfloat = rows % 7
                    cdouble = rows % 8
                    cvarchar = "SanFrancisco" if rows % 3 == 1 else "LosAngeles"
                    csmallint = rows % 9
                    cusmallint = rows % 10
                    ctinyint = rows % 11
                    cutinyint = rows % 12
                    cbool = rows % 2 if rows % 20 != 1 else "NULL"
                    cnchar = cvarchar
                    cvarbinary = cvarchar
                    cdecimal8 = "0" if rows % 3 == 1 else "8"
                    cdecimal16 = "4" if rows % 3 == 1 else "16"
                    cgeometry = "POINT(1.0 1.0)" if rows % 3 == 1 else "POINT(2.0 2.0)"
                    sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}') "
                tdSql.execute(sql)

    def prepareNormalTables(
        self,
        db="qdb",
        precision="ms",
        start="2025-01-01 00.00.00",
        interval=30,
        tables=10,
        rowBatch=2,
        rowsPerBatch=500,
    ):
        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsInterval = interval * prec

        tdLog.info(f"create total {tables} normal tables")
        for table in range(tables):
            tdSql.execute(
                f"create table {db}.n{table} ("
                "  cts timestamp"
                ", cint int"
                ", cuint int unsigned"
                ", cbigint bigint"
                ", cubigint bigint unsigned"
                ", cfloat float"
                ", cdouble double"
                ", cvarchar varchar(32)"
                ", csmallint smallint"
                ", cusmallint smallint unsigned"
                ", ctinyint tinyint"
                ", cutinyint tinyint unsigned"
                ", cbool bool"
                ", cnchar nchar(32)"
                ", cvarbinary varbinary(32)"
                ", cdecimal8 decimal(8)"
                ", cdecimal16 decimal(16)"
                ", cgeometry geometry(32)"
                ")"
            )

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        for table in range(tables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.n{table} values "
                for row in range(rowsPerBatch):
                    rows = batch * rowsPerBatch + row
                    ts = tsStart + rows * tsInterval
                    cint = rows
                    cuint = rows % 4
                    cbigint = rows % 5
                    cubigint = rows % 6
                    cfloat = rows % 7
                    cdouble = rows % 8
                    cvarchar = "SanFrancisco" if rows % 3 == 1 else "LosAngeles"
                    csmallint = rows % 9
                    cusmallint = rows % 10
                    ctinyint = rows % 11
                    cutinyint = rows % 12
                    cbool = rows % 2
                    cnchar = cvarchar
                    cvarbinary = cvarchar
                    cdecimal8 = "0" if rows % 3 == 1 else "8"
                    cdecimal16 = "4" if rows % 3 == 1 else "16"
                    cgeometry = "POINT(1.0 1.0)" if rows % 3 == 1 else "POINT(2.0 2.0)"
                    sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}')"
                tdSql.execute(sql)

    def prepareVirtualTables(
        self,
        db="qdb",
        stb="vmeters",
        precision="ms",
        start="2025-01-01 00.00.00",
        tables=10,
    ):
        # each virtual table is sourced from 10 child-tables.
        tdSql.execute(f"use {db}")

        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec

        tdLog.info(f"create virtual super table")
        tdSql.execute(
            f"create stable {db}.{stb} ("
            "  cts timestamp"
            ", cint int"
            ", cuint int unsigned"
            ", cbigint bigint"
            ", cubigint bigint unsigned"
            ", cfloat float"
            ", cdouble double"
            ", cvarchar varchar(32)"
            ", csmallint smallint"
            ", cusmallint smallint unsigned"
            ", ctinyint tinyint"
            ", cutinyint tinyint unsigned"
            ", cbool bool"
            ", cnchar nchar(32)"
            ", cvarbinary varbinary(32)"
            ", cgeometry geometry(32)"
            ") tags("
            "  tts timestamp"
            ", tint int"
            ", tuint int unsigned"
            ", tbigint bigint"
            ", tubigint bigint unsigned"
            ", tfloat float"
            ", tdouble double"
            ", tvarchar varchar(32)"
            ", tsmallint smallint"
            ", tusmallint smallint unsigned"
            ", ttinyint tinyint"
            ", tutinyint tinyint unsigned"
            ", tbool bool"
            ", tnchar nchar(16)"
            ", tvarbinary varbinary(32)"
            ", tgeometry geometry(32)"
            ") VIRTUAL 1"
        )

        tdLog.info(f"create total {tables} virtual tables")
        for table in range(tables):
            t0 = table * 10
            t1 = table * 10 + 1
            t2 = table * 10 + 2
            t3 = table * 10 + 3
            t4 = table * 10 + 4
            t5 = table * 10 + 5
            t6 = table * 10 + 6
            t7 = table * 10 + 7
            t8 = table * 10 + 8
            t9 = table * 10 + 9

            tts = tsStart if table % 3 == 1 else tsNext
            tint = table % 3
            tuint = table % 4
            tbigint = table % 5
            tubigint = table % 6
            tfloat = table % 7
            tdouble = table % 8
            tvarchar = "SanFrancisco" if table % 3 == 1 else "LosAngeles"
            tsmallint = table % 9
            tusmallint = table % 10
            ttinyint = table % 11
            tutinyint = table % 12
            tbool = table % 2
            tnchar = tvarchar
            tvarbinary = tvarchar
            tgeometry = "POINT(1.0 1.0)" if table % 3 == 1 else "POINT(2.0 2.0)"

            tdSql.execute(
                f"create vtable v{table}("
                f"  t{t0}.cint"
                f", t{t0}.cuint"
                f", t{t1}.cbigint"
                f", t{t1}.cubigint"
                f", t{t2}.cfloat"
                f", t{t2}.cdouble"
                f", t{t3}.cvarchar"
                f", t{t4}.csmallint"
                f", t{t4}.cusmallint"
                f", t{t5}.ctinyint"
                f", t{t5}.cutinyint"
                f", t{t7}.cbool"
                f", t{t8}.cnchar"
                f", t{t8}.cvarbinary"
                f", t{t9}.cgeometry"
                f") using {db}.{stb} tags("
                f"  {tts}"
                f", {tint}"
                f", {tuint}"
                f", {tbigint}"
                f", {tubigint}"
                f", {tfloat}"
                f", {tdouble}"
                f", '{tvarchar}'"
                f", {tsmallint}"
                f", {tusmallint}"
                f", {ttinyint}"
                f", {tutinyint}"
                f", {tbool}"
                f", '{tnchar}'"
                f", '{tvarbinary}'"
                f", '{tgeometry}') "
            )

    def prepareJsonTables(
        self,
        db="qdb",
        stb="jmeters",
        precision="ms",
        start="2025-01-01 00.00.00",
        interval=30,
        tbBatch=1,
        tbPerBatch=10,
        rowBatch=2,
        rowsPerBatch=500,
    ):
        tdLog.info(f"create super table")
        tdSql.execute(
            f"create stable {db}.{stb} ("
            "  cts timestamp"
            ", cint int composite key"
            ", cuint int unsigned"
            ", cbigint bigint"
            ", cubigint bigint unsigned"
            ", cfloat float"
            ", cdouble double"
            ", cvarchar varchar(32)"
            ", csmallint smallint"
            ", cusmallint smallint unsigned"
            ", ctinyint tinyint"
            ", cutinyint tinyint unsigned"
            ", cbool bool"
            ", cnchar nchar(32)"
            ", cvarbinary varbinary(32)"
            ", cdecimal8 decimal(8)"
            ", cdecimal16 decimal(16)"
            ", cgeometry geometry(32)"
            ") tags("
            "  tjson JSON"
            ")"
        )

        dt = datetime.strptime(start, "%Y-%m-%d %H.%M.%S")
        if precision == "us":
            prec = 1000 * 1000 * 1000
        elif precision == "ns":
            prec = 1000 * 1000
        else:
            prec = 1000

        tsStart = int(dt.timestamp() * prec)
        tsNext = tsStart + 86400 * prec
        tsInterval = interval * prec

        totalTables = tbBatch * tbPerBatch
        tdLog.info(f"create total {totalTables} child tables")
        str1 = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
        str2 = '{\\"k1\\":\\"v1\\",\\"k2\\":\\"v2\\"}'
        for batch in range(tbBatch):
            sql = "create table "
            for tb in range(tbPerBatch):
                table = batch * tbPerBatch + tb
                tjson = str1 if table % 3 == 1 else str2
                sql += f'{db}.j{table} using {db}.{stb} tags("{tjson}")'
            tdSql.execute(sql)

        totalRows = rowsPerBatch * rowBatch
        tdLog.info(f"write total:{totalRows} rows, {rowsPerBatch} rows per table")
        for table in range(totalTables):
            for batch in range(rowBatch):
                sql = f"insert into {db}.j{table} values "
                for row in range(rowsPerBatch - 1):
                    rows = batch * rowsPerBatch + row
                    ts = (
                        tsStart + rows * tsInterval
                        if rows % 2 == 0
                        else tsStart + (rows - 1) * tsInterval
                    )
                    cint = rows
                    cuint = rows % 4
                    cbigint = rows % 5
                    cubigint = rows % 6
                    cfloat = rows % 7
                    cdouble = rows % 8
                    cvarchar = "SanFrancisco" if rows % 3 == 1 else "LosAngeles"
                    csmallint = rows % 9
                    cusmallint = rows % 10
                    ctinyint = rows % 11
                    cutinyint = rows % 12
                    cbool = rows % 2
                    cnchar = cvarchar
                    cvarbinary = cvarchar
                    cdecimal8 = "0" if rows % 3 == 1 else "8"
                    cdecimal16 = "4" if rows % 3 == 1 else "16"
                    cgeometry = "POINT(1.0 1.0)" if rows % 3 == 1 else "POINT(2.0 2.0)"
                    sql += f"({ts}, {cint}, {cuint}, {cbigint}, {cubigint}, {cfloat}, {cdouble}, '{cvarchar}', {csmallint}, {cusmallint}, {ctinyint}, {cutinyint}, {cbool}, '{cnchar}', '{cvarbinary}', '{cdecimal8}', '{cdecimal16}', '{cgeometry}') "
                tdSql.execute(sql)

                rows = rows + 1
                ts = tsStart + rows * tsInterval
                cint = rows
                sql = f"insert into {db}.j{table} (cts, cint) values ({ts}, {cint})"
                tdSql.execute(sql)

    def prepareViews(
        self,
        db="qdb",
        views=10,
    ):
        tdSql.execute(f"use {db}")

        tdLog.info(f"create total {views} views")
        for v in range(views):
            sql = f"create view view{v} as select cts, cint, cuint, cbigint, cubigint, cfloat, cdouble, cvarchar, csmallint, cusmallint, ctinyint, cutinyint, cbool, cnchar, cvarbinary, cgeometry from qdb.t{v}"
            tdSql.execute(sql)


tdStream = StreamUtil()


class StreamItem:
    def __init__(
        self,
        id,
        stream,
        res_query="",
        exp_query="",
        exp_rows=[],
        check_func=None,
    ):
        self.id = id
        self.stream = stream
        self.res_query = res_query
        self.exp_query = exp_query
        self.exp_rows = exp_rows
        self.check_func = check_func

    def createStream(self):
        tdLog.info(self.stream)
        tdSql.execute(self.stream)

    def checkResults(self):
        tdLog.info(f"check stream:s{self.id} result")

        if self.check_func != None:
            self.check_func()

        if self.exp_query != "":
            if self.exp_rows == []:
                exp_result = tdSql.getResult(self.exp_query)
            else:
                exp_result = []
                tmp_result = tdSql.getResult(self.exp_query)
                for r in self.exp_rows:
                    exp_result.append(tmp_result[r])

            tdSql.checkResultsByArray(self.res_query, exp_result, self.exp_query)

        tdLog.info(f"check stream:s{self.id} result successfully")
