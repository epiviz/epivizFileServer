from multiprocessing import Process, Manager, Lock
from parser import BaseFile, BigWig, BigBed
from datetime import datetime, timedelta
import pymysql.cursors
import pickle
import os
# import sqlite3
import asyncio
import concurrent.futures
import threading
import handler.utils as utils

dbUsername = 'root'
dbPassword = '123123123'


class FileHandlerProcess(object):
    """docstring for ProcessHandler"""
    def __init__(self, fileTime, recordTime, MAXWORKER):
        # self.manager = Manager()
        # self.dict = self.manager.dict()
        self.records = {}
        self.fileTime = fileTime
        self.recordTime = recordTime
        self.ManagerLock = Lock()
        self.counter = 0
        self.dbConnection = {}
        self.inProgress = {}

        # if in the future we switch to python 3.7
        # self.executor = concurrent.futures.ThreadPoolExecutor(max_workers = MAXWORKER, initializer = self.threadInit, initargs=())

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers = MAXWORKER)

        # self.db = sqlite3.connect('data.db', check_same_thread=False)
        self.mainConnection = pymysql.connect(host='localhost',
                    user=dbUsername,
                    password=dbPassword,
                    db='DB',
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True)   
        # self.c = self.db.cursor()
        c = self.mainConnection.cursor()
        c.execute('''DROP TABLE IF EXISTS cache''')

        #self.c.execute('''CREATE TABLE cache
        #     (fileId integer, lastTime timestamp, zoomlvl integer, startI integer, endI integer, chrom text, valueBW real, valueBB text,
        #      UNIQUE(fileId, zoomlvl, startI, endI))''')
        c.execute('''CREATE TABLE cache
             (fileId int, lastTime timestamp, zoomlvl int, startI bigint, endI bigint, chrom varchar(255), valueBW double, valueBB varchar(255),
             UNIQUE(fileId, zoomlvl, startI, endI))''')
        # c.execute('''ALTER TABLE  cache 
        #             ADD UNIQUE indexname
        #             (fileId, zoomlvl, startI, endI);''')
        self.mainConnection.commit()
        c.close()

    def cleanFileOBJ(self):
        tasks = []
        for fileName, record in self.records.items():
            if datetime.now() - record.get("time") > timedelta(seconds = self.fileTime) and not record.get("pickled"):
                tasks.append(self.pickleFileObject(fileName))
        return tasks

    async def cleanDbRecord(self):
        c = self.mainConnection.cursor()
        c.execute('DELETE FROM cache WHERE lastTime < %s', (datetime.now() - timedelta(seconds = self.recordTime),))
        self.mainConnection.commit()
        c.close()

    def threadInit(self):
        self.dbConnection[threading.get_ident()] = pymysql.connect(host='localhost',
                    user=dbUsername,
                    password=dbPassword,
                    db='DB',
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor,
                    autocommit=True)

    def getConnection(self, threadId):
        if not self.dbConnection.get(threadId):
            self.threadInit()
        return self.dbConnection[threadId]

    async def pickleFileObject(self, fileName):
        record = self.records.get(fileName)
        record["pickling"] = True
        record["pickled"] = True
        record["fileObj"].clearLock()
        filehandler = open(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache", "wb")
        pickle.dump(record["fileObj"], filehandler)
        filehandler.close()
        record["pickling"] = False
        record["fileObj"] = None

    def printRecords(self):
        return str(self.records)

    def setManager(self, fileName, fileObj):
        self.ManagerLock.acquire()
        self.records[fileName] = {"fileObj":fileObj, "time": datetime.now(), "pickled": False, "pickling": False, "ID": self.counter}
        self.counter += 1
        self.ManagerLock.release()
        return self.records.get(fileName)["ID"]

    def updateTime(self, fileName):
        record = self.records.get(fileName)
        record["time"] = datetime.now()
        while record["pickling"]:
            pass
        if record["pickled"]:
            record["pickling"] = True
            record["pickled"] = False
            filehandler = open(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache", "rb")
            record["fileObj"] = pickle.load(filehandler)
            record["fileObj"].reinitLock()
            record["pickling"] = False
            filehandler.close()
            os.remove(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache")

        return record["fileObj"], record["ID"]

    # the start and end indices will be arrays of non consequtive ranges
    def sqlQuery(self, startIndices, endIndices, chrom, zoomlvl, fileId, col):
        result = []
        start = []
        end = []
        print(self.dbConnection)
        c = self.getConnection(threading.get_ident()).cursor()
        # for row in self.c.execute('SELECT startI, endI, valueBW FROM cache WHERE (fileId=%s AND zoomlvl=%s AND startI>=%s AND endI<=%s AND chrom=%s)', 
        #     (fileId, zoomlvl, startIndex, endIndex, chrom)):
            # result.append((row[0], row[1], row[2]))
            # # calculate missing range
            # if row[0] > startIndex:
            #     start.append(startIndex)
            #     end.append(row[0])
            # startIndex = row[1]
        for startIndex, endIndex in zip(startIndices, endIndices):
            c.execute('SELECT startI, endI, %s FROM cache WHERE (fileId=%s AND zoomlvl=%s AND startI>=%s AND endI<=%s AND chrom=%s)', 
                (col, fileId, zoomlvl, startIndex, endIndex, chrom))
            for row in c.fetchall():
                result.append((row["startI"], row["endI"], row[col]))
                # calculate missing range
                if row["startI"] > startIndex:
                    start.append(startIndex)
                    end.append(row["startI"])
                startIndex = row["endI"]
            start.append(startIndex)
            end.append(endIndex)
        c.close()
        return start, end, result

    def addToDb(self, result, chrom, fileId, zoomlvl, col):
        c = self.getConnection(threading.get_ident()).cursor()
        # for s, e, v in zip(result.gets("start"), result.gets("end"), result.gets("value")):
        for r in result:
            for s in r:
                if col is "valueBB":
                    bb = s[2]
                    bw = 0
                else:
                    bw = s[2]
                    bb = 0
                c.execute("INSERT INTO cache VALUES (%s, %s, %s, %s, %s, %s, %f, %f) ON DUPLICATE KEY UPDATE lastTime = %s", 
                    (fileId, datetime.now(), zoomlvl, s[0], s[1], chrom, bw, bb, datetime.now()))
                # c.execute("UPDATE cache SET lastTime = %s WHERE (fileId=%s AND zoomlvl=%s AND startI=%s AND endI=%s AND chrom=%s)",
                #     (datetime.now(), fileId, zoomlvl, s[0], s[1], chrom))
        self.connection.commit()
        c.close()

    def bigwigWrapper(self, fileObj, chrom, startIndices, endIndices, points, fileId, zoomlvl):
        f=[]
        result = []
        (start, end, dbRusult) = self.sqlQuery(startIndices, endIndices, chrom, zoomlvl, fileId, "valueBW")
        for s, e in zip(start, end):
            f.append(self.executor.submit(fileObj.getRange, chrom, s, e, zoomlvl = zoomlvl))
        for t in concurrent.futures.as_completed(f):
            result.append(t.result())
        self.executor.submit(self.addToDb, result, chrom, fileId, zoomlvl, "valueBW")
        # asyncio.ensure_future(addToDb)
        # return await self.mergeBW(result, dbRusult)
        result.append(dbRusult)
        return self.merge(result)

    async def bigbedWrapper(self, fileObj, chrom, startIndices, endIndices, fileId, zoomlvl = -2):
        f=[]
        result = []
        (start, end, dbRusult) = self.sqlQuery(startIndices, endIndices, chrom, zoomlvl, fileId, "valueBB")
        for s, e in zip(start, end):
            f.append(self.executor.submit(fileObj.getRange, chrom, s, e, zoomlvl = zoomlvl))
        for t in concurrent.futures.as_completed(f):
            result.append(t.result())
        self.executor.submit(self.addToDb, result, chrom, fileId, zoomlvl, "valueBB")
        # asyncio.ensure_future(addToDb)
        # return await self.mergeBW(result, dbRusult)
        result.append(dbRusult)
        return self.merge(result)

    def merge(self, result):
        l = [item for sublist in result for item in sublist]
        g = lambda i: i[0]
        l.sort(key=g)
        return l

    # search for any overlap that is already in progress, returns the updated start and end index,
    # and a future array containing overlapped results.
    def locateRedundent(self, fileId, startIndex, endIndex, zoomlvl):
        futures = []
        startIndices = []
        endIndices = []

        if self.inProgress.get(fileId) is not None:
            if self.inProgress.get(fileId).get(zoomlvl) is not None:
                for (start, end, future) in self.inProgress.get(fileId).get(zoomlvl):
                    # calculate missing range
                    if start > endIndex:
                        break
                    elif start >= startIndex and end < endIndex:
                        futures.append((start, end, future))
                        startIndices.append(startIndex)
                        endIndices.append(start)
                        startIndex = end
                    elif start >= startIndex and end >= endIndex: 
                        futures.append((start, endIndex, future))   
                        startIndices.append(startIndex)
                        endIndices.append(start)
                        startIndex = endIndex
                    elif start < startIndex and end < endIndex:
                        futures.append((startIndex, end, future))
                        startIndex = end
                    elif start < startIndex and end >= endIndex:
                        futures.append((startIndex, endIndex, future))
                        startIndex = endIndex

        startIndices.append(startIndex)
        endIndices.append(endIndex)

        return startIndices, endIndices, futures

    def updateInprogress(self, m, fileId, zoomlvl, startIndices, endIndices):
        if self.inProgress.get(fileId) is None:
            self.inProgress[fileId] = {}
        if self.inProgress.get(fileId).get(zoomlvl) is None:
            self.inProgress.get(fileId)[zoomlvl] = []
        for start, end in zip(startIndices, endIndices):
            self.inProgress.get(fileId)[zoomlvl].append((start, end, m))
        g = lambda i: i[0]
        self.inProgress.get(fileId)[zoomlvl].sort(key=g)
        # result = await m.result()
        # self.inProgress.get(fileId)[zoomlvl].remove((start, end, m))

    def removeInprogress(self, m, fileId, zoomlvl, startIndices, endIndices):
        for start, end in zip(startIndices, endIndices):
            self.inProgress.get(fileId)[zoomlvl].remove((start, end, m))

    async def calculateResult(self, startIndex, endIndex, future):
        result = []
        for (start, end, value) in await future:
            if start >= startIndex and end <= endIndex:
                result.append((start, end, value))
        return result

    async def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.records.get(fileName) == None:
            bigwig = BigWig(fileName)
            bigwig.getHeader()
            fileId = self.setManager(fileName, bigwig)
        else:
            bigwig, fileId = self.updateTime(fileName)
        loop = asyncio.get_event_loop()
        m = loop.run_in_executor(self.executor, self.bigwigWrapper, bigwig, chrom, startIndex, endIndex, points, fileId, zoomlvl)
        return await m

    async def handleBigBed(self, fileName, chrom, startIndex, endIndex):
        if self.records.get(fileName) == None:
            # p = BieBedProcess(fileName, "BB")
            bigbed = BigBed(fileName)
            await bigbed.getHeader()
            fileId = self.setManager(fileName, bigbed)
        else:
            bigbed, fileId = self.updateTime(fileName)
        # r.start(chrom, startIndex, endIndex)
        return await self.bigbedWrapper(bigbed, chrom, startIndex, endIndex, fileId)


    async def handleFile(self, fileName, fileType, chrom, startIndex, endIndex,points):
        if self.records.get(fileName) == None:
            fileObj = utils.create_parser_object(fileType, 
                                                fileName)
            fileObj.getHeader()
            fileId = self.setManager(fileName, fileObj)
        else:
            fileObj, fileId = self.updateTime(fileName)
        
        loop = asyncio.get_event_loop()
        wrapper = None
        if fileType in ["bigwig", "bigWig", "BigWig", "bw"]:
            wrapper = self.bigwigWrapper
            points = (endIndex - startIndex) if points > (endIndex - startIndex) else points
            step = (endIndex - startIndex)*1.0/points
            zoomlvl, _ = self.executor.submit(fileObj.getZoom, step).result()
            startIndices, endIndices, futures = self.locateRedundent(fileId, startIndex, endIndex, zoomlvl)
        elif fileType in ["bigbed", "bb", "BigBed", "bigBed"]:
            wrapper = self.bigbedWrapper
            zoomlvl = -2
            startIndices, endIndices, futures = self.locateRedundent(fileId, startIndex, endIndex, zoomlvl)

        print(1)
        m = loop.run_in_executor(self.executor, wrapper, fileObj, 
                                    chrom, startIndices, endIndices, points, fileId, zoomlvl)
        self.updateInprogress(m, fileId, zoomlvl, startIndices, endIndices)
        result = [await m]
        self.removeInprogress(m, fileId, zoomlvl, startIndices, endIndices)
        print(2)
        for f in concurrent.futures.as_completed(futures):
            result.append(await self.calculateResult(f[0], f[1], f[2]))
        print(3)
        print(result)
        return result