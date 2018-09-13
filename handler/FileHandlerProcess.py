from multiprocessing import Process, Manager, Lock
from parser import BaseFile, BigWig, BigBed
from datetime import datetime, timedelta
import pickle
import os
# import sqlite3
import asyncio
import concurrent.futures
import threading
import handler.utils as utils

# clean up finished futures
# grab proper result from those futures


class FileHandlerProcess(object):
    """docstring for ProcessHandler"""
    def __init__(self, fileTime, MAXWORKER):
        # self.manager = Manager()
        # self.dict = self.manager.dict()
        self.records = {}
        self.fileTime = fileTime
        self.ManagerLock = Lock()
        self.counter = 0
        self.inProgress = {}
 
        # if in the future we switch to python 3.7
        # self.executor = concurrent.futures.ThreadPoolExecutor(max_workers = MAXWORKER, initializer = self.threadInit, initargs=())

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers = MAXWORKER)

    def cleanFileOBJ(self):
        tasks = []
        for fileName, record in self.records.items():
            if datetime.now() - record.get("time") > timedelta(seconds = self.fileTime) and not record.get("pickled"):
                tasks.append(self.pickleFileObject(fileName))
        return tasks

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


    def bigwigWrapper(self, fileObj, chrom, startIndices, endIndices, points, fileId, zoomlvl):
        f=[]
        result = []
        for s, e in zip(startIndices, endIndices):
            f.append(self.executor.submit(fileObj.getRange, chrom, s, e, zoomlvl = zoomlvl))
        for t in concurrent.futures.as_completed(f):
            result.append(t.result())
        return self.merge(result)

    def bigbedWrapper(self, fileObj, chrom, startIndices, endIndices, fileId, zoomlvl = -2):
        f=[]
        result = []
        for s, e in zip(startIndices, endIndices):
            f.append(self.executor.submit(fileObj.getRange, chrom, s, e, zoomlvl = zoomlvl))
        for t in concurrent.futures.as_completed(f):
            result.append(t.result())
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
                    if start >= endIndex or startIndex == endIndex:
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

        if startIndex is not endIndex:
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
        result = await future
        loop = asyncio.get_event_loop()
        m = loop.run_in_executor(self.executor, self.calculateResultInAnotherThread, startIndex, endIndex, result)
        result = await m
        return result

    def calculateResultInAnotherThread(self, startIndex, endIndex, result):
        r = []
        for (start, end, value) in result:
            if start >= startIndex and end <= endIndex:
                r.append((start, end, value))
        return r

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

    async def handleFile(self, fileName, fileType, chrom, startIndex, endIndex, points):
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

        m = loop.run_in_executor(self.executor, wrapper, fileObj, 
                                    chrom, startIndices, endIndices, points, fileId, zoomlvl)
        self.updateInprogress(m, fileId, zoomlvl, startIndices, endIndices)
        result = [await m]
        for (fStart, fEnd, fFuture) in futures:
            sadf = await self.calculateResult(fStart, fEnd, fFuture)
            result.append(sadf)

        self.removeInprogress(m, fileId, zoomlvl, startIndices, endIndices)
        return self.merge(result)