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

# ----- things to be done -------
# remove cache and db cache at start
# fix bug
# add support for non-consequtive range after sql search
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
        #     (fileId integer, lastTime timestamp, zoomLvl integer, startI integer, endI integer, chrom text, valueBW real, valueBB text,
        #      UNIQUE(fileId, zoomLvl, startI, endI))''')
        c.execute('''CREATE TABLE cache
             (fileId int, lastTime timestamp, zoomLvl int, startI int, endI int, chrom varchar(255), valueBW varchar(255), valueBB varchar(255),
             UNIQUE(fileId, zoomLvl, startI, endI))''')
        # c.execute('''ALTER TABLE  cache 
        #             ADD UNIQUE indexname
        #             (fileId, zoomLvl, startI, endI);''')
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

    def sqlQueryBW(self, startIndex, endIndex, chrom, zoomLvl, fileId):
        result = []
        start = []
        end = []
        print(self.dbConnection)
        c = self.getConnection(threading.get_ident()).cursor()
        print(c)
        # for row in self.c.execute('SELECT startI, endI, valueBW FROM cache WHERE (fileId=%s AND zoomLvl=%s AND startI>=%s AND endI<=%s AND chrom=%s)', 
        #     (fileId, zoomLvl, startIndex, endIndex, chrom)):
            # result.append((row[0], row[1], row[2]))
            # # calculate missing range
            # if row[0] > startIndex:
            #     start.append(startIndex)
            #     end.append(row[0])
            # startIndex = row[1]
        c.execute('SELECT startI, endI, valueBW FROM cache WHERE (fileId=%s AND zoomLvl=%s AND startI>=%s AND endI<=%s AND chrom=%s)', 
            (fileId, zoomLvl, startIndex, endIndex, chrom))
        for row in c.fetchall():
            result.append((row["startI"], row["endI"], row["valueBW"]))
            # calculate missing range
            if row["startI"] > startIndex:
                start.append(startIndex)
                end.append(row["startI"])
            startIndex = row["endI"]
        start.append(startIndex)
        end.append(endIndex)
        c.close()
        return start, end, result

    

    def addToDbBW(self, result, chrom, fileId, zoomLvl):
        c = self.getConnection(threading.get_ident()).cursor()
        # for s, e, v in zip(result.gets("start"), result.gets("end"), result.gets("value")):
        for r in result:
            for s in r:
                c.execute("INSERT INTO cache VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE lastTime = %s", 
                    (fileId, datetime.now(), zoomLvl, s[0], s[1], chrom, s[2], "", datetime.now()))
                # c.execute("UPDATE cache SET lastTime = %s WHERE (fileId=%s AND zoomLvl=%s AND startI=%s AND endI=%s AND chrom=%s)",
                #     (datetime.now(), fileId, zoomLvl, s[0], s[1], chrom))
        self.connection.commit()
        c.close()

    def bigwigWrapper(self, fileObj, chrom, startIndex, endIndex, points, fileId):
        f=[]
        result = []
        points = (endIndex - startIndex) if points > (endIndex - startIndex) else points
        step = (endIndex - startIndex)*1.0/points
        f.append(self.executor.submit(fileObj.getRange, chrom, startIndex, endIndex))
        zoomLvl, _ = self.executor.submit(fileObj.getZoom, step).result()
        m = self.executor.submit(self.sqlQueryBW, startIndex, endIndex, chrom, zoomLvl, fileId)
        concurrent.futures.as_completed([m])
        (start, end, dbRusult) = m.result()
        for s, e in zip(start, end):
            f.append(self.executor.submit(fileObj.getRange, chrom, s, e, zoomlvl = zoomLvl))
            result.append(fileObj.getRange(chrom, s, e, zoomlvl = zoomLvl))
        for t in concurrent.futures.as_completed(f):
            result.append(t.result())
        self.executor.submit(self.addToDbBW, result, chrom, fileId, zoomLvl)
        # asyncio.ensure_future(addToDb)
        # return await self.mergeBW(result, dbRusult)
        result.append(dbRusult)
        return self.merge(result)

    def sqlQueryBB(self, startIndex, endIndex, chrom, fileId):
        result = []
        start = []
        end = []
        for row in self.c.execute('SELECT startI, endI, valueBB FROM cache WHERE (fileId=%s AND startI>=%s AND endI<=%s AND chrom=%s)', 
            (fileId, startIndex, endIndex, chrom)):
            result.append((row[0], row[1], row[2]))
            # calculate missing range
            if row[0] > startIndex:
                start.append(startIndex)
                end.append(row[0])
            startIndex = row[1]
        start.append(startIndex)
        end.append(endIndex)

        return start, end, result

    async def addToDbBB(self, result, chrom, fileId):
        # for s, e, v in zip(result.gets("start"), result.gets("end"), result.gets("value")):
        for r in result:
            for s in r:
                self.c.execute("INSERT OR IGNORE INTO cache VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", 
                    (fileId, datetime.now(), -2, s[0], s[1], chrom, 0.0, s[2]))
                self.c.execute("UPDATE cache SET lastTime = %s WHERE (fileId=%s AND startI=%s AND endI=%s AND chrom=%s)",
                    (datetime.now(), fileId, s[0], s[1], chrom))
        self.db.commit()

    async def bigbedWrapper(self, fileObj, chrom, startIndex, endIndex, fileId):
        result=[]
        (start, end, dbRusult) = await self.sqlQueryBB(startIndex, endIndex, chrom, fileId)
        for s, e in zip(start, end):
            result.append(await fileObj.getRange(chrom, s, e))
        asyncio.ensure_future(self.addToDbBB(result, chrom, fileId))
        result.append(dbRusult)
        return self.merge(result)

    def merge(self, result):
        l = [item for sublist in result for item in sublist]
        g = lambda i: i[0]
        l.sort(key=g)
        return l

    async def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.records.get(fileName) == None:
            # p = Process(target=f, args=(d, l))
            # p = BieWigProcess(fileName, "BW")
            bigwig = BigWig(fileName)
            bigwig.getHeader()
            fileId = self.setManager(fileName, bigwig)
        else:
            bigwig, fileId = self.updateTime(fileName)
        # p.start(chrom, startIndex, endIndex, points)
        loop = asyncio.get_event_loop()
        m = await loop.run_in_executor(self.executor, self.bigwigWrapper, bigwig, chrom, startIndex, endIndex, points, fileId)
        # f = self.executor.submit(self.bigwigWrapper, bigwig, chrom, startIndex, endIndex, points, fileId)
        # m=await f.result()
        return m

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
        elif fileType in ["bigbed", "bb", "BigBed", "bigBed"]:
            wrapper = self.bigbedWrapper

        m = await loop.run_in_executor(self.executor, wrapper, fileObj, 
                                    chrom, startIndex, endIndex, points, fileId)
        return m