from multiprocessing import Process, Manager, Lock
from parser import BaseFile, BigWig, BigBed
from datetime import datetime, timedelta
import pickle
import os
import sqlite3
import asyncio
# ----- things to be done -------
# remove cache and db cache at start
# fix bug
# add support for non-consequtive range after sql search

class FileHandlerProcess(object):
    """docstring for ProcessHandler"""
    def __init__(self, timePeriod):
        # self.manager = Manager()
        # self.dict = self.manager.dict()
        self.records = {}
        self.timePeriod = timePeriod
        self.ManagerLock = Lock()
        self.counter = 0
        self.db = sqlite3.connect('data.db')
        self.c = self.db.cursor()
        self.c.execute('''DROP TABLE IF EXISTS cache''')

        self.c.execute('''CREATE TABLE cache
             (fileId integer, lastTime timestamp, zoomLvl integer, startI integer, endI integer, chrom text, valueBW real, valueBB text,
             UNIQUE(fileId, zoomLvl, startI, endI))''')
        self.db.commit()

    def clean(self):
        tasks = []
        for fileName, record in self.records.items():
            if datetime.now() - record.get("time") > timedelta(seconds = self.timePeriod) and not record.get("pickled"):
                tasks.append(self.pickleFileObject(fileName))
        return tasks

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

    async def sqlQueryBW(self, startIndex, endIndex, chrom, zoomLvl, fileId):
        result = []
        start = []
        end = []
        for row in self.c.execute('SELECT startI, endI, valueBW FROM cache WHERE (fileId=? AND zoomLvl=? AND startI>=? and endI<=?)', (fileId, zoomLvl, startIndex, endIndex)):
            result.append((row[0], row[1], row[2]))
            # calculate missing range
            if row[0] > startIndex:
                print(startIndex, (row[0], row[1], row[2]))
                start.append(startIndex)
                end.append(row[0])
            startIndex = row[1]
        start.append(startIndex)
        end.append(endIndex)

        return start, end, result

    async def addToDbBW(self, result, chrom, fileId, zoomLvl):
        # for s, e, v in zip(result.gets("start"), result.gets("end"), result.gets("value")):
        for r in result:
            for s in r:
                self.c.execute("INSERT OR IGNORE INTO cache VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                    (fileId, datetime.now(), zoomLvl, s[0], s[1], chrom, s[2], ""))
        self.db.commit()

    async def bigwigWrapper(self, fileObj, chrom, startIndex, endIndex, points, fileId):
        result=[]
        points = (endIndex - startIndex) if points > (endIndex - startIndex) else points
        step = (endIndex - startIndex)*1.0/points
        zoomLvl, _ = await fileObj.getZoom(step)
        print(zoomLvl)
        (start, end, dbRusult) = await self.sqlQueryBW(startIndex, endIndex, chrom, zoomLvl, fileId)
        for s, e in zip(start, end):
            print(s, e)
            result.append(await fileObj.getRange(chrom, s, e, zoomlvl = zoomLvl))
        addToDb = self.addToDbBW(result, chrom, fileId, zoomLvl)
        await addToDb
        # asyncio.ensure_future(addToDb)
        #return await self.mergeBW(result, dbRusult)
        result.append(dbRusult)
        return result

    async def bigbedWrapper(self, fileObj, chrom, startIndex, endIndex):
        return await fileObj.getRange(chrom, startIndex, endIndex)

    async def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.records.get(fileName) == None:
            # p = Process(target=f, args=(d, l))
            # p = BieWigProcess(fileName, "BW")
            bigwig = BigWig(fileName)
            await bigwig.getHeader()
            fileId = self.setManager(fileName, bigwig)
        else:
            bigwig, fileId = self.updateTime(fileName)
        # p.start(chrom, startIndex, endIndex, points)
        return await self.bigwigWrapper(bigwig, chrom, startIndex, endIndex, points, fileId)

    async def handleBigBed(self, fileName, chrom, startIndex, endIndex):
        if self.records.get(fileName) == None:
            # p = BieBedProcess(fileName, "BB")
            bigbed = BigBed(fileName)
            await bigbed.getHeader()
            self.setManager(fileName, bigbed)
        else:
            bigbed = self.updateTime(fileName)
        # r.start(chrom, startIndex, endIndex)
        return await self.bigbedWrapper(bigbed, chrom, startIndex, endIndex)
    