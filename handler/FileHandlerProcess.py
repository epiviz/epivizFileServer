from multiprocessing import Process, Manager, Lock
from parser import BaseFile, BigWig, BigBed
from datetime import datetime, timedelta
import pickle
import os


class FileHandlerProcess(object):
    """docstring for ProcessHandler"""
    def __init__(self, timePeriod):
        # self.manager = Manager()
        # self.dict = self.manager.dict()
        self.records = {}
        self.timePeriod = timePeriod
        self.ManagerLock = Lock()
        self.counter = 0

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
        print(record["fileObj"])
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

        return record["fileObj"]

    async def bigwigWrapper(self, fileObj, chrom, startIndex, endIndex, points):
        return await fileObj.getRange(chrom, startIndex, endIndex, points)

    async def bigbedWrapper(self, fileObj, chrom, startIndex, endIndex):
        return await fileObj.getRange(chrom, startIndex, endIndex)

    async def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.records.get(fileName) == None:
            # p = Process(target=f, args=(d, l))
            # p = BieWigProcess(fileName, "BW")
            bigwig = BigWig(fileName)
            self.setManager(fileName, bigwig)
        else:
            bigwig = self.updateTime(fileName)
        # p.start(chrom, startIndex, endIndex, points)
        return await self.bigwigWrapper(bigwig, chrom, startIndex, endIndex, points)

    async def handleBigBed(self, fileName, chrom, startIndex, endIndex):
        if self.records.get(fileName) == None:
            # p = BieBedProcess(fileName, "BB")
            bigbed = BigBed(fileName)
            self.setManager(fileName, bigbed)
        else:
            bigbed = self.updateTime(fileName)
        # r.start(chrom, startIndex, endIndex)
        return await self.bigbedWrapper(bigbed, chrom, startIndex, endIndex)
    