from multiprocessing import Process, Manager, Lock
import pickle
import threading
import handler.utils as utils
import os
from datetime import datetime, timedelta
from aiocache import cached, SimpleMemoryCache
from aiocache.serializers import JsonSerializer
from dask.distributed import Client
# clean up finished futures
# grab proper result from those futures

class FileHandlerProcess(object):
    """docstring for ProcessHandler"""
    def __init__(self, fileTime, MAXWORKER):
        self.records = {}
        self.client = Client(asynchronous=True)
        self.fileTime = fileTime
        self.IDcount = 0
        # self.futures = {}
    

    def setRecord(self, name, fileObj, fileType):
        # self.records[name] = {"obj":obj}
        self.records[name] = {"fileObj":fileObj, "time": datetime.now(), "pickled": False, "pickling": False, 
                            "ID": self.IDcount, "fileType": fileType}
        self.IDcount += 1
        # return self.records.get(fileName).get("fileObj")

    async def getRecord(self, name):
        record = self.records.get(name)
        while record["pickling"]:
            pass
        if record["pickled"]:
            record["pickling"] = True
            record["pickled"] = False
            filehandler = open(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache", "rb")

            cache = pickle.load(filehandler)
            fileType = record.get("fileType")
            fileClass = utils.create_parser_object(fileType, name)
            fileFuture = self.client.submit(fileClass, name, actor=True)
            fileObj = await self.client.gather(fileFuture)
            record["fileObj"] = fileObj
            await fileObj.set_cache(cache)

            record["pickling"] = False
            filehandler.close()
            os.remove(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache")
        # return record["obj"]
        record["time"] = datetime.now()
        return record.get("fileObj")

    def cleanFileOBJ(self):
        tasks = []
        for fileName, record in self.records.items():
            if datetime.now() - record.get("time") > timedelta(seconds = self.fileTime) and not record.get("pickled"):
                tasks.append(self.pickleFileObject(fileName))
        return tasks

    async def pickleFileObject(self, fileName):
        record = self.records.get(fileName)
        record["pickling"] = True
        record["pickled"] = True
        # record["fileObj"].clearLock()
        filehandler = open(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache", "wb")
        cache = await record["fileObj"].get_cache()
        # pickle.dump(record["fileObj"], filehandler)
        pickle.dump(cache, filehandler)
        filehandler.close()
        record["pickling"] = False
        record["fileObj"] = None

    # @cached()
    async def handleFile(self, fileName, fileType, chr, start, end, points = 2000):
        if self.records.get(fileName) == None:
            fileClass = utils.create_parser_object(fileType, fileName)
            fileFuture = self.client.submit(fileClass, fileName, actor=True)
            fileObj = await self.client.gather(fileFuture)
            self.setRecord(fileName, fileObj, fileType)

        fileObj = await self.getRecord(fileName)
        data, _ = await fileObj.getRange(chr, start, end, points)
        return data
