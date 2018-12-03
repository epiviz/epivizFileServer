from multiprocessing import Process, Manager, Lock
import pickle
import threading
import handler.utils as utils
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
        # self.futures = {}
    
    def setRecord(self, name, obj):
        # self.records[name] = {"obj":obj}
        self.records[fileName] = {"fileObj":fileObj, "time": datetime.now(), "pickled": False, "pickling": False}
        # return self.records.get(fileName).get("fileObj")

    def getRecord(self, name):
        record = self.records.get(name)
        # return record["obj"]
        record["time"] = datetime.now()
        return record["fileObj"]

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
        record["fileObj"].clearLock()
        filehandler = open(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache", "wb")
        pickle.dump(record["fileObj"], filehandler)
        filehandler.close()
        record["pickling"] = False
        record["fileObj"] = None

    # def sync(self, fileObj, update):
    #     for item, d in update.items():
    #         if d: 
    #             if not hasattr(fileObj, item): 
    #                 setattr(fileObj, item, d)
    #             else :
    #                 objItem = getattr(fileObj, item)
    #                 for key, value in d.items():
    #                     if objItem.get(key) is None:
    #                         objItem[str(key)] = value 
    #                 setattr(fileObj, item, objItem)
    #     return fileObj

    # @cached()
    async def handleFile(self, fileName, fileType, chr, start, end, points = 2000):
        if self.records.get(fileName) == None:
            fileClass = utils.create_parser_object(fileType, fileName)
            fileFuture = self.client.submit(fileClass, fileName, actor=True)
            fileObj = await self.client.gather(fileFuture)
            self.setRecord(fileName, fileObj)

        fileObj = self.getRecord(fileName)
        data, _ = await fileObj.getRange(chr, start, end, points)
        return data
