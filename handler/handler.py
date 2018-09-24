from multiprocessing import Process, Manager, Lock
import pickle
import threading
import handler.utils as utils
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
        self.futures = {}
    
    def setRecord(self, name, obj):
        self.records[name] = {"obj":obj}

    def getRecord(self, name):
        record = self.records.get(name)
        return record["obj"]

    def sync(self, fileObj, update):
        for item, d in update.items():
            if d: 
                if not hasattr(fileObj, item): 
                    setattr(fileObj, item, d)
                else :
                    objItem = getattr(fileObj, item)
                    for key, value in d.items():
                        if objItem.get(key) is None:
                            objItem[str(key)] = value 
                    setattr(fileObj, item, objItem)
        return fileObj

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
