from multiprocessing import Process, Manager, Lock
from parser import BaseFile, BigWig, BigBed
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
            fileObj = utils.create_parser_object(fileType, 
                                                fileName)
            await fileObj.getHeader()
            self.setRecord(fileName, fileObj)

        fileObj = self.getRecord(fileName)
        # print(fileObj.cacheData.keys())
        # future = await self.client.submit(fileObj.daskWrapper, fileObj, chr, start, end, points)
        # future = self.client.submit(fileObj.getRange, chr, start, end, points)
        # future = self.client.submit(fileObj.getRange, chr, start, end, points, pure=False)
        # data, update = await self.client.gather(future)
        data, update = await fileObj.getRange(chr, start, end, points)
        # print(fileObj.cacheData.keys())
        # if update:
        #     fileObj = self.sync(fileObj, update)
        #     self.setRecord(fileName, fileObj)
        return data
