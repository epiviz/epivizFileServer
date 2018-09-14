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

        # self.executor = concurrent.futures.ThreadPoolExecutor(max_workers = MAXWORKER)
    
    def setRecord(self, name, obj):
        self.records[name] = {"obj":obj}

    def getRecord(self, name):
        record = self.records.get(name)
        return record["obj"]

    def sync(self, fileObj, update):
        print(fileObj)
        for item, d in update.items():
            if d: 
                for key, value in d.items():
                    if not hasattr(fileObj, item): 
                        setattr(fileObj, item, {})
                    objItem = getattr(fileObj, item)
                    if objItem.get(key) is None:
                        objItem[key] = value            

    # @cached()
    async def handleFile(self, fileName, fileType, chr, start, end, points = 2000):
        if self.records.get(fileName) == None:
            fileObj = utils.create_parser_object(fileType, 
                                                fileName)
            self.setRecord(fileName, fileObj)
        else:
            fileObj = self.getRecord(fileName)

        print(fileObj)
        future = self.client.submit(fileObj.getRange, chr, start, end, points)
        data, update = await self.client.gather(future)
        if update:
            self.sync(fileObj, update)
        return data
