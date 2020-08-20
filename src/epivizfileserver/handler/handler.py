# from multiprocessing import Process, Manager, Lock
import pickle
# import threading
from .utils import create_parser_object
import os
from datetime import datetime, timedelta
import ujson
import pandas as pd
import numpy as np
import math
from aiocache import cached, Cache
from aiocache.serializers import JsonSerializer, PickleSerializer
# import logging
from dask.distributed import Client

# logger = logging.getLogger(__name__)
from sanic.log import logger as logging

def bin_rows(data, chr, start, end, columns=None, metadata=None, bins = 400):
    if len(data) == 0 or len(data) <= bins: 
        return data, None

    row_cut = pd.cut(data.index, bins=bins)
    rows = {}

    groups = data.groupby(row_cut)
    rows["start"] = groups["start"].first()
    rows["end"] = groups["end"].last()
    for col in columns:
        rows[col] = groups[col].mean()
    
    return pd.DataFrame.from_dict(rows), None

class FileHandlerProcess(object):
    """
    Class to manage query, transformation and cache using dask distributed

    Args:
        fileTime (int): time to keep file objects in memory
        MAXWORKER (int): maximum workers that can be used

    Attributes:
        records: a dictionary of all file objects
        client: asynchronous dask server client
    """
    def __init__(self, fileTime, MAXWORKER, client = None):
        self.records = {}
        print("creating dask client")
        self.client = client
        # Client(asynchronous=True)
        self.fileTime = fileTime
        self.IDcount = 0
        # self.cache = Cache(Cache.MEMORY, serializer=PickleSerializer())
        # self.futures = {}
    

    def setRecord(self, name, fileObj, fileType):
        """add or update `records` with new file object

        Args:
            name (str): file name
            fileObj: file object
            fileType: file type
        """
        # self.records[name] = {"obj":obj}
        self.records[name] = {"fileObj":fileObj, "time": datetime.now(), "pickled": False, "pickling": False, 
                            "ID": self.IDcount, "fileType": fileType}
        self.IDcount += 1
        # return self.records.get(fileName).get("fileObj")

    async def getRecord(self, name):
        """get  file object from `records` by name

        Args:
            name (str): file name

        Returns:
            file object
        """
        record = self.records.get(name)
        while record["pickling"]:
            pass
        if record["pickled"]:
            record["pickling"] = True
            record["pickled"] = False
            filehandler = open(os.getcwd() + "/cache/"+ str(record["ID"]) + ".cache", "rb")

            cache = pickle.load(filehandler)
            fileType = record.get("fileType")
            fileClass = create_parser_object(fileType, name)
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
        """automated task to pickle all fileobjects to disk
        """
        logging.debug("Handler: %s" %("cleanFileObj"))
        tasks = []
        for fileName, record in self.records.items():
            if datetime.now() - record.get("time") > timedelta(seconds = self.fileTime) and not record.get("pickled"):
                tasks.append(self.pickleFileObject(fileName))
        return tasks

    async def pickleFileObject(self, fileName):
        """automated task to load a pickled file object

        Args:
            fileName: file name to load
        """
        logging.debug("Handler: %s\t%s" %(fileName,  "pickleFileObject"))
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
    
    async def get_file_object(self, fileName, fileType):
        if self.records.get(fileName) == None:
            fileClass = create_parser_object(fileType, fileName)
            fileFuture = self.client.submit(fileClass, fileName, actor=True)
            fileObj = await self.client.gather(fileFuture)
            self.setRecord(fileName, fileObj, fileType)
        fileObj = await self.getRecord(fileName)
        return fileObj

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="handlefile")
    async def handleFile(self, fileName, fileType, chr, start, end, bins = 2000):
        """submit tasks to the dask client

        Args: 
            fileName: file location
            fileType: file type
            chr: chromosome
            start: genomic start
            end: genomic end
            points: number of base-pairse to group per bin
        """
        logging.debug("Handler: %s\t%s" %(fileName,  "handleFile"))
        fileObj = await self.get_file_object(fileName, fileType)
        try:
            data, err = await fileObj.getRange(chr, start, end, bins)
        except Exception as e:
            # assuming worker is no longer available, retry
            del self.records[fileName]
            fileObj = await self.get_file_object(fileName, fileType)
            data, err = await fileObj.getRange(chr, start, end, bins)
        return data, err

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="handlesearch")
    async def handleSearch(self, fileName, fileType, query, maxResults):
        """submit tasks to the dask client

        Args: 
            fileName: file location
            fileType: file type
            chr: chromosome
            start: genomic start
            end: genomic end
        """
        logging.debug("Handler: %s\t%s" %(fileName, "handleSearch"))
        fileObj = await self.get_file_object(fileName, fileType)
        try:
            data, err = await fileObj.search_gene(query, maxResults)
        except Exception as e:
            # assuming worker is no longer available, retry
            del self.records[fileName]
            fileObj = await self.get_file_object(fileName, fileType)
            data, err = await fileObj.search_gene(query, maxResults)
        return data, err

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="binfile")
    async def binFileData(self, fileName, fileType, data, chr, start, end, bins, columns, metadata):
        """submit tasks to the dask client
        """
        logging.debug("Handler: %s\t%s" %(fileName,  "handleBinData"))
        fileFuture = self.client.submit(bin_rows, data, chr, start, end, columns=columns, metadata=metadata, bins=bins)
        data, err = await self.client.gather(fileFuture)
        return data, err