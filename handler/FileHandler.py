from multiprocessing import Process, Manager, Lock
from parser import BaseFile, BigWig, BigBed

class FileProcess(Process):
    """docstring for FileObj"""
    def __init__(self, fileName, fileType):
        super(FileProcess, self).__init__()
        if fileType == "BW":
            self.file = BigWig(fileName)
        elif fileType == "BB":
            self.file = BigBed(fileName)
        
        if self.file == None:
            raise Exception("fileType not supported yet :(")
        
    def run():
        raise Exception("fileType not supported yet :(")


class BieWigProcess(FileProcess):
    """docstring for BieWigProcess"""
    def __init__(self, fileName, fileType):
        super(BieWigProcess, self).__init__(fileName, fileType)

class BieBedProcess(FileProcess):
    """docstring for BieBedProcess"""
    def __init__(self, fileName, fileType):
        super(BieBedProcess, self).__init__(fileName, fileType)


class FileHandler(object):
    """docstring for ProcessHandler"""
    def __init__(self):
        self.manager = Manager()
        self.dict = self.manager.dict()
        self.record = {}
        self.ManagerLock = Lock()

    def printRecord(self):
        return str(self.record)

    def setManager(self, fileName, fileLock):
        self.ManagerLock.acquire()
        self.record[fileName] = fileLock
        self.ManagerLock.release()

    def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.record.get(fileName) == None:
            l = Lock()
            self.setManager(fileName, fileLock)
            
        if self.record.get(fileName) == None:
            # p = Process(target=f, args=(d, l))
            p = BieWigProcess(fileName, "BW")
            self.setManager(fileName, p)
        else:
            p = self.record[fileName]
        # p.start(chrom, startIndex, endIndex, points)
        return str(p)

    def handleBigBed(self, fileName, chrom, startIndex, endIndex):
        if self.record.get(fileName) == None:
            p = BieBedProcess(fileName, "BB")
            self.setManager(fileName, p)
        else:
            p = self.record[fileName]
        # r.start(chrom, startIndex, endIndex)
        return str(p)
    