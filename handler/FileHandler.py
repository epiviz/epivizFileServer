from multiprocessing import Process, Manager,Lock
from parser import BaseFile, BigWig, BigBed

class FileProcess(Process):
    """docstring for FileObj"""
    def __init__(self, fileName, fileType):
        super(P, self).__init__()
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

    def run(self, chrom, startIndex, endIndex, points):
        ---- MISSING ----

class BieBedProcess(FileProcess):
    """docstring for BieBedProcess"""
    def __init__(self, fileName, fileType):
        super(BieBedProcess, self).__init__(fileName, fileType)

    def run(fileName, chrom, startIndex, endIndex):
        ---- MISSING ----


class FileHandler(object):
    """docstring for ProcessHandler"""
    def __init__(self):
        # self.manager = Manager()
        self.manager = {}
        self.ManagerLock = Lock()

    def printManager(self):
        return str(self.manager)

    def setManager(self, fileName, fileP):
        self.ManagerLock.acquire()
        self.manager[fileName] = fileP
        self.ManagerLock.release()

    def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.manager.get(fileName) == None:
            # p = Process(target=f, args=(d, l))
            p = BieWigProcess(fileName, "BW")
            self.setManager(fileName, p)
        else:
            p = self.manager[fileName]
        p.start(chrom, startIndex, endIndex, points)
        return ---- MISSING ----

    def handleBigBed(self, fileName, chrom, startIndex, endIndex):
        if self.manager.get(fileName) == None:
            p = BieBedProcess(fileName, "BB")
            self.setManager(fileName, p)
        else:
            p = self.manager[fileName]
        r.start(chrom, startIndex, endIndex)
        return ---- MISSING ----
    