from multiprocessing import Process, Manager,Lock
from parser import BaseFile, BigWig, BigBed

class FileHandler(object):
    """docstring for ProcessHandler"""
    def __init__(self):
        # self.manager = Manager()
        self.manager = {}
        self.lock = Lock()


    # @staticmethod  
    # def f(fileName, chrom, startIndex, endIndex, points):
    #     b = BigWig(fileName)
    #     result = b.getRange(chrom, startIndex, endIndex, points)
    #     return result    

    def printManager(self):
        return str(self.manager)

    def setManager(self, fileName, file):
        self.lock.acquire()
        self.manager[fileName] = file
        self.lock.release()

    def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.manager.get(fileName) == None:
            bigwig = BigWig(fileName)
            self.setManager(fileName, bigwig)
        else:
            bigwig = self.manager[fileName]
        return bigwig.getRange(chrom, startIndex, endIndex, points)

    def handleBigBed(self, fileName, chrom, startIndex, endIndex):
        if self.manager.get(fileName) == None:
            bigbed = BigBed(fileName)
            self.setManager(fileName, bigbed)
        else:
            bigbed = self.manager[fileName]
        return bigbed.getRange(chrom, startIndex, endIndex)
    