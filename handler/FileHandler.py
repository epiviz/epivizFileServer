from multiprocessing import Process, Manager
from parser import BaseFile, BigWig, BigBed

class FileHandler(object):
    """docstring for ProcessHandler"""
    def __init__(self):
        # self.manager = Manager()
        self.manager = {}


    # @staticmethod  
    # def f(fileName, chrom, startIndex, endIndex, points):
    #     b = BigWig(fileName)
    #     result = b.getRange(chrom, startIndex, endIndex, points)
    #     return result    

    def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.manager.get(fileName) == None:
            bigwig = BigWig(fileName)
            self.manager[fileName] = bigwig
        else:
            bigwig = self.manager[fileName]
        return bigwig.getRange(chrom, startIndex, endIndex, points)

    def handleBigBed(self, fileName, chrom, startIndex, endIndex):
        if self.manager.get(fileName) == None:
            bigbed = BigBed(fileName)
            self.manager[fileName] = bigbed
        else:
            bigbed = self.manager[fileName]
        return bigbed.getRange(chrom, startIndex, endIndex)
    