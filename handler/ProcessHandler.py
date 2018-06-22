from multiprocessing import Process
from parser import BaseFile, BigWig, BigBed

class ProcessHandler(object):
    """docstring for ProcessHandler"""
        
    def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        b = BigWig(fileName)
        result = b.getRange(chrom, startIndex, endIndex, points)
        return result