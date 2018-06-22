from multiprocessing import Process, Manager
from parser import BaseFile, BigWig, BigBed

class ProcessHandler(object):
    """docstring for ProcessHandler"""
    def __init__(self):
        # self.manager = Manager()
        self.manager = {}


    @staticmethod  
    def f(fileName, chrom, startIndex, endIndex, points):
        b = BigWig(fileName)
        result = b.getRange(chrom, startIndex, endIndex, points)
        return result    

    def handleBigWig(self, fileName, chrom, startIndex, endIndex, points):
        if self.manager.get(fileName) == None:
            p = Process(target=ProcessHandler.f, args=(fileName, chrom, startIndex, endIndex, points,))
            self.manager[fileName] = {'process': p}
            print(self.manager)
            return p.start()
        return self.manager.get(fileName)
    