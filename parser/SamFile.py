import pysam

class SamFile(Object):

    def __init__(self, filePath):
        self.file = pysam.AlignmentFile(filePath, "r")
        self.cacheData = {}

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "JSON"):
        iter = self.file.fetch("seq1", 10, 20)
        for x in iter:
            print (str(x))


