import pysam

class SamFile(object):

    def __init__(self, filePath):
        self.file = pysam.AlignmentFile(filePath, "r")
        self.cacheData = {}

    def get_cache():
        return self.cacheData

    def set_cache(cache):
        self.cacheData = cache
        
    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "JSON"):
        try:
            iter = self.file.fetch(chr, start, end)
            result = []
            for x in iter:
                returnBin = (x.pos, x.aend, x.query)
                result.append(returnBin)
            return result
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")

        


