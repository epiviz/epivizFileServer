import pysam
from .SamFile import SamFile

class TbxFile(SamFile):

    def __init__(self, filePath):
        self.file = pysam.TabixFile(filePath, "rb")
        self.cacheData = {}

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "JSON"):
        iter = self.file.fetch(chr, start, end)
        result = []
        for x in iter:
        	cols = x.split('\t')
        	result.append({"qname": cols[0], 
        					"flag": cols[1],
        					"chrName": cols[2],
        					"pos": cols[3],
        					"mapQuality": cols[4],
        					"cigar": cols[5],
        					"Rnext": cols[6],
        					"Pnext": cols[7],
        					"Tlen": cols[8],
        					"SEQ": cols[9],
        					"QUAL": cols[10]})

       	return result
