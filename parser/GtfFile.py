import pysam
from .SamFile import SamFile
from .utils import toDataFrame

class GtfFile(SamFile):

    def __init__(self, file, columns=None):
        self.file = pysam.TabixFile(file)
        self.cacheData = {}
        self.columns = columns

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        try:
            iter = self.file.fetch(chr, start, end, parser=pysam.asGTF())
            result = []
            for x in iter:
                cols = (chr) + tuple(x.split('\t'))
                result.append(cols)

            if self.columns is None: 
                self.columns = ["chr", "feature", "source", "start", "end", "score", "strand", "frame", "attribute"]

            if respType is "DataFrame":
                result = toDataFrame(result, self.columns)

            return result, None
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")
