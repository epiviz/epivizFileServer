import pysam
from .SamFile import SamFile
from .utils import toDataFrame

class TbxFile(SamFile):

    def __init__(self, file, columns):
        self.file = pysam.TabixFile(file)
        self.cacheData = {}
        self.columns = columns

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        try:
            iter = self.file.fetch(chr, start, end)
            result = []
            for x in iter:
                cols = (chr) + tuple(x.split('\t'))
                result.append(cols)

            if self.columns is None: 
                colLength = len(result[0])
                self.columns = ["chr", "start", "end"]
                for i in colLength:
                    self.columns.append("column" + str(i))

            if respType is "DataFrame":
                result = toDataFrame(result, self.columns)

            return result, None
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")
