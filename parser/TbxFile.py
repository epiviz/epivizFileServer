import pysam
from .SamFile import SamFile
from .utils import toDataFrame
from Helper import get_range_helper

class TbxFile(SamFile):

    def __init__(self, file, columns=None):
        self.file = pysam.TabixFile(file)
        self.cacheData = {}
        self.columns = columns

    def get_bin(x):
        return (chr) + tuple(x.split('\t'))

    def get_col_names(columns):
        if columns is None:
            colLength = len(result[0])
            columns = ["chr", "start", "end"]
            for i in colLength:
                columns.append("column" + str(i))
        return columns

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        try:
            iter = self.file.fetch(chr, start, end)
            # result = []
            # for x in iter:
            #     cols = (chr) + tuple(x.split('\t'))
            #     result.append(cols)

            # if self.columns is None: 
            #     colLength = len(result[0])
            #     self.columns = ["chr", "start", "end"]
            #     for i in colLength:
            #         self.columns.append("column" + str(i))

            # if respType is "DataFrame":
            #     result = toDataFrame(result, self.columns)

            (self.columns, result, _) = get_range_helper(get_bin, get_col_names, chr, start, end, iter, self.columns)

            return result, None
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")
