import pysam
from .utils import toDataFrame

class SamFile(object):

    def __init__(self, file, columns=None):
        self.file = pysam.AlignmentFile(file, "r")
        self.fileSrc = file
        self.cacheData = {}
        self.columns = columns

    def get_cache(self):
        return self.cacheData

    def set_cache(self, cache):
        self.cacheData = cache
        
    def get_bin(x):
        return (x.reference_name, x.reference_start, x.reference_end, x.query_alignment_sequence, x.query_sequence)

    def get_col_names(columns):
        if columns is None:
            columns = ["chr", "start", "end", "query_alignment_sequence", "query_sequence"]
        return columns

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        try:
            iter = self.file.fetch(chr, start, end)
            # result = []
            # for x in iter:
            #     returnBin = (x.reference_name, x.reference_start, x.reference_end, x.query_alignment_sequence, x.query_sequence)
            #     result.append(returnBin)

            # if self.columns is None:
            #     self.columns = ["chr", "start", "end", "query_alignment_sequence", "query_sequence"]

            # if respType is "DataFrame":
            #     result = toDataFrame(result, self.columns)

            (self.columns, result, _) = get_range_helper(get_bin, get_col_names, chr, start, end, iter, self.columns)

            return result, None
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")

        


