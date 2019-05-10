import pysam
from .utils import toDataFrame
from .Helper import get_range_helper

class SamFile(object):
    """
    SAM File Class to parse sam files 

    Args:
        file (str): file location can be local (full path) or hosted publicly
        columns ([str]) : column names for various columns in file
    
    Attributes:
        file: a pysam file object
        fileSrc: location of the file
        cacheData: cache of accessed data in memory
        columns: column names to use
    """
    def __init__(self, file, columns=None):
        self.file = pysam.AlignmentFile(file, "r")
        self.fileSrc = file
        self.cacheData = {}
        self.columns = columns

    def get_cache(self):
        return self.cacheData

    def set_cache(self, cache):
        self.cacheData = cache
        
    def get_bin(self, x):
        return (x.reference_name, x.reference_start, x.reference_end, x.query_alignment_sequence, x.query_sequence)

    def toDF(self, result):
        return toDataFrame(result, self.columns)

    def get_col_names(self, result):
        if self.columns is None:
            self.columns = ["chr", "start", "end", "query_alignment_sequence", "query_sequence"]
        return self.columns

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        """Get data for a given genomic location

        Args:
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            respType (str): result format type, default is "DataFrame

        Returns:
            result
                a DataFrame with matched regions from the input genomic location if respType is DataFrame else result is an array
            error 
                if there was any error during the process
        """
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

            (result, _) = get_range_helper(self.toDF, self.get_bin, self.get_col_names, chr, start, end, iter, self.columns, respType)

            return result, None
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")
