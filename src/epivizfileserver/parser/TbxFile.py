import pysam
from .SamFile import SamFile
from .utils import toDataFrame
from .Helper import get_range_helper

class TbxFile(SamFile):
    """
    TBX File Class to parse tbx files 

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
        self.file = pysam.TabixFile(file)
        self.cacheData = {}
        self.columns = columns

    def get_bin(self, x):
        # return (chr) + tuple(x.split('\t'))
        return tuple(x.split('\t'))

    def get_col_names(self, result):
        if self.columns is None:
            colLength = len(result)
            self.columns = ["chr", "start", "end"]
            for i in range(colLength - 3):
                self.columns.append("column" + str(i))
        return self.columns

    def toDF(self, result):
        return toDataFrame(result, self.columns)

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
            #     cols = (chr) + tuple(x.split('\t'))
            #     result.append(cols)

            # if self.columns is None: 
            #     colLength = len(result[0])
            #     self.columns = ["chr", "start", "end"]
            #     for i in colLength:
            #         self.columns.append("column" + str(i))

            # if respType is "DataFrame":
            #     result = toDataFrame(result, self.columns)

            (result, _) = get_range_helper(self.toDF, self.get_bin, self.get_col_names, chr, start, end, iter, self.columns, respType)

            return result, None
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")
