import tiledb
import numpy as np
import pandas as pd

class TileDB(object):
    """
    HDF5 File Class to parse only local hdf5 files 

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
        self.count = tiledb.open(file + "/data.tiledb", 'r')
        self.rows = pd.read_csv(file + "/rows", sep="\t", index_col=0)
        self.cols = pd.read_csv(file + "/cols", sep="\t", index_col=0)
        if columns is None:
            self.columns = self.cols["samples"].values

    def getRange(self, chr, start = None, end = None, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame", treedisk=None):
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
        result = pd.DataFrame(columns=self.columns)

        try:
            result_rows = self.rows[(self.rows["chr"] == chr) & (self.rows["start"] <= end) & (self.rows["end"] >= start)]
            indices = result_rows.index.values            
            matrix = self.count[indices[0]:indices[-1]+1,] 

            result_matrix = pd.DataFrame(matrix, index=indices, columns=self.columns)           
            result_merge = pd.concat([result_rows, result_matrix], axis=1)

            return result_merge, None
        except Exception as e:
            print(str(e))
            return result, str(e)
