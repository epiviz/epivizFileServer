import pysam
from .SamFile import SamFile
from .utils import toDataFrame
from .Helper import get_range_helper
import pandas as pd
from aiocache import cached, Cache
from aiocache.serializers import JsonSerializer, PickleSerializer

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
    def __init__(self, file, columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"]):
        self.file = pysam.TabixFile(file)
        self.cacheData = {}
        self.columns = columns

        # iter = pysam.tabix_iterator(open(file), parser = pysam.asTuple)
        # (result, _) = get_range_helper(self.toDF, self.get_bin, self.get_col_names, None, None, None, iter, self.columns, respType="DataFrame")
        
        # for x, r in enumerate(self.iterator(open(file), pysam.asTuple)):
        #     print(x)
        #     print(r)

        # print("Parsing chromsomes and their lengths")
        # chromosomes = []
        # groupByChr = result.groupby("chr")

        # for name, gdf in groupByChr:
        #     chromosomes.append([name, 1, int(gdf["end"].values.max())])

        # self.chromosomes = chromosomes
            

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

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="tbxsearchgene")
    async def searchGene(self, query, maxResults = 5):
        return [], None
    
    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="tbxgetdata")
    async def get_data(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        return self.getRange(chr, start, end, bins=bins, zoomlvl=zoomlvl, metric=metric, respType=respType)

