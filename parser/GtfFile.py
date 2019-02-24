import pysam
from .SamFile import SamFile
from .utils import toDataFrame
from .Helper import get_range_helper
import pandas as pd


class GtfFile(SamFile):

    def __init__(self, file, columns=None):
        self.file = pysam.TabixFile(file)
        self.fileSrc = file
        self.cacheData = {}
        self.columns = columns


    def get_bin(self, x):
        # return (chr) + tuple(x.split('\t'))
        result = tuple(str(x).split('\t'))
        # if seperated by space:
        if self.ensembl:
            sgn = " "
        # if seperated by =:
        else:
            sgn = "="
        attr = [list(filter(bool, subattr.strip().split(sgn, 1))) for subattr in result[8].strip().split(";")]
        attr = list(filter(bool, attr))

        # THIS IS A DICTIONARY. GREAT DESIGN.
        print(result)
        cols = [k for k,v in attr]
        data = {}
        # if (self.columns is None) or (len(self.columns) < (8+len(cols))):
        #     self.get_col_names(cols)
        for k, v in zip(self.columns, result[0:9]):
            data[k] = v
        for k,v in attr:
            data[k] = v
        return data

        # return result[0:9] + tuple([v for k,v in attr])

    def toDF(self, result):
        return pd.DataFrame.from_dict(result)
        # return toDataFrame(result)

    def get_col_names(self, result):
        return None

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame", ensembl = True):
        try:
            self.ensembl = ensembl
            self.columns = ["chr", "feature", "source", "start", "end", "score", "strand", "frame"]
            iter = self.file.fetch(chr, start, end)
            # result = []
            # for x in iter:
            #     cols = (chr) + tuple(x.split('\t'))
            #     result.append(cols)

            # if self.columns is None: 
            #     self.columns = ["chr", "feature", "source", "start", "end", "score", "strand", "frame", "attribute"]

            # if respType is "DataFrame":
            #     result = toDataFrame(result, self.columns)
            (result, _) = get_range_helper(self.toDF, self.get_bin, self.get_col_names, chr, start, end, iter, self.columns, respType)
            return result, None
        except ValueError as e:
            raise Exception("didn't find chromId with the given name")
