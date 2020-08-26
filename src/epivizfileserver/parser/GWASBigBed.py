from .BigBed import BigBed
import struct
import zlib
import math
import pandas as pd

class GWASBigBed(BigBed):
    """
    Bed file parser
    
    Args: 
        file (str): GWASBigBed file location
    """
    magic = "0x8789F2EB"
    def __init__(self, file, columns=None):
        self.colFlag = False
        super(GWASBigBed, self).__init__(file, columns=columns)

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame", treedisk=None):
        return super(GWASBigBed, self).getRange(chr, start, end, bins, zoomlvl = -2, metric=metric, respType=respType, treedisk=treedisk)
