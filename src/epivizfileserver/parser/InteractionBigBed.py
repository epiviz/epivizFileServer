from .BigBed import BigBed
import struct
import zlib
import math
import pandas as pd

class InteractionBigBed(BigBed):
    """
    BigBed file parser for chromosome interaction Data

    Columns in the bed file are 

        (chr, start, end, name, score, value (strength of interaction, same as value),
        exp, color,
        region1chr, region1start, region1end, region1name, region1strand,
        region2chr, region2start, region2end, region2name, region2strand)
    
    Args: 
        file (str): InteractionBigBed file location
    """
    magic = "0x8789F2EB"
    def __init__(self, file, columns=["chr", "start", "end", "name", "score", "value",
        "exp", "color",
        "region1chr", "region1start", "region1end", "region1name", "region1strand",
        "region2chr", "region2start", "region2end", "region2name", "region2strand"]):
        self.colFlag = False
        print("init interaction bigbed")
        super(InteractionBigBed, self).__init__(file, columns=columns)
        print(self.columns)

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame", treedisk=None):
        result, _ = super(InteractionBigBed, self).getRange(chr, start, end, bins, zoomlvl = -2, metric=metric, respType=respType, treedisk=treedisk)
        print(result)
        return result, _
