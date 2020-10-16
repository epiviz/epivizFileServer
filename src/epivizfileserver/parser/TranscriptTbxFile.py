import pysam
from .TbxFile import TbxFile
from .utils import toDataFrame
from .Helper import get_range_helper
import pandas as pd
from aiocache import cached, Cache
from aiocache.serializers import JsonSerializer, PickleSerializer

class TranscriptTbxFile(TbxFile):
    """
    Class for tabix indexed transcript files

    Args:
        file (str): file location can be local (full path) or hosted publicly
        columns ([str]) : column names for various columns in file
    
    Attributes:
        file: a pysam file object
        fileSrc: location of the file
        cacheData: cache of accessed data in memory
        columns: column names to use
    """
    def __init__(self, file, columns=['chr', 'start', 'end', 'strand', 'transcript_id', 'exon_starts', 'exon_ends', 'gene']):
        super(TranscriptTbxFile, self).__init__(file, columns=columns)
