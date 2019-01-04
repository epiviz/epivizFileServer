import pysam
from .SamFile import SamFile

class BamFile(SamFile):

    def __init__(self, file, columns):
        self.file = pysam.AlignmentFile(file, "rb")
        self.cacheData = {}
        self.columns = columns
