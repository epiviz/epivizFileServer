import pysam
from .SamFile import SamFile

class BamFile(SamFile):

    def __init__(self, file, columns=None):
        self.file = pysam.AlignmentFile(file, "rb")
        self.fileSrc = file
        self.cacheData = {}
        self.columns = columns
