import pysam
from .SamFile import SamFile

class BamFiles(SamFile):

    def __init__(self, filePath):
        self.file = pysam.AlignmentFile(filePath, "rb")
        self.cacheData = {}
