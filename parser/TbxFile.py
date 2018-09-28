import pysam
from .SamFile import SamFile

class TbxFile(SamFile):

    def __init__(self, filePath):
        self.file = pysam.TabixFile(filePath, "rb")
        self.cacheData = {}
