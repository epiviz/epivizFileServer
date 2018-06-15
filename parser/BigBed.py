from .BigWig import BigWig
import struct
import zlib
import math

class BigBed(BigWig):
    """
        File BigBed class
    """
    magic = "0x8789F2EB"
    def __init__(self, file):
        super(BigBed, self).__init__(file)
        self.zoomOffset = 0

    def getRange(self, chr, start, end):
        if start >= end:
            raise Exception("InputError")

        self.tree = self.getTree()
        valueArray = self.getValues(chr, start, end)

        return valueArray

    # placeholder because of super function
    def grepAnnoyingSections(self, dataOffset, dataSize, chrmId, startIndex, endIndex):
        data = self.get_bytes(dataOffset, dataSize)
        decom = zlib.decompress(data) if self.compressed else data
        result = []
        x = 0
        length = len(decom)
        while x < length:
            (chromId, start, end) = struct.unpack("III", decom[x:x + 12])
            x += 11
            if chromId == chrmId:
                value = ""
                while x < length and not decom[x + 1] == "\0":
                    x += 1
                    value += chr(struct.unpack("b", decom[x])[0])
                x += 2
                if start > endIndex:
                    pass
                elif endIndex - 1 <= end:
                    result.append((startIndex, endIndex - 1, value))
                    startIndex = endIndex
                elif end > startIndex:
                    result.append((startIndex, end, value))
                    startIndex = end + 1
            else:
                while x < length and not decom[x + 1] == "\0":
                    x += 1
                x += 2
        return startIndex, result

    def grepSections(self, dataOffset, dataSize, startIndex, endIndex):
        data = self.get_bytes(dataOffset, dataSize)
        decom = zlib.decompress(data) if self.compressed else data
        result = []
        x = 0
        length = len(decom)

        while x < length and startIndex < endIndex:
            (chromId, start, end) = struct.unpack("III", decom[x:x + 12])
            x += 11
            value = ""
            while x < length and not decom[x + 1] == "\0":
                x += 1
                value += chr(struct.unpack("b", decom[x])[0])
            x += 2
            if start > endIndex:
                pass
            elif endIndex - 1 <= end:
                result.append((startIndex, endIndex - 1, value))
                startIndex = endIndex
            elif end > startIndex:
                result.append((startIndex, end, value))
                startIndex = end + 1
        return startIndex, result