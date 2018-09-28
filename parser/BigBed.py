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

    def parseLeafDataNode(self, chrmId, start, end, zoomlvl, rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize):
        if self.cacheData.get(str(rdataOffset)):
            decom = self.cacheData.get(str(rdataOffset))
        else:
            self.sync = True
            data = self.get_bytes(rdataOffset, rDataSize)
            decom = zlib.decompress(data) if self.compressed else data
            self.cacheData[str(rdataOffset)] = decom
        result = []
        x = 0
        length = len(decom)
        while x < length and x+12 < length:
            (chrmIdv, startv, endv) = struct.unpack(self.endian + "III", decom[x:x + 12])
            x += 12
            if chrmIdv == chrmId:
                valuev = ""
                while x < length:
                    (tempv) = struct.unpack(self.endian + "c", decom[x:x+1])
                    (tempNext) = struct.unpack(self.endian + "c", decom[x+1:x+2])
                    valuev += str(tempv[0].decode())
                    if tempNext[0].decode() == '\x00': 
                        if startv <= end:
                            result.append((chrmIdv, startv, endv, valuev))
                        break
                    x += 1
            else:
                while x < length:
                    (tempv) = struct.unpack(self.endian + "c", decom[x:x+1])
                    (tempNext) = struct.unpack(self.endian + "c", decom[x+1:x+2])
                    if tempNext[0].decode() == '\x00': 
                        break
                    x += 1
            x += 2
        return result