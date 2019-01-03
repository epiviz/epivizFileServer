from .BigWig import BigWig
import struct
import zlib
import math

class BigBed(BigWig):
    """
        File BigBed class
    """
    magic = "0x8789F2EB"
    def __init__(self, file, columns=None):
        super(BigBed, self).__init__(file, columns=columns)

    # def getHeader(self):
    #     super(BigBed, self).getHeader()
    #     if self.columns is None:
    #         self.columns = self.get_autosql()

    def get_autosql(self):
        data = self.get_bytes(self.header.get("autoSqlOffset"), self.header.get("totalSummaryOffset") - self.header.get("autoSqlOffset")).decode('ascii')
        columns = []
        lines = data.split("\n")
        for l in lines[3:len(lines)-2]:
            words = l.split(" ")
            words = list(filter(None, words))
            columns.append(words[1][:-1])
        allColumns = ["chr", "start", "end"]
        allColumns.extend(columns[3:])
        return allColumns

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
                            tRec = (chrmIdv, startv, endv)
                            tValues = tuple(valuev.split("\t"))
                            result.append(tRec + tValues)
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