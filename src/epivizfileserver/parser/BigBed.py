from .BigWig import BigWig
import struct
import zlib
import math
import pandas as pd

class BigBed(BigWig):
    """
    Bed file parser
    
    Args: 
        file (str): bigbed file location
    """
    magic = "0x8789F2EB"
    def __init__(self, file, columns=None):
        self.colFlag = False
        super(BigBed, self).__init__(file, columns=columns)

    def get_autosql(self):
        """parse autosql stored in file

        Returns: 
            an array of columns in file parsed from autosql
        """
        if self.header.get("autoSqlOffset") == 0:
            self.colFlag = True
            cols = ["chr", "start", "end"]
            for i in range(0, self.header.get("fieldCount") - 3):
                cols.append("column_" + str(i))
            return cols
        else:
            data = self.get_bytes(self.header.get("autoSqlOffset"), self.header.get("totalSummaryOffset") - self.header.get("autoSqlOffset"))
            data = data.decode('utf-8')
            columns = []
            lines = data.split("\n")
            for l in lines[3:len(lines)-2]:
                words = l.split(" ")
                words = list(filter(None, words))
                if len(words) > 1:
                    columns.append(words[1])
            allColumns = ["chr", "start", "end"]
            allColumns.extend(columns[3:])
            return allColumns

    def parseLeafDataNode(self, chrmId, start, end, zoomlvl, rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize):
        """Parse leaf node
        """       
        if self.cacheData.get(str(zoomlvl) + "-" + str(rdataOffset)):
            decom = self.cacheData.get(str(zoomlvl) + "-" + str(rdataOffset))
        else:
            self.sync = True
            data = self.get_bytes(rdataOffset, rDataSize)
            decom = zlib.decompress(data) if self.compressed else data
            self.cacheData[str(zoomlvl) + "-" + str(rdataOffset)] = decom

        result = []
        x = 0
        length = len(decom)

        if zoomlvl is not -2:
            ## Not used currently.
            ## see todo above
            itemCount = int(len(decom)/32)
            for i in range(0, itemCount):
                (chromId, statv, endv, validCount, minVal, maxVal, sumData, sumSquares) = struct.unpack("4I4f", decom[i*32 : (i+1)*32])
        else:
            # print(chromId, chromStart, chromEnd, itemStep, itemSpan, iType, itemCount)
            while x < length:
                (chrmIdv, startv, endv) = struct.unpack(self.endian + "III", decom[x:x + 12])
                x += 12
                
                if self.header.get("fieldCount") == 3: 
                    result.append((chrmIdv, startv, endv))
                    x += 1
                elif self.header.get("fieldCount") > 3:
                    if chrmIdv == chrmId:
                        valuev = ""
                        while x < length:
                            (tempv) = struct.unpack(self.endian + "c", decom[x:x+1])
                            (tempNext) = struct.unpack(self.endian + "c", decom[x+1:x+2])
                            valuev += str(tempv[0].decode())
                            if tempNext[0].decode() == '\x00': 
                                if startv <= end:
                                    tRec = (chrmIdv, startv, endv)
                                    tValues = tuple(valuev.split("\t", len(self.columns) - 4))
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
