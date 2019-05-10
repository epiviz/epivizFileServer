from .BigWig import BigWig
import struct
import zlib
import math

class BigBed(BigWig):
    """
    Bed file parser
    
    Args: 
        file (str): bigbed file location
    """
    magic = "0x8789F2EB"
    def __init__(self, file, columns=None):
        super(BigBed, self).__init__(file, columns=columns)

    # def getHeader(self):
    #     super(BigBed, self).getHeader()
    #     if self.columns is None:
    #         self.columns = self.get_autosql()

    def get_autosql(self):
        """parse autosql stored in file

        Returns: 
            an array of columns in file parsed from autosql
        """
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

    ## TODO:    
    ## for BigBeds, use the fullDataOffset
    ## also figure out when using zoom rec is 
    ## appropriate for BigBed
    def getZoom(self, zoomlvl=-1, binSize = 2000):
        """Get Zoom record for the given bin size

        Args:
            zoomlvl (int): zoomlvl to get
            binSize (int): bin data by bin size

        Returns: 
            zoom level
        """
        if not hasattr(self, 'zooms'):
            self.sync = True
            self.zooms = {}
            totalLevels = self.header.get("zoomLevels")
            if totalLevels <= 0:
                return -2, self.header.get("fullIndexOffset")
            data = self.get_bytes(64, totalLevels * 24)
            
            for level in range(0, totalLevels):
                ldata = data[level*24:(level + 1)*24]
                (reductionLevel, reserved, dataOffset, indexOffset) = struct.unpack(self.endian + "IIQQ", ldata)
                self.zooms[level] = [reductionLevel, indexOffset, dataOffset]

            # buffer placeholder for the last zoom level
            self.zooms[totalLevels - 1].append(-1)
            # set buffer size for other zoom levels
            for level in range(0, totalLevels - 1):
                self.zooms[level].append(self.zooms[level + 1][2] - self.zooms[level][1])

        lvl = -2
        offset = self.header.get("fullIndexOffset")
        return lvl, offset

    def parseLeafDataNode(self, chrmId, start, end, zoomlvl, rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize):
        """Parse leaf node
        """
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

        if zoomlvl is not -2:
            ## Not used currently.
            ## see todo above
            itemCount = int(len(decom)/32)
            for i in range(0, itemCount):
                (chromId, statv, endv, validCount, minVal, maxVal, sumData, sumSquares) = struct.unpack("4I4f", decom[i*32 : (i+1)*32])
        else:
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