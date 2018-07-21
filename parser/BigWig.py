from .BaseFile import BaseFile
from multiprocessing import Lock
import struct
import zlib
import os
import math

class BigWig(BaseFile):
    """
        File BigWig class
    """
    magic = "0x888FFC26"

    def __init__(self, file):
        super(BigWig, self).__init__(file)

        self.writeLockChrm = Lock()
        self.writeLockZoom = Lock()
        self.tree = {}
        self.zoomOffset = {}

    def clearLock(self):
        self.writeLockChrm = None
        self.writeLockZoom = None

    def reinitLock(self):
        self.writeLockChrm = Lock()
        self.writeLockZoom = Lock()

    def parse_header(self):
        # parse header

        data = self.get_bytes(0, 56)
        (magic, version, zoomLevels, chromTreeOffset, fullDataOffset, fullIndexOffset,
            fieldCount, definedFieldCount) = struct.unpack(self.endian + "IHHQQQHH", data[:36])

        (autoSqlOffset, totalSummaryOffset, uncompressBufSize) = struct.unpack(self.endian + "QQI", data[36:])

        return {"magic" : magic, "version" : version, "zoomLevels" : zoomLevels, "chromTreeOffset" : chromTreeOffset, 
                "fullDataOffset" : fullDataOffset, "fullIndexOffset" : fullIndexOffset, "fieldCount" : fieldCount, 
                "definedFieldCount" : definedFieldCount, "autoSqlOffset" : autoSqlOffset, "totalSummaryOffset" : totalSummaryOffset, 
                "uncompressBufSize" : uncompressBufSize}

    def getTree(self, zoomlvl):
        if zoomlvl == -2:
            (rMagic, rBlockSize, rItemCount, rStartChromIx, rStartBase, rEndChromIx, rEndBase,
                rEndFileOffset, rItemsPerSlot, rReserved) = struct.unpack("IIQIIIIQII", self.get_bytes(self.header["fullIndexOffset"], 48))
            return self.get_bytes(self.header["fullIndexOffset"], rEndFileOffset)
        else:
            return self.get_bytes(self.zooms[zoomlvl][1], self.zooms[zoomlvl][3]) if self.zooms[zoomlvl][3] is not -1 else self.get_bytes(self.zooms[zoomlvl][1], self.zooms[0][1] - self.header["fullIndexOffset"])
        
        raise Exception("get tree error: this should not have happened")
              
    def getHeader(self):
        data = self.get_bytes(0, 4)
        # parse magic code for byteswap
        if struct.unpack("I", data)[0] == int(self.__class__.magic, 0):
            self.endian = "="
        elif struct.unpack("<I", data)[0] == int(self.__class__.magic, 0):
            self.endian = "<"
        else:
            raise Exception("BadFileError")

        self.header = self.parse_header()

        if self.header.get("uncompressBufSize") == 0:
            self.compressed = False 

    def getRange(self, chr, start, end, points=2000, zoomlvl=-1, metric="AVG", respType = "JSON"):
        print(start, end)
        if not hasattr(self, 'header'):
            self.getHeader()
        if start > end:
            print(start, end)
            raise Exception("InputError")
        elif start is end:
            return []
        # in the case that points are greater than the range
        points = (end - start) if points > (end - start) else points
        step = (end - start)*1.0/points if zoomlvl is -1 else 0
        zoomlvl, zoomOffset = self.getZoom(step, zoomlvl)
        if self.tree.get(zoomlvl) == None:
            self.tree[zoomlvl] = self.getTree(zoomlvl)
        value = []
        startArray = []
        endArray = []
        valueArray = self.getValues(chr, start, end, zoomlvl)
        # if metric is "AVG":
        #     metricFunc = self.averageOfArray
        for item in valueArray:
            startArray.append(item[0])
            endArray.append(item[1])
            value.append(item[2])
            start = item[1]
        if respType is "JSON":
            formatFunc = self.formatAsJSON
        # return formatFunc({"start" : startArray, "end" : endArray, "values": value})
        return valueArray

    # a note on zoom levels: 0 to totalLevels are the regular zoom level index
    # -2 for not using zoom
    # -1 for auto zoom
    def getZoom(self, step, zoomlvl = -1):
        self.writeLockZoom.acquire()
        if not hasattr(self, 'zooms'):
            self.zooms = {}
            totalLevels = self.header.get("zoomLevels")
            data = self.get_bytes(64, totalLevels * 24)
            for level in range(0, totalLevels):
                ldata = data[level*24:(level + 1)*24]
                (reductionLevel, reserved, dataOffset, indexOffset) = struct.unpack(self.endian + "IIQQ", ldata)
                self.zooms[level] = [reductionLevel, indexOffset, dataOffset]
            # buffer placeholder for the last zoom level
            self.zooms[totalLevels - 1].append(-1)
            # set buffer size for othere zoom levels
            for level in range(0, totalLevels - 1):
                self.zooms[level].append(self.zooms[level + 1][2] - self.zooms[level][1])
        self.writeLockZoom.release()

        offset = 0
        totalLevels = self.header.get("zoomLevels")
        if zoomlvl > totalLevels or zoomlvl < -2:
            zoomlvl = -1

        if zoomlvl == -1:
            zoomlvl = -2
            distance = step ** 2
            for level in range(0, self.header.get("zoomLevels")):
                newDis = ((self.zooms[level][0] - step)*1.0) ** 2
                if newDis < distance:
                        distance = newDis
                        offset = self.zooms[level][1]
                        lvl = level
        # if it is not zero
        elif zoomlvl is not -2:
            offset = self.zooms[zoomlvl - 1][1] if not levelF else zoomlvl
            lvl = zoomlvl
        else:
            zoomlvl = -2
            offset = self.header.get("fullIndexOffset")

        return zoomlvl, offset

    def fixRange(self, startOffset, chromId, start, end):
        start = self.fixStartRange(startOffset, 48, chromId, start, end)
        maxEnd = self.getMaxEnd(startOffset, 48, chromId)
        end = maxEnd if maxEnd < end else end
        return start, end

    # re adjust the start index to the closest start point of that chrom if needed,
    # return the value of the end point or return start if none is found
    def fixStartRange(self, startOffset, offset, chromId, start, end, zoomOffset):
        i = 0
        node = self.readRtreeHeadNode(startOffset, offset, zoomOffset)
        rCount = node["rCount"]
        isLeaf = node["rIsLeaf"]
        while i < rCount:
            i += 1
            node = self.readRtreeNode(startOffset, offset, isLeaf, zoomOffset)
            # query this layer of rTree
            # if leaf layer
            if node["rStartChromIx"] > chromId:
                break
            elif node["rEndChromIx"] >= chromId and node["rStartChromIx"] <= chromId:
                if isLeaf == 1 and node["rStartChromIx"] == chromId and start <= node["rStartBase"]:
                    return node["rStartBase"]
                elif isLeaf == 1 and node["rStartChromIx"] == chromId and start > node["rStartBase"]:
                    return start
                elif isLeaf == 0:
                    # do the recursive call
                    # jump to next layer
                    startV = self.fixStartRange(startOffset, node["rdataOffset"] - startOffset, chromId, start, end)
                    if startV < end:
                        return startV
            offset = node["nextOff"] - startOffset
        return end

    # get Max end range for that chrom
    def getMaxEnd(self, startOffset, offset, chromId, zoomOffset):
        i = 0
        node = self.readRtreeHeadNode(startOffset, offset, zoomOffset)
        rCount = node["rCount"]
        isLeaf = node["rIsLeaf"]
        end = 0
        while i < rCount:
            i += 1
            node = self.readRtreeNode(startOffset, offset, isLeaf, zoomOffset)
            # query this layer of rTree
            # if leaf layer
            if node["rStartChromIx"] > chromId:
                break
            elif node["rEndChromIx"] >= chromId and node["rStartChromIx"] <= chromId:
                if isLeaf == 1 and node["rStartChromIx"] == chromId:
                    end = node["rEndBase"] if node["rEndBase"] > end else end
                elif isLeaf == 0:
                    # do the recursive call
                    # jump to next layer
                    endV = self.getMaxEnd(startOffset, node["rdataOffset"] - startOffset, chromId)
                    if endV > end:
                        end = endV

            offset = node["nextOff"] - startOffset
        return end

    def getValues(self, chr, start, end, zoomlvl):
        chromArray = []
        # startOffset = zoomOffset if zoomlvl is not -2 else self.header.get("fullIndexOffset")
        offset = 48

        chrmId = self.getId(chr)
        if chrmId == None:
            raise Exception("didn't find chromId with the given name")
        while start < end:
            (startV, sections) = self.locateTreeAverage(offset, chrmId, start, end, zoomlvl)
            if sections == []:
                break
            else:
                start = startV
            for section in sections:
                chromArray.append(section)
        
        return chromArray

    def getId(self, chrmzone):
        print(chrmzone)
        self.writeLockChrm.acquire()
        if not hasattr(self, 'chrmIds'):
            self.chrmIds = {}
            chromosomeTreeOffset = self.header.get("chromTreeOffset")
            data = self.get_bytes(chromosomeTreeOffset, 36)

            (treeMagic, blockSize, keySize, valSize, itemCount, treeReserved, isLeaf, nodeReserved, count) = struct.unpack(self.endian + "IIIIQQBBH", data)
            chrmId = -1

            data = self.get_bytes(chromosomeTreeOffset + 36, count*(keySize + 8))

            for y in range(0, count):
                key = ""
                for x in range(0, keySize):
                    dataIndex = (y*(keySize+8)) + x
                    idata = data[dataIndex:dataIndex+1]
                    temp = struct.unpack(self.endian + "b", idata) 
                    if chr(temp[0]) != "\x00":
                        key += chr(temp[0])
                dataIndex = (y*(keySize+8)) + keySize
                if isLeaf == 1:
                    idata = data[dataIndex:dataIndex+8]
                    (chromId, chromSize) = struct.unpack(self.endian + "II", idata)
                    self.chrmIds[key] = chromId
        self.writeLockChrm.release()

        return self.chrmIds.get(chrmzone)

    # returns (startIndex, [(startIndex, endIndex, average)s])
    # the returned cRange is the final endIndex - startIndex - 1
    def locateTreeAverage(self, offset, chrmId, startIndex, endIndex, zoomlvl):
        i = 0
        node = self.readRtreeHeadNode(offset, zoomlvl)
        rCount = node["rCount"]
        isLeaf = node["rIsLeaf"]
        while i < rCount:
            i += 1
            node = self.readRtreeNode(offset, isLeaf, zoomlvl)
            # query this layer of rTree
            # if leaf layer
            if node["rStartChromIx"] > chrmId:
                break
            # rEndBase - 1 for inclusive range
            elif node["rEndChromIx"] >= chrmId  and node["rStartChromIx"] <= chrmId:
                # in node that contains 1 chrom in range
                if isLeaf == 1 and node["rStartChromIx"] == chrmId and node["rEndChromIx"] == chrmId and startIndex >= node["rStartBase"] and startIndex < node["rEndBase"]:
                    (startV, content) = self.grepSections(node["rdataOffset"], node["rDataSize"], startIndex, endIndex, zoomlvl)
                    return self.grepSections(node["rdataOffset"], node["rDataSize"], startIndex, endIndex, zoomlvl)
                # in node that contains 1 chrom but the given start range is less than the node range
                elif isLeaf == 1 and node["rStartChromIx"] == chrmId and node["rEndChromIx"] == chrmId and startIndex < node["rStartBase"] and endIndex > node["rStartBase"]:
                    return self.grepSections(node["rdataOffset"], node["rDataSize"], node["rStartBase"], endIndex, zoomlvl)
                # in node that contains 2 or more chroms, the accurate data is the start of the first chrom and the end of the last chrom.
                # pure garbage
                # but it should only happen in zoomed data
                elif isLeaf == 1 and zoomlvl is not -2:
                    (startV, content) = self.grepAnnoyingSections(node["rdataOffset"], node["rDataSize"], chrmId, startIndex, endIndex)
                    if startV != startIndex and len(content) != 0:
                        return (startV, content)
                elif isLeaf == 0:
                    # do the recursive call
                    # jump to next layer
                    _, startOffset = self.getZoom(0, zoomlvl = zoomlvl)
                    (startV, content) = self.locateTreeAverage(node["rdataOffset"] - startOffset, chrmId, startIndex, endIndex, zoomlvl)
                    if startV != startIndex and len(content) != 0:
                        return(startV, content)
            
            offset = node["nextOff"]

        # if didn't found the right intersection bad request
        return None, []

    def readRtreeNode(self, offset, isLeaf, zoomlvl):
        # data = self.get_bytes(offset, 4)
        data = self.tree[zoomlvl][offset:offset + 4]
        (rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "BBH", data)
        if isLeaf:
            # data = self.get_bytes(offset + 4, 32)
            data = self.tree[zoomlvl][offset + 4 : offset + 4 + 32]
            (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(self.endian + "IIIIQQ", data)
            return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "rDataSize": rDataSize, "nextOff": offset + 32}
        else:
            # data = self.get_bytes(offset + 4, 24)
            data = self.tree[zoomlvl][offset + 4 : offset + 4 + 24]
            (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(self.endian + "IIIIQ", data)
            return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "nextOff": offset + 24}

    # read 1 r tree head node
    def readRtreeHeadNode(self, offset, zoomlvl):
        # data = self.get_bytes(offset, 4)
        data = self.tree[zoomlvl][offset:offset + 4]
        (rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "BBH", data)
        return self.readRtreeNode(offset, rIsLeaf, zoomlvl)


    def grepAnnoyingSections(self, dataOffset, dataSize, chrmId, startIndex, endIndex):
        data = self.get_bytes(dataOffset, dataSize)
        decom = zlib.decompress(data) if self.compressed else data
        result = []
        itemCount = len(decom)/32
        x = 0
        while x < itemCount and startIndex < endIndex:
            (zoomChromID, start, end, _, minVal, maxVal, sumData, sumSquares) = struct.unpack("4I4f", decom[x*32 : (x+1)*32])
            x += 1
            end -= 1
            if zoomChromID == chrmId:
                if start > endIndex:
                    pass
                elif endIndex - 1 <= end:
                    value = sumData / (end - start) # if was quering for something else this could change
                    # for exclusive endIndex return
                    result.append((startIndex, endIndex, value))
                    # for inclusive endIndex return
                    # result.append((startIndex, endIndex - 1, value))
                    startIndex = endIndex
                elif end > startIndex:
                    value = sumData / (end - start) # if was quering for something else this could change
                    # for exclusive endIndex return
                    result.append((startIndex, end + 1, value))
                    # for inclusive endIndex return
                    # result.append((startIndex, end, value))
                    startIndex = end + 1

        return (startIndex, result)

    # returns (startIndex, [(startIndex, endIndex, average)s])
    def grepSections(self, dataOffset, dataSize, startIndex, endIndex, zoomlvl):
        data = self.get_bytes(dataOffset, dataSize)
        decom = zlib.decompress(data) if self.compressed else data
        if zoomlvl is not -2:
            result = []
            itemCount = len(decom)/32
        else:
            header = decom[:24]
            result = []
            (chromId, chromStart, chromEnd, itemStep, itemSpan, iType, _, itemCount) = struct.unpack(self.endian + "IIIIIBBH", header)
        x = 0
        while x < itemCount and startIndex < endIndex:
            # zoom summary
            if zoomlvl is not -2:
                (zoomChromID, start, end, _, minVal, maxVal, sumData, sumSquares) = struct.unpack("4I4f", decom[x*32 : (x+1)*32])
                end -= 1
                value = sumData / (end - start) # if was quering for something else this could change
            # bedgraph   
            elif iType == 1:
                (start, end, value) = struct.unpack(self.endian + "IIf", decom[24 + 12*x : 36 + 12*x])
                end = end - 1
            # varStep
            elif iType == 2:
                (start, value) = struct.unpack(self.endian + "If", decom[24 + 8*x : 32 + 8*x])
                if x == itemCount - 1:
                    end = chromEnd - 1
                else:
                    end = struct.unpack(self.endian + "I", decom[32 + 8*x : 36 + 8*x])[0] - 1
            # fixStep
            elif iType == 3:
                value = struct.unpack(self.endian + "f", decom[24 + 4*x : 28 + 4*x])[0]
                start = chromStart + x*itemStep
                end = chromStart + (x + 1)*itemStep - 1
            else:
                raise Exception("BadFileError")

            if start > endIndex:
                pass
            elif endIndex - 1 <= end:
                # for exclusive endIndex return
                result.append((startIndex, endIndex, value))
                # for inclusive endIndex return
                # result.append((startIndex, endIndex - 1, value))
                startIndex = endIndex
            elif end > startIndex:
                # for exclusive endIndex return
                result.append((startIndex, end + 1, value))
                # for inclusive endIndex return
                # result.append((startIndex, end, value))
                startIndex = end + 1
            x += 1

        return (startIndex, result)

    # parameter: start, end, array of (start, end, value)
    # return: mean of the values
    def averageOfArray(self, start, end, averageArray):
        count = 0
        value = 0.0

        for section in averageArray:
            if start > section[1] or start >= end:
                continue
            elif section[1] - 1 <= end:
                count += (end - start) + 1
                value = ((end - start) + 1) * section[2]
                start = end
            else:
                count += (section[1] - start) + 1
                value = ((section[1] - start) + 1) * section[2]
                start = section[1] + 1
        return 0.0 if count == 0 else value/count