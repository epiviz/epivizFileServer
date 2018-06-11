from .BaseFile import BaseFile
import struct
import zlib
import math

class BigWig(BaseFile):
    """
        File BigWig class
    """

    def __init__(self, file):
        super(BigWig, self).__init__(file)

        data = self.get_bytes(0, 4)

        # parse magic code for byteswap
        if struct.unpack("I", data)[0] == int("0x888FFC26", 0):
            self.endian = "="
        elif struct.unpack("<I", data)[0] == int("0x888FFC26", 0):
            self.endian = "<"
        else:
            raise Exception("BadFileError")

        self.header = self.parse_header()

        if self.header.get("uncompressBufSize") == 0:
            self.compressed = False

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

    def getTree(self):
        if self.zoomOffset == 0:
            return self.get_bytes(self.header["fullIndexOffset"], self.zooms[0][1] - self.header["fullIndexOffset"])
        else:
            for x in range(0, self.header["zoomLevels"]):
                if self.zooms[x][1] == self.zoomOffset:
                    return self.get_bytes(self.zooms[x][1], self.zooms[x][3]) if self.zooms[x][3] != -1 else self.get_bytes(self.zooms[x][1], self.zooms[0][1] - self.header["fullIndexOffset"])
        raise Exception("ger tree error: this should not have happened")
                

    def getRange(self, chr, start, end, points=2000, metric="AVG", respType = "JSON"):
        if start >= end:
            raise Exception("InputError")
        # in the case that points are greater than the range
        points = (end - start) if points > (end - start) else points

        step = (end - start)*1.0/points
        self.zoomOffset = self.getZoom(start, end, step)
        self.tree = self.getTree()
        mean = []
        startArray = []
        endArray = []
        start = start*1.0

        valueArray = self.getValues(chr, start, end)
        if metric is "AVG":
            metricFunc = self.averageOfArray

        while start < end:
            t_end = math.floor(start + step)
            t_end = end * 1.0 if (end - t_end) < step else t_end
            startArray.append(start)
            endArray.append(t_end)
            mean.append(metricFunc(start, t_end, valueArray))
            start = t_end

        if respType is "JSON":
            formatFunc = self.formatAsJSON

        self.tree = None
        return formatFunc({"start" : startArray, "end" : endArray, "values": mean})

    def getZoom(self, start, end, step):
        if not hasattr(self, 'zooms'):
            self.zooms = {}
            data = self.get_bytes(64, self.header.get("zoomLevels") * 24)
            for level in range(0, self.header.get("zoomLevels")):
                ldata = data[level*24:(level+1)*24]
                (reductionLevel, reserved, dataOffest, indexOffset) = struct.unpack(self.endian + "IIQQ", ldata)
                self.zooms[level] = [reductionLevel, indexOffset, dataOffest]
            # placeholder for the last zoom level
            self.zooms[self.header.get("zoomLevels") - 1].append(-1)

            for level in range(0, self.header.get("zoomLevels") - 1):
                self.zooms[level].append(self.zooms[level + 1][2] - self.zooms[level][1])
        
        offset = 0
        distance = step**2
        for level in self.zooms:
            newDis = ((self.zooms[level][0] - step)*1.0) ** 2
            if newDis < distance:
                    distance = newDis
                    offset = self.zooms[level][1]

        return offset

    def getValues(self, chr, start, end):

        chromArray = []
        headNodeOffset = self.zoomOffset if self.zoomOffset else self.header.get("fullIndexOffset")
        offset = 48

        chrmId = self.getId(chr)
        if chrmId == None:
            raise Exception("didn't find chromId with the given name")
        while start < end:
            (start, sections) = self.locateTreeAverage(headNodeOffset, offset, chrmId, start, end)
            if(start == None and sections == None):
                raise Exception("With the given chr name, the range was not found")
            for section in sections:
                chromArray.append(section)
        
        return chromArray

    def getId(self, chrmzone):
        if not hasattr(self, 'chrmIds'):
            self.chrmIds = {}

            data = self.get_bytes(self.header.get("chromTreeOffset"), 36)

            (treeMagic, blockSize, keySize, valSize, itemCount, treeReserved, isLeaf, nodeReserved, count) = struct.unpack(self.endian + "IIIIQQBBH", data)
            chrmId = -1

            data = self.get_bytes(self.header.get("chromTreeOffset") + 36, count*(keySize + 8))

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

        return self.chrmIds.get(chrmzone)

    # returns (startIndex, [(startIndex, endIndex, average)s])
    # the returned cRange is the final endIndex - startIndex - 1
    def locateTreeAverage(self, startOffset, offset, chrmId, startIndex, endIndex):
        i = 0
        node = self.readRtreeHeadNode(startOffset, offset)
        rCount = node["rCount"]
        isLeaf = node["rIsLeaf"]
        while i < rCount:
            i += 1
            node = self.readRtreeNode(startOffset, offset, isLeaf)
            
            # query this layer of rTree
            # if leaf layer
            if node["rStartChromIx"] > chrmId:
                break
            # rEndBase - 1 for inclusive range
            elif node["rEndChromIx"] >= chrmId  and node["rStartChromIx"] <= chrmId:
                if isLeaf == 1 and not (node["rStartChromIx"] == chrmId and startIndex >= node["rStartBase"] and startIndex < node["rEndBase"] - 1):
                    offset = node["nextOff"] - startOffset
                elif isLeaf == 1:
                    return self.grepSections(node["rdataOffset"], node["rDataSize"], startIndex, endIndex)
                elif isLeaf == 0:
                    # do the recursive call
                    # jump to next layer
                    (start, content) = self.locateTreeAverage(startOffset, node["rdataOffset"] - startOffset, chrmId, startIndex, endIndex)
                    if start == None and content == None:
                        offset = node["nextOff"] - startOffset
                    else:
                        return(start, content)
                else:
                    raise Exception("BadFileError")
            else:
                offset = node["nextOff"] - startOffset

        # if didn't found the right intersection bad request
        return None, None

    def readRtreeNode(self, startOffset, offset, isLeaf):
        # data = self.get_bytes(offset, 4)
        data = self.tree[offset:offset + 4]
        (rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "BBH", data)
        if isLeaf:
            # data = self.get_bytes(offset + 4, 32)
            data = self.tree[offset + 4 : offset + 4 + 32]
            (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(self.endian + "IIIIQQ", data)
            return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "rDataSize": rDataSize, "nextOff": startOffset + offset + 32}
        else:
            # data = self.get_bytes(offset + 4, 24)
            data = self.tree[offset + 4 : offset + 4 + 24]
            (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(self.endian + "IIIIQ", data)
            return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "nextOff": startOffset + offset + 24}

    # read 1 r tree head node
    def readRtreeHeadNode(self, startOffset, offset):
        # data = self.get_bytes(offset, 4)
        data = self.tree[offset:offset + 4]
        (rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "BBH", data)
        return self.readRtreeNode(startOffset, offset, rIsLeaf)

    # returns (startIndex, [(startIndex, endIndex, average)s])
    def grepSections(self, dataOffest, dataSize, startIndex, endIndex):
        data = self.get_bytes(dataOffest, dataSize)
        decom = zlib.decompress(data) if self.compressed else data
        if self.zoomOffset:
            result = []
            itemCount = len(decom)/32
        else:
            header = decom[:24]
            result = []
            (chromId, chromStart, chromEnd, itemStep, itemSpan, iType, _, itemCount) = struct.unpack(self.endian + "IIIIIBBH", header)
        x = 0
        while x < itemCount and startIndex < endIndex:
            # zoom summary
            if self.zoomOffset:
                (_, start, end, _, minVal, maxVal, sumData, sumSquares) = struct.unpack("4I4f", decom[x*32 : (x+1)*32])
                x += 1
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
                break
            elif endIndex - 1 <= end:
                result.append((startIndex, endIndex - 1, value))
                startIndex = endIndex
            elif end > startIndex:
                result.append((startIndex, end, value))
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