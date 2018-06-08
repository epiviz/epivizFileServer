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

        f = open(self.file, "rb")
        f.seek(0)

        # parse magic code for byteswap
        if struct.unpack("I", f.read(4))[0] == int("0x888FFC26", 0):
            self.endian = "="
        elif struct.unpack("<I", f.read(4))[0] == int("0x888FFC26", 0):
            self.endian = "<"
        else:
            raise Exception("BadFileError")

        self.header = self.parse_header(f)

        if self.header.get("uncompressBufSize") == 0:
            self.compressed = False

        f.close()

    def parse_header(self, f):
        # parse header
        f.seek(0)

        data = f.read(36)
        (magic, version, zoomLevels, chromTreeOffset, fullDataOffset, fullIndexOffset,
            fieldCount, definedFieldCount) = struct.unpack(self.endian + "IHHQQQHH", data)

        data = f.read(20)
        (autoSqlOffset, totalSummaryOffset, uncompressBufSize) = struct.unpack(self.endian + "QQI", data)

        return {"magic" : magic, "version" : version, "zoomLevels" : zoomLevels, "chromTreeOffset" : chromTreeOffset, 
                "fullDataOffset" : fullDataOffset, "fullIndexOffset" : fullIndexOffset, "fieldCount" : fieldCount, 
                "definedFieldCount" : definedFieldCount, "autoSqlOffset" : autoSqlOffset, "totalSummaryOffset" : totalSummaryOffset, 
                "uncompressBufSize" : uncompressBufSize}

    def getRange(self, chr, start, end, points=2000, metric="AVG", respType = "JSON"):
        if start == end:
            raise Exception("InputError")

        if metric is "AVG":
            metricFunc = self.aveBigWig
        f = open(self.file, "rb")
        step = (end - start)*1.0/points
        self.zoomOffset = self.getZoom(f, start, end, step)
        mean = []
        startArray = []
        endArray = []
        start = start*1.0

        averageArray = metricFunc(f, chr, start, end)

        while start < end:
            t_end = math.floor(start + step)
            t_end = end * 1.0 if (end - t_end) < step else t_end
            startArray.append(start)
            endArray.append(t_end)
            mean.append(self.averageOfArray(start, t_end, averageArray))
            start = t_end

        if respType is "JSON":
            formatFunc = self.formatAsJSON

        return formatFunc({"start" : startArray, "end" : endArray, "values": mean})

    def getZoom(self, f, start, end, step):
        f.seek(0)
        data = f.read(8)
        (_, _, zoomLevels) = struct.unpack(self.endian + "IHH", data)

        f.seek(64)
        offset = 0
        distance = step**2

        for x in range(1, zoomLevels + 1):
            data = f.read(24)
            (reductionLevel, reserved, dataOffest, indexOffset) = struct.unpack(self.endian + "IIQQ", data)
            newDis = ((reductionLevel - step)*1.0) ** 2
            if newDis < distance:
                distance = newDis
                offset = indexOffset

        return offset

    def aveBigWig(self, f, chr, start, end):

        chromArray = []
        headNodeOffset = self.zoomOffset + 48 if self.zoomOffset else self.header.get("fullIndexOffset") + 48

        chrmId = self.getId(f, chr)
        if chrmId == None:
            raise Exception("didn't find chromId with the given name")
        while start < end:
            (start, sections) = self.locateTreeAverage(f, headNodeOffset, chrmId, start, end)
            if(start == None and sections == None):
                raise Exception("With the given chr name, the range was not found")
            for section in sections:
                chromArray.append(section)
        
        return chromArray

    def getId(self, f, chrmzone):
        if not hasattr(self, 'chrmIds'):
            self.chrmIds = {}
            f.seek(self.header.get("chromTreeOffset"))
            data = f.read(4)
            treeMagic = struct.unpack(self.endian + "I", data)
            treeMagic = treeMagic[0]

            data = f.read(12)
            (blockSize, keySize, valSize) = struct.unpack(self.endian + "III", data)
            data = f.read(16)
            (itemCount, treeReserved) = struct.unpack(self.endian + "QQ", data)
            data = f.read(4)

            chrmId = -1
            (isLeaf, nodeReserved, count) = struct.unpack(self.endian + "BBH", data) 
            for y in range(0, count):
                key = ""
                for x in range(0, keySize):
                    data = f.read(1)
                    temp = struct.unpack(self.endian + "b", data) 
                    if chr(temp[0]) != "\x00":
                        key += chr(temp[0])
                if isLeaf == 1:
                    data = f.read(8)
                    (chromId, chromSize) = struct.unpack(self.endian + "II", data)
                    self.chrmIds[key] = chromId

        return self.chrmIds.get(chrmzone)

    # returns (startIndex, [(startIndex, endIndex, average)s])
    # the returned cRange is the final endIndex - startIndex - 1
    def locateTreeAverage(self, f, rTree, chrmId, startIndex, endIndex):
        offset = rTree
        i = 0
        node = self.readRtreeHeadNode(f, rTree)
        rCount = node["rCount"]
        isLeaf = node["rIsLeaf"]
        while i < rCount:
            i += 1
            node = self.readRtreeNode(f, offset, isLeaf)
            
            # query this layer of rTree
            # if leaf layer
            if node["rStartChromIx"] > chrmId:
                break
            # rEndBase - 1 for inclusive range
            elif node["rEndChromIx"] >= chrmId  and node["rStartChromIx"] <= chrmId:
                if isLeaf == 1 and not (node["rStartChromIx"] == chrmId and startIndex >= node["rStartBase"] and startIndex < node["rEndBase"] - 1):
                    offset = node["nextOff"]
                elif isLeaf == 1:
                    return self.grepSections(f, node["rdataOffset"], node["rDataSize"], startIndex, endIndex)
                elif isLeaf == 0:
                    # do the recursive call
                    # jump to next layer
                    (start, content) = self.locateTreeAverage(f, node["rdataOffset"], chrmId, startIndex, endIndex)
                    if start == None and content == None:
                        offset = node["nextOff"]
                    else:
                        return(start, content)
                else:
                    raise Exception("BadFileError")
            else:
                offset = node["nextOff"]

        # if didn't found the right intersection bad request
        return None, None

    def readRtreeNode(self, f, offset, isLeaf):
        f.seek(offset)
        data = f.read(4)
        (rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "BBH", data)
        if isLeaf:
            data = f.read(32)
            (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(self.endian + "IIIIQQ", data)
            return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "rDataSize": rDataSize, "nextOff": offset + 32}
        else:
            data = f.read(24)
            (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(self.endian + "IIIIQ", data)
            return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "nextOff": offset + 24}

    # read 1 r tree head node
    def readRtreeHeadNode(self, f, offset):
        f.seek(offset)
        data = f.read(4)
        (rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "BBH", data)
        return self.readRtreeNode(f, offset, rIsLeaf)

    # returns (startIndex, [(startIndex, endIndex, average)s])
    def grepSections(self, f, dataOffest, dataSize, startIndex, endIndex):
        f.seek(dataOffest)
        data = f.read(dataSize)
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
            else:
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