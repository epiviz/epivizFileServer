from .BaseFile import BaseFile
import struct
import ujson
import zlib

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

        self.parse_header()

        f.close()

    def parse_header(self):
        f = open(self.file, "rb")

        # parse header
        f.seek(0)

        data = f.read(36)
        (self.magic, self.version, self.zoomLevels, self.chromTreeOffset, self.fullDataOffset, self.fullIndexOffset,
            self.fieldCount, self.definedFieldCount) = struct.unpack(self.endian + "IHHQQQHH", data)

        data = f.read(20)
        (self.autoSqlOffset, self.totalSummaryOffset, self.uncompressBufSize) = struct.unpack(self.endian + "QQI", data)

        if self.uncompressBufSize == 0:
            self.compressed = False

        f.close()

    def getRange(self, chr, start, end, points=2000, metric="AVG", respType = "JSON"):
        f = open(self.file, "rb")
        if start == end:
            raise Exception("InputError")

        if metric is "AVG":
            metricFunc = self.aveBigWig

        step = (end - start)*1.0/points
        zoomOffset = self.getZoom(f, start, end, step)
        mean = []
        startArray = []
        endArray = []

        while start < end:
            t_end = start + step 
            t_end = end if (end - t_end) < step else t_end
            startArray.append(start)
            endArray.append(t_end)
            mean.append(metricFunc(f, chr, start, t_end, zoomOffset))
            start = t_end

        if respType is "JSON":
            formatFunc = self.formatAsJSON

        return formatFunc({"start" : startArray, "end" : endArray, "values": mean})


    def formatAsJSON(self, data):
        return ujson.dumps(data)


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

    def aveBigWig(self, f, chr, start, end, zoomOffset):

        chromArray = []
        headNodeOffset = zoomOffset + 48 if zoomOffset else self.fullIndexOffset + 48

        chrmId = self.getId(f, chr)

        while start < end:
            (startIndex, sections) = self.locateTreeAverage(f, headNodeOffset, chrmId, start, end, zoomOffset)
            for section in sections:
                chromArray.append(section)
        
        return self.averageOfArray(chromArray)

    def getId(self, f, chrmzone):
        f.seek(self.chromTreeOffset)
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
                if key == chrmzone:
                    chrmId = chromId

        if chrmId == -1:
            raise Exception("InputError")

        return chrmId

    # returns (startIndex, [(startIndex, endIndex, average)s])
    # the returned cRange is the final endIndex - startIndex - 1
    def locateTreeAverage(self, f, rTree, chrmId, startIndex, endIndex, zoomOffset):
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
                raise Exception("BadChromName")
            # rEndBase - 1 for inclusive range
            elif node["rStartChromIx"] < chrmId or not (startIndex >= node["rStartBase"] and startIndex < node["rEndBase"] - 1):
                offset = node["nextOff"]
            else:
                if isLeaf == 1:
                    return self.grepSections(f, node["rdataOffset"], node["rDataSize"], startIndex, endIndex, zoomOffset)
                elif isLeaf == 0:
                    # jump to next layer
                    offset = node["rdataOffset"]
                    node = self.readRtreeHeadNode(f, offset)
                    rCount = node["rCount"]
                    isLeaf = node["rIsLeaf"]
                    i = 0
                else:
                    raise Exception("BadFileError")

        # if didn't found the right intersection bad request
        raise Exception("InputError")

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
    def grepSections(self, f, dataOffest, dataSize, startIndex, endIndex, zoomOffset):
        f.seek(dataOffest)
        data = f.read(dataSize)
        decom = zlib.decompress(data) if self.compressed else data
        if zoomOffset:
            result = []
            itemCount = len(decom)/32
        else:
            header = decom[:24]
            result = []
            (chromId, chromStart, chromEnd, itemStep, itemSpan, iType, _, itemCount) = struct.unpack(self.endian + "IIIIIBBH", header)
        x = 0
        while x < itemCount and startIndex < endIndex:
            # zoom summary
            if zoomOffset:
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

        return (startIndex, result)

    # parameter: array of (start, end, value)
    # return: mean of the values
    def averageOfArray(self, chromArray):
        count = 0
        value = 0.0
        for section in chromArray:
            count += section[1] - section[0] + 1
            value += (section[1] - section[0] + 1) * section[2]
        # this shouldn't happen, the error should be thrown else where
        # instead of reaching here
        if count == 0:
            raise Exception("InputError")
        return value/count
