import struct
import zlib
import math

Endian = "="
compressed = True
zoomOffset = 0

def readRtreeNode(f, offset, isLeaf):
    f.seek(offset)
    data = f.read(4)
    (rIsLeaf, rReserved, rCount) = struct.unpack(Endian + "BBH", data)
    if isLeaf:
        data = f.read(32)
        (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(Endian + "IIIIQQ", data)
        return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "rDataSize": rDataSize, "nextOff": offset + 32}
    else:
        data = f.read(24)
        (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(Endian + "IIIIQ", data)
        return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "nextOff": offset + 24}

# read 1 r tree head node
def readRtreeHeadNode(f, offset):
    f.seek(offset)
    data = f.read(4)
    (rIsLeaf, rReserved, rCount) = struct.unpack(Endian + "BBH", data)
    return readRtreeNode(f, offset, rIsLeaf)
    


# returns (startIndex, [(startIndex, endIndex, average)s])
def grepSections(f, dataOffest, dataSize, startIndex, endIndex):
    f.seek(dataOffest)
    print(dataOffest)
    data = f.read(dataSize)
    decom = zlib.decompress(data) if compressed else data
    if zoomOffset:
        result = []
        itemCount = len(decom)/32
    else:
        header = decom[:24]
        result = []
        (chromId, chromStart, chromEnd, itemStep, itemSpan, 
            iType, _, itemCount) = struct.unpack(Endian + "IIIIIBBH", header)
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
            (start, end, value) = struct.unpack(Endian + "IIf", decom[24 + 12*x : 36 + 12*x])
            end = end - 1
        # varStep
        elif iType == 2:
            (start, value) = struct.unpack(Endian + "If", decom[24 + 8*x : 32 + 8*x])
            if x == itemCount - 1:
                end = chromEnd - 1
            else:
                end = struct.unpack(Endian + "I", decom[32 + 8*x : 36 + 8*x])[0] - 1
        # fixStep
        elif iType == 3:
            value = struct.unpack(Endian + "f", decom[24 + 4*x : 28 + 4*x])[0]
            start = chromStart + x*itemStep
            end = chromStart + (x + 1)*itemStep - 1
        else:
            print("bad file")
            error()

        if start > endIndex:
            break
        elif endIndex - 1 <= end:
            result.append((startIndex, endIndex - 1, value))
            startIndex = endIndex
        else:
            result.append((startIndex, end, value))
            startIndex = end + 1


    return (startIndex, result)


# returns (startIndex, [(startIndex, endIndex, average)s])
# the returned cRange is the final endIndex - startIndex - 1
def locateTreeAverage(f, rTree, chrmId, startIndex, endIndex):
    offset = rTree
    i = 0
    node = readRtreeHeadNode(f, rTree)
    rCount = node["rCount"]
    isLeaf = node["rIsLeaf"]
    while i < rCount:
        i += 1
        node = readRtreeNode(f, offset, isLeaf)
        
        # query this layer of rTree
        # if leaf layer
        if node["rStartChromIx"] > chrmId:
            print(chrmId, node["rStartChromIx"], node["rEndChromIx"], node["rStartBase"], node["rEndBase"] - 1)
            print("bad chrom name or chromZone range")
            error() # not found in tree
        # rEndBase - 1 for inclusive range
        elif node["rStartChromIx"] < chrmId and not (startIndex >= node["rStartBase"] and startIndex < node["rEndBase"] - 1):
            offset = node["nextOff"]
        else:
            if isLeaf == 1:
                return grepSections(f, node["rdataOffset"], node["rDataSize"], startIndex, endIndex)
            elif isLeaf == 0:
                # jump to next layer
                offset = node["rdataOffset"]
                node = readRtreeHeadNode(f, offset)
                rCount = node["rCount"]
                isLeaf = node["rIsLeaf"]
                i = 0
            else:
                error() # bad file

    # if didn't found the right intersection
    # bad request
    print("bad request: didn't found")
    error()

# parameter: start, end, array of (start, end, value)
# return: mean of the values
def averageOfArray(start, end, averageArray):
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

def getId(f, chromTreeOffset, chrmzone):
    f.seek(chromTreeOffset)
    data = f.read(4)
    treeMagic = struct.unpack(Endian + "I", data)
    treeMagic = treeMagic[0]

    data = f.read(12)
    (blockSize, keySize, valSize) = struct.unpack(Endian + "III", data)
    data = f.read(16)
    (itemCount, treeReserved) = struct.unpack(Endian + "QQ", data)
    data = f.read(4)

    chrmId = -1
    (isLeaf, nodeReserved, count) = struct.unpack(Endian + "BBH", data) 
    for y in range(0, count):
        key = ""
        for x in range(0, keySize):
            data = f.read(1)
            temp = struct.unpack(Endian + "b", data) 
            if chr(temp[0]) != "\x00":
                key += chr(temp[0])
        if isLeaf == 1:
            data = f.read(8)
            (chromId, chromSize) = struct.unpack(Endian + "II", data)
            if key == chrmzone:
                chrmId = chromId
    if chrmId == -1:
        print("error")
        exit() # need to handle error
    return chrmId

def getZoom(f, startIndex, endIndex, step):
    f.seek(0)
    data = f.read(8)
    (_, _, zoomLevels) = struct.unpack(Endian + "IHH", data)
    f.seek(64)
    offset = 0
    distance = step**2
    for x in range(1, zoomLevels + 1):
        data = f.read(24)
        (reductionLevel, reserved, dataOffest, indexOffset) = struct.unpack(Endian + "IIQQ", data)
        newDis = ((reductionLevel - step)*1.0) ** 2
        if newDis < distance:
            distance = newDis
            offset = indexOffset
    return offset


# end needs to be greater than start
def aveBigWig(f, chrmzone, startIndex, endIndex):
    f.seek(0)
    data = f.read(36)
    (magic, version, zoomLevels, chromTreeOffset, fullDataOffset, fullIndexOffset,
        fieldCount, definedFieldCount) = struct.unpack(Endian + "IHHQQQHH", data)

    data = f.read(20)
    (autoSqlOffset, totalSummaryOffset, uncompressBufSize) = struct.unpack(Endian + "QQI", data)
    if uncompressBufSize == 0:
        compressed = False

    chrmId = getId(f, chromTreeOffset, chrmzone)

    chromArray = []
    headNodeOffset = zoomOffset + 48 if zoomOffset else fullIndexOffset + 48
    while startIndex < endIndex:
        (startIndex, sections) = locateTreeAverage(f, headNodeOffset, chrmId, startIndex, endIndex)
        for section in sections:
            chromArray.append(section)
    
    return chromArray

# the endIndex is exclusive
# the endIndex is exclusive
def getBigwigRange(file, chrmzone, startIndex, endIndex, points):
    global zoomOffset
    if startIndex == endIndex:
        print("wrong indecies")
        error()
    f = open(file, "rb")
    step = (endIndex - startIndex)*1.0/points
    zoomOffset = getZoom(f, startIndex, endIndex, step)
    mean = []
    startArray = []
    endArray = []
    startIndex = startIndex*1.0

    f.seek(0)
    if struct.unpack("I", f.read(4))[0] == int("0x888FFC26", 0):
        pass
    elif struct.unpack("<I", f.read(4))[0] == int("0x888FFC26", 0):
        Endian = "<"
    else:
        print("unsupported file type")
        error()
    averageArray = aveBigWig(f, chrmzone, startIndex, endIndex)

    while startIndex < endIndex:
        end = math.floor(startIndex + step)
        end = endIndex * 1.0 if (endIndex - end) < step else end
        startArray.append(startIndex)
        endArray.append(end)
        mean.append(averageOfArray(startIndex, end, averageArray))
        startIndex = end

    f.close()
    return startArray, endArray, mean

def getBigbedRange(file, chromZone, startIndex, endIndex):
    if startIndex == endIndex:
        print("wrong indecies")
        error()
    f = open(file, "rb")
    if struct.unpack("I", f.read(4))[0] == int("0x8789F2EB", 0):
        pass
    elif struct.unpack("<I", f.read(4))[0] == int("0x8789F2EB", 0):
        Endian = "<"
    else:
        print("unsupported file type")
        error()
    pass
# print(getBigwigRange("39214.bigwig", "chr11", 3947953, 7164991, 2000))