import struct
import zlib

Enddian = "="
compressed = True
useZoom = False

def readRtreeNode(f, offset, isLeaf):
    f.seek(offset)
    data = f.read(4)
    (rIsLeaf, rReserved, rCount) = struct.unpack(Enddian + "BBH", data)
    if isLeaf:
        data = f.read(32)
        (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(Enddian + "IIIIQQ", data)
        return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "rDataSize": rDataSize, "nextOff": offset + 32}
    else:
        data = f.read(24)
        (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(Enddian + "IIIIQ", data)
        return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "nextOff": offset + 24}

# read 1 r tree head node
def readRtreeHeadNode(f, offset):
    f.seek(offset)
    data = f.read(4)
    (rIsLeaf, rReserved, rCount) = struct.unpack(Enddian + "BBH", data)
    return readRtreeNode(f, offset, rIsLeaf)
    


# returns (startIndex, [(startIndex, endIndex, average)s])
def grepSections(f, dataOffest, dataSize, startIndex, endIndex):
    f.seek(dataOffest)
    data = f.read(dataSize)
    decom = zlib.decompress(data) if compressed else data
    header = decom[:24]
    result = []
    (chromId, chromStart, chromEnd, itemStep, itemSpan, 
        iType, _, itemCount) = struct.unpack(Enddian + "IIIIIBBH", header)
    for x in range(0, itemCount):
        # bedgraph
        if iType == 1:
            (start, end, value) = struct.unpack(Enddian + "IIf", decom[24 + 12*x : 36 + 12*x])
            end = end - 1
        # varStep
        elif iType == 2:
            (start, value) = struct.unpack(Enddian + "If", decom[24 + 8*x : 32 + 8*x])
            if x == itemCount - 1:
                end = chromEnd - 1
            else:
                end = struct.unpack(Enddian + "I", decom[32 + 8*x : 36 + 8*x])[0] - 1
        # fixStep
        elif iType == 3:
            value = struct.unpack("f", decom[24 + 4*x : 28 + 4*x])[0]
            start = chromStart + x*itemStep
            end = chromStart + (x + 1)*itemStep - 1
        else:
            print("bad file")
            error()

        if start > endIndex or startIndex > endIndex:
            break
        elif endIndex - 1 <= end:
            result.append((startIndex, endIndex - 1, value))
            startIndex = endIndex
        else:
            result.append((startIndex, end, value))
            startIndex = end + 1


    return (startIndex, result)

# returns (startIndex, [(startIndex, endIndex, average)s])
def grepZoomSections(f, dataOffset, dataSize, startIndex, endIndex):
    f.seek(dataOffset)
    data = f.read(2939)
    decom = zlib.decompress(data) if compressed else data
    x = 0
    result = []
    length = len(decom)
    while x*32 < length and startIndex < endIndex:
        (_, start, end, _, minVal, maxVal, sumData, sumSquares) = struct.unpack("4I4f", decom[x*32 : (x+1)*32])
        x += 1
        end -= 1
        value = sumData / (end - start) # if was quering for something else this could change
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
            print(node["rStartChromIx"], chrmId)
            print("bad chrom name or chromZone range")
            error() # not found in tree
        # rEndBase - 1 for inclusive range
        elif node["rStartChromIx"] < chrmId or not (startIndex >= node["rStartBase"] and startIndex < node["rEndBase"] - 1):
            offset = node["nextOff"]
        else:
            if isLeaf == 1:
                return grepZoomSections(f, node["rdataOffset"], node["rDataSize"], startIndex, endIndex) if useZoom else grepSections(f, node["rdataOffset"], node["rDataSize"], startIndex, endIndex)
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

# parameter: array of (start, end, value)
# return: mean of the values
def averageOfArray(chromArray):
    print(chromArray)
    count = 0
    value = 0.0
    for section in chromArray:
        count += section[1] - section[0] + 1
        value += (section[1] - section[0] + 1) * section[2]
    # this shouldn't happen, the error should be thrown else where
    # instead of reaching here
    if count == 0:
        print("bad range")
        error()
    return value/count

def getId(f, chromTreeOffset, chrmzone):
    f.seek(chromTreeOffset)
    data = f.read(4)
    treeMagic = struct.unpack(Enddian + "I", data)
    treeMagic = treeMagic[0]

    data = f.read(12)
    (blockSize, keySize, valSize) = struct.unpack(Enddian + "III", data)
    data = f.read(16)
    (itemCount, treeReserved) = struct.unpack(Enddian + "QQ", data)
    data = f.read(4)

    chrmId = -1
    (isLeaf, nodeReserved, count) = struct.unpack(Enddian + "BBH", data) 
    for y in range(0, count):
        key = ""
        for x in range(0, keySize):
            data = f.read(1)
            temp = struct.unpack(Enddian + "b", data) 
            if chr(temp[0]) != "\x00":
                key += chr(temp[0])
        if isLeaf == 1:
            data = f.read(8)
            (chromId, chromSize) = struct.unpack(Enddian + "II", data)
            if key == chrmzone:
                chrmId = chromId
    if chrmId == -1:
        print("error")
        exit() # need to handle error
    return chrmId

def getZoom(f, zoomLevels):
    f.seek(64)
    zooms = []
    for x in range(1, zoomLevels + 1):
        zoom = {}
        data = f.read(24)
        (reductionLevel, reserved, dataOffest, indexOffset) = struct.unpack("=IIQQ", data)
        zoom["level"] = x
        zoom["reductionLevel"] = reductionLevel
        zoom["reserved"] = reserved
        zoom["ldataOffest"] = dataOffest
        zoom["indexOffset"] = indexOffset
        zooms.append(zoom)
    return zooms[0]["indexOffset"]


# end needs to be greater than start
def aveBigWig(f, chrmzone, startIndex, endIndex):
    f.seek(0)
    data = f.read(36)
    (magic, version, zoomLevels, chromTreeOffset, fullDataOffset, fullIndexOffset,
        fieldCount, definedFieldCount) = struct.unpack(Enddian + "IHHQQQHH", data)

    data = f.read(20)
    (autoSqlOffset, totalSummaryOffset, uncompressBufSize) = struct.unpack(Enddian + "QQI", data)
    if uncompressBufSize == 0:
        compressed = False

    chrmId = getId(f, chromTreeOffset, chrmzone)

    chromArray = []
    headNodeOffset = getZoom(f, zoomLevels) + 48 if useZoom else fullIndexOffset + 48
    while startIndex < endIndex:
        (startIndex, sections) = locateTreeAverage(f, headNodeOffset, chrmId, startIndex, endIndex)
        for section in sections:
            chromArray.append(section)
    

    return averageOfArray(chromArray)

# the endIndex is exclusive
def readBioFile(file, chrmzone, startIndex, endIndex):
    # for test purposes
    global useZoom 
    # useZoom= True

    if startIndex == endIndex:
        print("wrong indecies")
    f = open(file, "rb")
    if struct.unpack("I", f.read(4))[0] == int("0x888FFC26", 0):
        mean = aveBigWig(f, chrmzone, startIndex, endIndex)
    elif struct.unpack("<I", f.read(4))[0] == int("0x888FFC26", 0):
        Enddian = "<"
        mean = aveBigWig(f, chrmzone, startIndex, endIndex)
    f.close()
    useZoom = False
    return mean

# print(readBioFile("39033.bigwig", "chrY", 0, 512))