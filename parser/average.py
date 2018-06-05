import struct
import zlib
# f = open("39033.bigwig", "rb")
# data = f.read(36)
# (magic, version, zoomLevels, chromosomeTreeOffset, fullDataOffset, fullIndexOffset,
#     fieldCount, definedFieldCount
#     ) = struct.unpack("=IHHQQQHH", data)


# data = f.read(20)
# (autoSqlOffset, totalSummaryOffset, uncompressBufSize) = struct.unpack("=QQI", data)

# data = f.read(8)
# reserved= struct.unpack("Q", data)
# reserved = reserved[0]

# #64

# zooms = []
# for x in range(1,zoomLevels + 1):
#     zoom = {}
#     data = f.read(24)
#     (reductionLevel, reserved, dataOffest, indexOffset) = struct.unpack("=IIQQ", data)
#     zoom["level"] = x
#     zoom["reductionLevel"] = reductionLevel
#     zoom["reserved"] = reserved
#     zoom["ldataOffest"] = dataOffest
#     zoom["indexOffset"] = indexOffset
#     zooms.append(zoom)

# # 64 + zoomLevels * 24

# data = f.read(40)
# (basesCovered, minVal, maxVal, sumData, sumSquares) = struct.unpack("=Qdddd", data)

# # 64 + zoomLevels * 24 + 40

# # tree header
# data = f.read(4)
# treeMagic = struct.unpack("=I", data)
# treeMagic = treeMagic[0]

# data = f.read(12)
# (blockSize, keySize, valSize) = struct.unpack("=III", data)
# data = f.read(16)
# (itemCount, treeReserved) = struct.unpack("=QQ", data)


# # parse 1 node
# data = f.read(4)
# (isLeaf, nodeReserved, count) = struct.unpack("BBH", data) 

# node = []
# for _ in range(0, count):
#     key = ""
#     for x in range(0, keySize):
#         data = f.read(1)
#         temp = struct.unpack("b", data) 
#         if chr(temp[0]) != "\x00":
#             key += chr(temp[0])
#     data = f.read(8)
#     (chromId, chromSize) = struct.unpack("II", data)
#     node.append({"key": key, "chromId": chromId, "chromSize": chromSize})

# # print header

# print("magic: " + hex(magic))
# print("version: " + hex(version))
# print("zoomLevels: " + str(zoomLevels))
# print("chromosomeTreeOffset: " + str(chromosomeTreeOffset))
# print("fullDataOffset: " + hex(fullDataOffset))
# print("fullIndexOffset: " + hex(fullIndexOffset))
# print("fieldCount: " + hex(fieldCount))
# print("definedFieldCount: " + hex(definedFieldCount))
# print("autoSqlOffset: " + hex(autoSqlOffset))
# print("totalSummaryOffset: " + hex(totalSummaryOffset))
# print("uncompressBufSize: " + hex(uncompressBufSize))
# print("reserved: " + hex(reserved))
# for zoom in zooms
#   print(zoom)
# print("basesCovered: " + hex(basesCovered))
# print("minVal: " + str(minVal))
# print("maxVal: " + str(maxVal))
# print("sumData: " + str(sumData))
# print("sumSquares: " + str(sumSquares))

# print("treeMagic: " + hex(treeMagic))
# print("blockSize: " + str(blockSize))
# print("keySize: " + str(keySize))
# print("valSize: " + str(valSize))
# print("itemCount: " + str(itemCount))
# print("reserved: " + str(treeReserved))
# print("isLeaf: " + str(isLeaf))
# print("nodeReserved: " + str(nodeReserved))
# print("count: " + str(count))
# for item in node:
#    print(item)

# # parsing data
# # full data offset jumps to dataCount
# data = f.seek(fullDataOffset)
# data = f.read(4)
# dataCount = struct.unpack("I", data)
# print(dataCount)

# data = f.seek(fullIndexOffset)
# data = f.read(48)
# (rMagic, rBlockSize, rItemCount, rStartChromIx, rStartBase, rEndChromIx, rEndBase,
#     rEndFileOffset, rItemsPerSlot, rReserved) = struct.unpack("IIQIIIIQII", data)
# print("(rMagic, rBlockSize, rItemCount, rStartChromIx, rStartBase, rEndChromIx, rEndBase, rEndFileOffset, rItemsPerSlot, rReserved)")
# print((rMagic, rBlockSize, rItemCount, rStartChromIx, rStartBase, rEndChromIx, rEndBase,
#     rEndFileOffset, rItemsPerSlot, rReserved))
# print(fullIndexOffset + 48)

# print(readRtreeHeadNode(f, fullIndexOffset + 48))
# print(readRtreeHeadNode(f, fullIndexOffset + 48 + 24))
# print(readRtreeHeadNode(f, 583410880))
# print(readRtreeNode(f, 583423176, 1))
# print(readRtreeNode(f, 583423208, 1))
# print(readRtreeNode(f, 583423240, 1))
# data = f.seek(713)
# data = f.read(4373)
# decom = zlib.decompress(data)
# header = decom[:24]

# print(struct.unpack("IIIIIBBH", header))
# print(struct.unpack("IIf", decom[24:36]))
# print(struct.unpack("IIf", decom[36:48]))

def readRtreeNode(f, offset, isLeaf):
    f.seek(offset)
    data = f.read(4)
    (rIsLeaf, rReserved, rCount) = struct.unpack("BBH", data)
    if isLeaf:
        data = f.read(32)
        (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack("IIIIQQ", data)
        return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "rDataSize": rDataSize, "nextOff": offset + 32}
    else:
        data = f.read(24)
        (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack("IIIIQ", data)
        return {"rIsLeaf": rIsLeaf, "rReserved": rReserved, "rCount": rCount, "rStartChromIx": rStartChromIx, "rStartBase": rStartBase, "rEndChromIx": rEndChromIx, "rEndBase": rEndBase, "rdataOffset": rdataOffset, "nextOff": offset + 24}

# read 1 r tree head node
def readRtreeHeadNode(f, offset):
    f.seek(offset)
    data = f.read(4)
    (rIsLeaf, rReserved, rCount) = struct.unpack("BBH", data)
    return readRtreeNode(f, offset, rIsLeaf)
    


# returns (cRange, [(startIndex, endIndex, average)s])
def grepSections(f, dataOffest, dataSize, startIndex, endIndex):
    f.seek(dataOffest)
    data = f.read(dataSize)
    decom = zlib.decompress(data)
    header = decom[:24]
    result = []
    (chromId, chromStart, chromEnd, itemStep, itemSpan, 
        iType, _, itemCount) = struct.unpack("IIIIIBBH", header)
    if iType == 1:
        for x in range(0, itemCount):
            (start, end, value) = struct.unpack("IIf", decom[24 + 12*x: 36 + 12*x])
            end = end - 1 # start and end are now inclusive
            if start > endIndex:
                break
            elif endIndex <= end:
                result.append((startIndex, endIndex, value))
                startIndex = endIndex
            else:
                result.append((startIndex, end, value))
                startIndex = end + 1
    return (startIndex, result)



# returns (cRange, [(startIndex, endIndex, average)s])
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
            error() # not found in tree
        # rEndBase - 1 for inclusive range
        elif node["rStartChromIx"] < chrmId or not (startIndex >= node["rStartBase"] and startIndex < node["rEndBase"] - 1):
            offset = node[nextOff]
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

# parameter: array of (start, end, value)
# return: mean of the values
def averageOfArray(chromArray):
    count = 0
    value = 0.0
    for section in chromArray:
        count += section[1] - section[0] + 1
        value += (section[1] - section[0] + 1) * section[2]
    return value/count

def getId(f, chromTreeOffset, chrmzone):
    f.seek(chromTreeOffset)
    data = f.read(4)
    treeMagic = struct.unpack("=I", data)
    treeMagic = treeMagic[0]

    data = f.read(12)
    (blockSize, keySize, valSize) = struct.unpack("=III", data)
    data = f.read(16)
    (itemCount, treeReserved) = struct.unpack("=QQ", data)
    data = f.read(4)
    chrmId = -1
    (isLeaf, nodeReserved, count) = struct.unpack("BBH", data) 
    for y in range(0, count):
        key = ""
        for x in range(0, keySize):
            data = f.read(1)
            temp = struct.unpack("b", data) 
            if chr(temp[0]) != "\x00":
                key += chr(temp[0])
        if isLeaf == 1:
            data = f.read(8)
            (chromId, chromSize) = struct.unpack("II", data)
        elif isLeaf == 0
            if key == chrmzone:
                chrmId = chromId
    if chrmId == -1:
        print("error")
        exit() # need to handle error
    return chrmId

# end needs to be greater than start
def aveBigWig(f, chrmzone, startIndex, endIndex):
    f.seek(0)
    data = f.read(36)
    (magic, version, zoomLevels, chromTreeOffset, fullDataOffset, fullIndexOffset,
        fieldCount, definedFieldCount) = struct.unpack("=IHHQQQHH", data)

    chrmId = getId(f, chromTreeOffset, chrmzone)

    data = f.seek(fullIndexOffset)
    data = f.read(48)
    (rMagic, rBlockSize, rItemCount, rStartChromIx, rStartBase, rEndChromIx, rEndBase,
        rEndFileOffset, rItemsPerSlot, rReserved) = struct.unpack("IIQIIIIQII", data)
    chromArray = []
    while startIndex != endIndex:
        (startIndex, sections) = locateTreeAverage(f, fullIndexOffset + 48, chrmId, startIndex, endIndex)
        for section in sections:
            chromArray.append(section)
    
    return averageOfArray(chromArray)




def readBioFile(file, chrmzone, startIndex, endIndex):
    f = open(file, "rb")
    if struct.unpack("I", f.read(4))[0] == int("0x888FFC26", 0):
        mean = aveBigWig(f, chrmzone, startIndex, endIndex)
    f.close()
    return mean


# print(readBioFile("39033.bigwig", "chrY", 2649467, 2649468))