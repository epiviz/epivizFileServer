from .BaseFile import BaseFile
import struct
import zlib
import os
import math
from .utils import toDataFrame
import pandas as pd

class BigWig(BaseFile):
    """
    BigWig file parser
    
    Args: 
        file (str): bigwig file location

    Attributes:
        tree: chromosome tree parsed from file
        columns: column names
        cacheData: locally cached data for this file
    """
    magic = "0x888FFC26"

    def __init__(self, file, columns=None):
        super(BigWig, self).__init__(file)
        self.tree = {}
        self.columns = columns
        self.getHeader()
        # self.getZoomHeader()
        self.cacheData = {}
        self.sync = False

    def get_cache(self):
        return (self.tree, self.endian, self.header, self.compressed, self.cacheData)

    def set_cache(self, cache):
        (self.tree, self.endian, self.header, self.compressed, self.cacheData) = cache

    def get_autosql(self):
        """parse autosql in file

        Returns: 
            an array of columns in file parsed from autosql
        """
        allColumns = ["chr", "start", "end", "score"]
        return allColumns

    def getHeader(self):
        """get header byte region in file
        """
        data = self.get_bytes(0, 64)

        magicd = data[0:4]
        # parse magic code for byteswap
        if struct.unpack("I", magicd)[0] == int(self.__class__.magic, 0):
            self.endian = "="
        elif struct.unpack("<I", magicd)[0] == int(self.__class__.magic, 0):
            self.endian = "<"
        else:
            raise Exception("BadFileError")

        self.header = self.parse_header(data[0:56])

        # self.headerExtra = self.get_bytes(64, (self.header.zoomLevels * 64) + )
        data = self.get_bytes(64, self.header.get("fullDataOffset") - 64)

        self.zoomBin = data[0:self.header.get("zoomLevels") * 24]
        # self.getZoom(3, 1)

        self.chromTreeBin = data[len(data) - (self.header.get("fullDataOffset") - self.header.get("chromTreeOffset")):]
        # self.getId("chr11")

        if self.columns is None:
            self.columns = self.get_autosql()

        if self.header.get("uncompressBufSize") == 0:
            self.compressed = False 

    def parse_header(self, data=None):
        """parse header in file

        Returns: 
            attributed stored in the header
        """
        if data is None:
            data = self.get_bytes(0, 56)
        (magic, version, zoomLevels, chromTreeOffset, fullDataOffset, fullIndexOffset,
            fieldCount, definedFieldCount) = struct.unpack(self.endian + "IHHQQQHH", data[:36])

        (autoSqlOffset, totalSummaryOffset, uncompressBufSize) = struct.unpack(self.endian + "QQI", data[36:])

        return {"magic" : magic, "version" : version, "zoomLevels" : zoomLevels, "chromTreeOffset" : chromTreeOffset, 
                "fullDataOffset" : fullDataOffset, "fullIndexOffset" : fullIndexOffset, "fieldCount" : fieldCount, 
                "definedFieldCount" : definedFieldCount, "autoSqlOffset" : autoSqlOffset, "totalSummaryOffset" : totalSummaryOffset, 
                "uncompressBufSize" : uncompressBufSize}

    def getZoomHeader(self, data):
        self.zooms = {}
        totalLevels = self.header.get("zoomLevels")
        if totalLevels <= 0:
            return -2, self.header.get("fullIndexOffset")

        if data is None:
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

    def daskWrapper(self, fileObj, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "JSON"):
        """Dask Wrapper
        """
        if hasattr(fileObj, 'zooms'):
            self.zooms = getattr(fileObj, "zooms")
        if hasattr(fileObj, 'chrmIds'):
            self.chrmIds = getattr(fileObj, "chrmIds")
        if hasattr(fileObj, 'tree'):
            self.tree = getattr(fileObj, "tree")
        if hasattr(fileObj, 'cacheData'):
            self.cacheData = getattr(fileObj, "cacheData")
        data = self.getRange(chr, start, end, bins, zoomlvl, metric, respType)
        return data

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame", treedisk=None):
        """Get data for a given genomic location

        Args:
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            respType (str): result format type, default is "DataFrame

        Returns:
            result
                a DataFrame with matched regions from the input genomic location if respType is DataFrame else result is an array
            error 
                if there was any error during the process
        """
        # if treedisk is not None:
        self.treedisk = treedisk

        if not hasattr(self, 'header'):
            self.sync = True
            self.getHeader()
        if start > end:
            raise Exception("InputError: chromosome start > end")
        elif start is end:
            return []

        if bins is None:
            bins = 2000

        result = pd.DataFrame(columns = self.columns)

        try:
            if bins is None:
                bins = (end - start) if bins > (end - start) else bins
                
            if zoomlvl != -2:
                zoomlvl, zoomOffset = self.getZoom(zoomlvl, (end - start) / bins)
            else:
                self.zooms = {}
                zoomOffset = self.header.get("fullIndexOffset")

            values = self.getValues(chr, start, end, zoomlvl)

            if respType is "DataFrame":
                result = toDataFrame(values, self.columns)
                result["chr"] = chr

            return result, None
        except Exception as e:
            return result, str(e)

    # a note on zoom levels: 0 to totalLevels are the regular zoom level index
    # -2 for using fullDataOffset
    # -1 for auto zoom
    def getZoom(self, zoomlvl, binSize):
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

            data = self.zoomBin
            # if data is None:
            #     data = self.get_bytes(64, totalLevels * 24)
            
            for level in range(0, totalLevels):
                ldata = data[level*24:(level + 1)*24]
                (reductionLevel, reserved, dataOffset, indexOffset) = struct.unpack(self.endian + "IIQQ", ldata)
                self.zooms[level] = [reductionLevel, indexOffset, dataOffset]

            # buffer placeholder for the last zoom level
            self.zooms[totalLevels - 1].append(-1)
            # set buffer size for other zoom levels
            for level in range(0, totalLevels - 1):
                self.zooms[level].append(self.zooms[level + 1][2] - self.zooms[level][1])

        offset = 0
        lvl = zoomlvl
        totalLevels = self.header.get("zoomLevels")
        if zoomlvl > totalLevels or zoomlvl < -2:
            zoomlvl = -1

        if zoomlvl == -1:
            bestDiff = float('inf')
            diff = 0
            for level in range(0, totalLevels):
                diff = binSize - self.zooms[level][0]
                if diff >= 0 and diff < bestDiff:
                    bestDiff = diff
                    lvl = level
                    offset = self.zooms[level][1]
        # if it is not zero
        elif zoomlvl is not -2:
            offset = self.zooms[zoomlvl - 1][1]
            lvl = zoomlvl
        else:
            lvl = -2
            offset = self.header.get("fullIndexOffset")

        if lvl is -1:
            lvl = -2
            offset = self.header.get("fullIndexOffset")
        return lvl, offset

    def getTree(self, zoomlvl):
        """Get chromosome tree for a given zoom level

        Args:
            zoomlvl (int): zoomlvl to get

        Returns: 
            Tree binary bytes
        """
        if zoomlvl == -2:
            (rMagic, rBlockSize, rItemCount, rStartChromIx, rStartBase, rEndChromIx, rEndBase,
                rEndFileOffset, rItemsPerSlot, rReserved) = struct.unpack("IIQIIIIQII", self.get_bytes(self.header["fullIndexOffset"], 48))

            if len(self.zooms.keys()) == 0:
                return self.get_bytes(self.header["fullIndexOffset"], rEndFileOffset)

            return self.get_bytes(self.header["fullIndexOffset"], self.zooms[list(self.zooms.keys())[0]][1] - self.header["fullIndexOffset"])
        else:
            return self.get_bytes(self.zooms[zoomlvl][1], self.zooms[zoomlvl][3]) if self.zooms[zoomlvl][3] != -1 else self.get_bytes(self.zooms[zoomlvl][1], self.zooms[0][1] - self.header["fullIndexOffset"])
        
        raise Exception("get tree error: this should not have happened")

    def getValues(self, chr, start, end, zoomlvl):
        """Get data for a region

        Note: Do not use this directly, use getRange

        Args:
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end

        Returns: 
            data for the region
        """
        # Add offset to ignore reading the RTree index header
        offset = 48

        chrmId = self.getId(chr)
        if chrmId == None:
            raise Exception("didn't find chromId with the given name")
            
        values = self.locateTree(chrmId, start, end, zoomlvl, offset)
        return values

    def getId(self, chrmzone):
        """Get mapping of chromosome to id stored in file

        Args:
            chrmzone (str): chromosome 

        Returns: 
            id in file for the given chromosome
        """
        if not hasattr(self, 'chrmIds'):
            self.sync = True
            self.chrmIds = {}
            # chromosomeTreeOffset = self.header.get("chromTreeOffset")
            # chromosomeTreeLength = self.header.get("fullDataOffset") - self.header.get("chromTreeOffset")
            # data = self.get_bytes(chromosomeTreeOffset, chromosomeTreeLength)
            data = self.chromTreeBin

            (treeMagic, blockSize, keySize, valSize, itemCount, treeReserved, isLeaf, nodeReserved, count) = struct.unpack(self.endian + "IIIIQQBBH", data[0:36])
            chrmId = -1

            # data = self.get_bytes(chromosomeTreeOffset + 36, count*(keySize + 8))
            data = data[36:]

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

    def locateTree(self, chrmId, start, end, zoomlvl, offset):
        """Locate tree for the given region

        Args:
            chrmId (int): chromosome 
            start (int): genomic start
            end (int): genomic end
            zoomlvl (int): zoom level
            offset (int): offset position in the file

        Returns: 
            nodes in the stored R-tree
        """
        result = []
        if start >= end: 
            return result

        rootNode = self.readRtreeNode(zoomlvl, offset)
        filtered_nodes = self.traverseRtreeNodes(rootNode, zoomlvl, chrmId, start, end, [])

        for node in filtered_nodes:
            result += self.parseLeafDataNode(chrmId, start, end, zoomlvl, node[0], node[1], node[2], node[3], node[4], node[5])
        
        return result
        # return filtered_nodes

    def getTreeBytes(self, zoomlvl, start, size):

        if self.treedisk is not None:
            f = open(self.treedisk, "rb")
            zoomOffset = self.header["fullIndexOffset"]
            f.seek(start)
            return(f.read(size))

        if self.tree.get(str(zoomlvl)) is not None:
            return(self.tree.get(str(zoomlvl))[start:start+size])

        zoomOffset = 0
        if zoomlvl == -2:
            zoomOffset = self.header["fullIndexOffset"]
        else:
            zoomOffset = self.zooms[zoomlvl][1]

        return(self.get_bytes(zoomOffset + start, size))

    def readRtreeHeaderNode(self, zoomlvl):
        """Parse an Rtree Header node

        Args:
            zoomlvl (int): zoom level

        Returns: 
            header node Rtree object
        """
        # data = self.tree.get(str(zoomlvl))[:52]
        data = self.getTreeBytes(zoomlvl, 0, 52)
        (rMagic, rBlockSize, rItemCount, rStartChromIx, rStartBase, rEndChromIx, rEndBase, 
            rEndFileOffset, rItemsPerSlot, _,
         rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "IIQIIIIQIIBBH", data)

        return {"rStartChromIx": rStartChromIx, "rStartBase": rStartBase, 
                    "rEndChromIx": rEndChromIx, "rEndBase": rEndBase}

    def readRtreeNode(self, zoomlvl, offset):
        """Parse an Rtree node

        Args:
            zoomlvl (int): zoom level
            offset (int): offset in the file

        Returns: 
            node Rtree object
        """
        # data = self.tree.get(str(zoomlvl))[offset:offset + 4]
        data = self.getTreeBytes(zoomlvl, offset, 4)
        (rIsLeaf, rReserved, rCount) = struct.unpack(self.endian + "BBH", data)
        return {"rIsLeaf": rIsLeaf, "rCount": rCount, "rOffset": offset + 4}

    def traverseRtreeNodes(self, node, zoomlvl, chrmId, start, end, result = []):
        """Traverse an Rtree to get nodes in the given range
        """
        offset = node.get("rOffset")

        if self.cacheData.get(str(zoomlvl) + "-" + str(offset)):
            tree = self.cacheData.get(str(zoomlvl) + "-" + str(offset))
        else:
            if node.get("rIsLeaf"):
                # print("leaf")
                tree = self.getTreeBytes(zoomlvl, offset, node.get("rCount") * 32)
            else:
                # print("not leaf")
                tree = self.getTreeBytes(zoomlvl, offset, node.get("rCount") * 24)
            self.cacheData[str(zoomlvl) + "-" + str(offset)] = tree

        if node.get("rIsLeaf"):
            for i in range(0, node.get("rCount")):
                data = tree[(i * 32) : ((i+1) * 32)]
                # data = self.getTreeBytes(zoomlvl, offset + (i * 32), 32)
                # data = self.tree.get(str(zoomlvl))[offset + (i * 32) : offset + ( (i+1) * 32 )]
                (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(self.endian + "IIIIQQ", data)
                if chrmId < rStartChromIx: break
                if chrmId < rStartChromIx or chrmId > rEndChromIx: continue
                
                if rStartChromIx != rEndChromIx:
                    if chrmId == rStartChromIx:
                        if rStartBase >= end: continue
                    elif chrmId == rEndChromIx:
                        if rEndBase <= start: continue
                else:
                    if rStartBase >= end or rEndBase <= start: continue
                result.append((rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize))
        else:
            for i in range(0, node.get("rCount")):
                data = tree[(i * 24) : ((i+1) * 24)]
                # data = self.getTreeBytes(zoomlvl, offset + (i * 24), 24)
                # data = self.tree.get(str(zoomlvl))[offset + (i * 24) : offset + ( (i+1) * 24 )]
                (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(self.endian + "IIIIQ", data)
                if chrmId < rStartChromIx: break
                if not (chrmId >= rStartChromIx and chrmId <= rEndChromIx): continue
                if rStartChromIx != rEndChromIx:
                    if chrmId == rStartChromIx:
                        if rStartBase >=end: continue
                    elif chrmId == rEndChromIx:
                        if rEndBase <= start: continue
                else:
                    if end <= rStartBase or start >= rEndBase: continue
                
                # remove index offset since the stored binary starts from 0
                diffOffset = self.header.get("fullIndexOffset")
                if zoomlvl > -1:
                    diffOffset = self.zooms[zoomlvl][1]
                
                childNode = self.readRtreeNode(zoomlvl, rdataOffset - diffOffset)
                self.traverseRtreeNodes(childNode, zoomlvl, chrmId, start, end, result)
        return result

    # def traverseRtreeNodes(self, node, zoomlvl, chrmId, start, end, result = []):
    #     """Traverse an Rtree to get nodes in the given range
    #     """
    #     threshold = 10
    #     offset = node.get("rOffset")

    #     # print("params ", zoomlvl, chrmId, start, end)
    #     # print("node: ", node)

    #     if self.cacheData.get(str(offset)):
    #         tree = self.cacheData.get(str(offset))
    #     else:
    #         if node.get("rIsLeaf"):
    #             # print("leaf")
    #             tree = self.getTreeBytes(zoomlvl, offset, node.get("rCount") * 32)
    #         else:
    #             # print("not leaf")
    #             tree = self.getTreeBytes(zoomlvl, offset, node.get("rCount") * 24)
    #         self.cacheData[str(offset)] = tree

    #     # if node.get("rCount") < threshold: 
    #     print("normal old school linear approach")
    #     if node.get("rIsLeaf"):
    #         for i in range(0, node.get("rCount")):
    #             data = tree[(i * 32) : ((i+1) * 32)]
    #             # data = self.getTreeBytes(zoomlvl, offset + (i * 32), 32)
    #             # data = self.tree.get(str(zoomlvl))[offset + (i * 32) : offset + ( (i+1) * 32 )]
    #             (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(self.endian + "IIIIQQ", data)
    #             if chrmId < rStartChromIx: break
    #             if chrmId < rStartChromIx or chrmId > rEndChromIx: continue
                
    #             if rStartChromIx != rEndChromIx:
    #                 if chrmId == rStartChromIx:
    #                     if rStartBase >= end: continue
    #                 elif chrmId == rEndChromIx:
    #                     if rEndBase <= start: continue
    #             else:
    #                 if rStartBase >= end or rEndBase <= start: continue
    #             result.append((rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize))
    #     else:
    #         for i in range(0, node.get("rCount")):
    #             data = tree[(i * 24) : ((i+1) * 24)]
    #             # data = self.getTreeBytes(zoomlvl, offset + (i * 24), 24)
    #             # data = self.tree.get(str(zoomlvl))[offset + (i * 24) : offset + ( (i+1) * 24 )]
    #             (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(self.endian + "IIIIQ", data)
    #             if chrmId < rStartChromIx: break
    #             if not (chrmId >= rStartChromIx and chrmId <= rEndChromIx): continue
    #             if rStartChromIx != rEndChromIx:
    #                 if chrmId == rStartChromIx:
    #                     if rStartBase >=end: continue
    #                 elif chrmId == rEndChromIx:
    #                     if rEndBase <= start: continue
    #             else:
    #                 if end <= rStartBase or start >= rEndBase: continue
                
    #             # remove index offset since the stored binary starts from 0
    #             diffOffset = self.header.get("fullIndexOffset")
    #             if zoomlvl > -1:
    #                 diffOffset = self.zooms[zoomlvl][1]
                
    #             childNode = self.readRtreeNode(zoomlvl, rdataOffset - diffOffset)
    #             self.traverseRtreeNodes(childNode, zoomlvl, chrmId, start, end, result)
    #     # else:
    #     # p = 0
    #     # q = node.get("rCount")
    #     # i = (p + q) / 2
    #     # flagi = -1
    #     # # search for the node that contains exactly the start range
    #     # while i >= 0 and i < q and q > p and flagi != i:
    #     #     i = math.ceil(i)
    #     #     flagi = i
    #     #     if node.get("rIsLeaf"):
    #     #         data = tree[(i * 32) : ((i+1) * 32)]
    #     #         # data = self.tree.get(str(zoomlvl))[offset + (i * 32) : offset + ( (i+1) * 32 )]
    #     #         # data = self.getTreeBytes(zoomlvl, offset + (i * 32), 32)
    #     #         (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(self.endian + "IIIIQQ", data)
    #     #     else: 
    #     #         data = tree[(i * 24) : ((i+1) * 24)]
    #     #         # data = self.tree.get(str(zoomlvl))[offset + (i * 24) : offset + ( (i+1) * 24 )]
    #     #         # data = self.getTreeBytes(zoomlvl, offset + (i * 24), 24)
    #     #         (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(self.endian + "IIIIQ", data)
    #     #     # print(p, i, q)
    #     #     # print(rStartChromIx, rStartBase, rEndChromIx, rEndBase)
    #     #     # print(chrmId, start, end)
    #     #     # print(rStartChromIx, rEndChromIx, rStartChromIx != rEndChromIx)
    #     #     if chrmId < rStartChromIx: 
    #     #         # if not node.get("rIsLeaf"):
    #     #         #     break
    #     #         q = i
    #     #         i = (i + p - 1) / 2
    #     #         continue
    #     #     if chrmId > rEndChromIx: 
    #     #         p = i
    #     #         i = (i + q - 1) / 2
    #     #         continue
    #     #     if rStartChromIx != rEndChromIx:
    #     #         if chrmId == rStartChromIx:
    #     #             if rStartBase >= start: 
    #     #                 q = i
    #     #                 i = (i + p - 1) / 2
    #     #                 continue
    #     #         elif chrmId == rEndChromIx:
    #     #             if rEndBase <= start: 
    #     #                 p = i
    #     #                 i = (i + q - 1) / 2
    #     #                 continue
    #     #     else:
    #     #         if rStartBase >= start:
    #     #             q = i
    #     #             i = (i + p - 1) / 2
    #     #             continue
    #     #         if rEndBase <= start: 
    #     #             p = i
    #     #             i = (i + q - 1) / 2
    #     #             continue
            
    #     #     break
    #     # i = math.ceil(i)
    #     # while i < node.get("rCount"):
    #     #     if node.get("rIsLeaf"):
    #     #         data = tree[(i * 32) : ((i+1) * 32)]
    #     #         # data = self.tree.get(str(zoomlvl))[offset + (i * 32) : offset + ( (i+1) * 32 )]
    #     #         # data = self.getTreeBytes(zoomlvl, offset + (i * 32), 32)
    #     #         (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize) = struct.unpack(self.endian + "IIIIQQ", data)
    #     #     else:
    #     #         data = tree[(i * 24) : ((i+1) * 24)]
    #     #         # data = self.tree.get(str(zoomlvl))[offset + (i * 24) : offset + ( (i+1) * 24 )]
    #     #         # data = self.getTreeBytes(zoomlvl, offset + (i * 24), 24)
    #     #         (rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset) = struct.unpack(self.endian + "IIIIQ", data)    
    #     #     # print(i, node.get("rCount"))
    #     #     # print(rStartChromIx, rStartBase, rEndChromIx, rEndBase)
    #     #     # print(chrmId, start, end)
    #     #     if chrmId > rEndChromIx or chrmId < rStartChromIx:
    #     #         break
    #     #     if chrmId == rStartChromIx:
    #     #         if rStartBase >= end:
    #     #             break
    #     #     if node.get("rIsLeaf"):
    #     #         result.append((rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize))
    #     #         # result.append((rStartBase, rEndBase, rdataOffset, rDataSize))
    #     #     else:                  
    #     #         diffOffset = self.header.get("fullIndexOffset")
    #     #         if zoomlvl > -1:
    #     #             diffOffset = self.zooms[zoomlvl][1]
                
    #     #         childNode = self.readRtreeNode(zoomlvl, rdataOffset - diffOffset)
    #     #         self.traverseRtreeNodes(childNode, zoomlvl, chrmId, start, end, result)
    #     #     i +=1
    #     return result

    def parseLeafDataNode(self, chrmId, start, end, zoomlvl, rStartChromIx, rStartBase, rEndChromIx, rEndBase, rdataOffset, rDataSize):
        """Parse an Rtree leaf node
        """
        if self.cacheData.get(str(rdataOffset)):
            decom = self.cacheData.get(str(rdataOffset))
        else:
            self.sync = True
            data = self.get_bytes(rdataOffset, rDataSize)
            decom = zlib.decompress(data) if self.compressed else data
            self.cacheData[str(rdataOffset)] = decom
        result = []
        startv = 0
        itemCount = 0

        if zoomlvl is not -2:
            itemCount = int(len(decom)/32)
        else:
            header = decom[:24]
            (chromId, chromStart, chromEnd, itemStep, itemSpan, iType, _, itemCount) = struct.unpack(self.endian + "IIIIIBBH", header)
            if iType == 3:
                startv = chromStart - itemStep

        for i in range(0, itemCount):
            if zoomlvl is not -2:
                (chromId, startv, endv, validCount, minVal, maxVal, sumData, sumSquares) = struct.unpack("4I4f", decom[i*32 : (i+1)*32])
                valuev = (sumData/validCount) if validCount > 0 else sumData
            elif iType == 1:
                (startv, endv, valuev) = struct.unpack(self.endian + "IIf", decom[24 + 12*i : 24 + 12*(i+1)])
            elif iType == 2:
                (startv, valuev) = struct.unpack(self.endian + "If", decom[24 + 8*i : 24 + 8*(i+1)])
                end = startv + itemSpan
            elif iType ==3:
                (valuev) = struct.unpack(self.endian + "f", decom[24 + 4*i : 24 + 4*(i+1)])
                startv += itemStep
                endv = startv + itemSpan
            
            if endv >= start and startv <= end and chromId == chrmId:
                result.append((chromId, startv, endv, valuev))

        return result
