from epivizFileParser import BigBed, BigWig, SamFile, TbxFile, TranscriptTbxFile, BamFile, GtfFile, GtfParsedFile, GWASBigBedPval, GWASBigBedPIP, TileDB, InteractionBigBed
def create_parser_object(format, source):
    """
    Create appropriate File class based on file format

    Args:
        format : Type of file
        request : Other request parameters

    Returns:
        An instance of parser class
    """  

    req_manager = {
        "BigWig": BigWig,
        "bigwig": BigWig,
        "bigWig": BigWig,
        "bw": BigWig,
        "BigBed": BigBed,
        "bigbed": BigBed,
        "bigBed": BigBed,
        "bb": BigBed,
        "sam": SamFile,
        "bam": BamFile,
        "tbx": TbxFile,
        "tabix": TbxFile,
        "gtf": GtfFile,
        "gtfparsed": GtfParsedFile,
        "gwas": GWASBigBedPval,
        "gwas_pip": GWASBigBedPIP,
        "tiledb": TileDB,
        "interaction_bigbed": InteractionBigBed,
        "interaction_bigBed": InteractionBigBed,
        "interaction_BigBed": InteractionBigBed,
        "transcript": TranscriptTbxFile
    }
    
    return req_manager[format]
    # return req_manager[format](source)

# def addFileObj(self, fileName, fileObj):
#     self.records[fileName] = {"fileObj":fileObj, "time": datetime.now(), "pickled": False, "pickling": False}
#     return self.records.get(fileName).get("fileObj")