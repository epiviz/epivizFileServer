import pandas
# import umsgpack

def create_parser_object(format, source, columns=None):
    """Create appropriate File class based on file format

    Args:
        format (str): format of file
        source (str): location of file

    Returns:
        An instance of parser class
    """  
    from .BigBed import BigBed
    from .BigWig import BigWig
    from .SamFile import SamFile
    from .BamFile import BamFile
    from .TbxFile import TbxFile
    from .GtfFile import GtfFile

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
    }
    
    return req_manager[format](source, columns)

def toDataFrame(records, header = None):
    input = pandas.DataFrame(records, columns=header)
    return input

# def toMsgpack(msg):
#     return umsgpack.packb(msg)