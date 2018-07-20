from parser import BigBed, BigWig

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
        "bb": BigBed
    }

    return req_manager[format](source)