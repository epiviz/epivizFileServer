from ..parser import BigBed, BigWig
import pandas
import ujson

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
        "gtfparsed": GtfParsedFile
    }

    return req_manager[format](source)

def format_result(input, params, offset=True):
    """
    Fromat result to a epiviz compatible format

    Args:
        input : input dataframe
        params : request parameters
        offset: defaults to True

    Returns:
        formatted JSON response
    """  

    if len(input) > 0:
        input.start = input.start.astype("float")
        input.end = input.end.astype("float")
    
    globalStartIndex = None

    data = {
        "rows": {
            "globalStartIndex": globalStartIndex,
            "useOffset" : offset,
            "values": {
                "id": None,
                "chr": [],
                "strand": [],
                "metadata": {}
            }
        },
        "values": {
            "globalStartIndex": globalStartIndex,
            "values": {}
        }
    }

    col_names = input.columns.values.tolist()
    row_names = ["chr", "start", "end", "strand", "id"]

    if len(input) > 0:
        globalStartIndex = input["start"].values.min()
        
        if offset:
            minStart = input["start"].iloc[0]
            minEnd = input["end"].iloc[0]
            input["start"] = input["start"].diff()
            input["end"] = input["end"].diff()
            input["start"].iloc[0] = minStart
            input["end"].iloc[0] = minEnd


        data = {
            "rows": {
                "globalStartIndex": globalStartIndex,
                "useOffset" : offset,
                "values": {
                    "id": None,
                    "chr": [],
                    "strand": [],
                    "metadata": {}
                }
            },
            "values": {
                "globalStartIndex": globalStartIndex,
                "values": {}
            }
        }

        for col in col_names:
            if params.get("measurement") is not None and col in params.get("measurement"):
                data["values"]["values"][col] = input[col].values.tolist()
            elif col in row_names:
                data["rows"]["values"][col] = input[col].values.tolist()
            else:
                data["rows"]["values"]["metadata"][col] = input[col].values.tolist()
    else:
        data["rows"]["values"]["start"] = []
        data["rows"]["values"]["end"] = []

        if params.get("metadata") is not None:
            for met in params.get("metadata"):
                data["rows"]["values"]["metadata"][met] = []
        else:
            for col in col_names:
                if params.get("measurement") is not None and col in params.get("measurement"):
                    data["values"]["values"][col] = input[col].values.tolist()
                elif col in row_names:
                    data["rows"]["values"][col] = input[col].values.tolist()
                else:
                    data["rows"]["values"]["metadata"][col] = input[col].values.tolist()

        if params.get("measurement"):
            for col in params.get("measurement"):
                data["values"]["values"][col] = []

    data["rows"]["values"]["id"] = None

    return data

def bin_rows(input, max_rows=2000):
    """
    Helper function to bin rows to resolution

    Args:
        input: dataframe to bin
        max_rows: resolution to scale rows

    Returns:
        data frame with scaled rows
    """

    input_length = len(input)

    if input_length < max_rows:
        return input

    step = max_rows
    col_names = input.columns.values.tolist()

    input["rowGroup"] = range(0, input_length)
    input["rowGroup"] = pandas.cut(input["rowGroup"], bins=max_rows)
    input_groups = input.groupby("rowGroup")

    agg_dict = {}

    for col in col_names:
        if col in ["chr", "probe", "gene", "region"]:
            agg_dict[col] = 'first'
        elif col in ["start", "id"]:
            agg_dict[col] = 'min'
        elif col == "end":
            agg_dict[col] = 'max'
        else:
            agg_dict[col] = 'mean'

    bin_input = input_groups.agg(agg_dict)

    return bin_input