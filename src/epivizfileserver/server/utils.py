from epivizFileParser import BigBed, BigWig, SamFile, BamFile, TbxFile, GtfParsedFile, GtfFile

import pandas
import ujson
import json


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
    Format result to a epiviz compatible format

    Args:
        input : input dataframe
        params : request parameters
        offset: defaults to True

    Returns:
        formatted JSON response
    """

    if type(input) is tuple:
        (coverage, junctions) = input

        if offset:
            minStart = coverage["start"].iloc[0]
            minEnd = coverage["end"].iloc[0]
            coverage["start"] = coverage["start"].diff()
            coverage["end"] = coverage["end"].diff()
            coverage["start"].iloc[0] = minStart
            coverage["end"].iloc[0] = minEnd

            minRegion1Start = junctions["region1_start"].iloc[0]
            minRegion2Start = junctions["region2_start"].iloc[0]
            minRegion1End = junctions["region1_end"].iloc[0]
            minRegion2End = junctions["region2_end"].iloc[0]
            junctions["region1_start"] = junctions["region1_start"].diff()
            junctions["region2_start"] = junctions["region2_start"].diff()
            junctions["region1_end"] = junctions["region1_end"].diff()
            junctions["region2_end"] = junctions["region2_end"].diff()
            junctions["region1_start"].iloc[0] = minRegion1Start
            junctions["region2_start"].iloc[0] = minRegion2Start
            junctions["region1_end"].iloc[0] = minRegion1End
            junctions["region2_end"].iloc[0] = minRegion2End

            coverage[["start", "end"]] = coverage[[
                "start", "end"]].astype(int)
            junctions[["region1_start", "region2_start", "region1_end", "region2_end"]] = junctions[[
                "region1_start", "region2_start", "region1_end", "region2_end"]].astype(int)

        coverage_dict = coverage.to_dict('list')
        junctions_dict = junctions.to_dict('list')

        data = json.dumps({
            "coverage": coverage_dict,
            "junctions": junctions_dict
        })

        return data

    if len(input) > 0:
        input.start = input.start.astype("float")
        input.end = input.end.astype("float")

    globalStartIndex = None

    data = {
        "rows": {
            "globalStartIndex": globalStartIndex,
            "useOffset": offset,
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
                "useOffset": offset,
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
