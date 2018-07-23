from parser import BigBed, BigWig
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
        "bb": BigBed
    }

    return req_manager[format](source)

async def format_result(input_data, params, offset=True):

    measurement = params.get("measurement")[0]
    input_json = []

    for item in input_data:
        input_json.append({"start": item[0], "end": item[1], measurement: round(item[2], 2)})

    input = pandas.read_json(ujson.dumps(input_json), orient="records")
    input = input.drop_duplicates()
    input.start.astype("int32")
    input.end.astype("int32")
    input[measurement].astype("float")

    # input = pandas.DataFrame(input_data, columns = ["start", "end", measurement])
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

    if len(input) > 0:
        globalStartIndex = input["start"].values.min()
        
        if offset:
            minStart = input["start"].iloc[0]
            minEnd = input["end"].iloc[0]
            input["start"] = input["start"].diff()
            input["end"] = input["end"].diff()
            input["start"].iloc[0] = minStart
            input["end"].iloc[0] = minEnd

        col_names = input.columns.values.tolist()
        row_names = ["chr", "start", "end", "strand", "id"]

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
        # else:
        #     data["rows"]["values"]["metadata"] = None

    data["rows"]["values"]["id"] = None

    if params.get("datasource") != "genes":
        data["rows"]["values"]["strand"] = None

    return data