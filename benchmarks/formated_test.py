file = "https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig"
from parser import *
import pysam
# import msgpack
import umsgpack
import sys
import pandas as pd
import time
import json
import random

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
    # measurement = params.get("measurement")[0]
    # input_json = []
    # for item in input_data:
    #     input_json.append({"chr":item[0],  "start": item[1], "end": item[2], measurement: item[3]})
    # input = pandas.read_json(ujson.dumps(input_json), orient="records")
    # input = input.drop_duplicates()
    input.start = input.start.astype("float")
    input.end = input.end.astype("float")
    # input[measurement] = input[measurement].astype("float")
    # input["chr"] = params.get("seqName")

    # input = bin_rows(input)
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


params = {
    "datasource" : "39033",
    "metadata": None,
    "measurement": ["39033"]
}

f = BigWig(file)
for u in range(1,6):
    for x in range(1,5):
        s = random.randint(1, 500)
        r = 10**(u+3) + s
        print("testing for range ", s, r)
        result, _ = f.getRange('chr1', s, r)
        formatted_result = format_result(result, params)
        # print(formatted_result)
        print("size of formatted result")
        print(sys.getsizeof(formatted_result))

        print("original DF size")
        print(sys.getsizeof(result))
        t1 = time.time()
        ms = umsgpack.packb(formatted_result)
        t1 = time.time() - t1
        t2 = time.time()
        temp = umsgpack.unpackb(ms)
        t2 = time.time() - t2
        disk = str(10**(u+3)+x) + ".msg.testfile"
        with open(disk, 'wb') as wr:
            wr.write(bytearray(ms))
            wr.close()
        print("time to compress to msgpack: ", t1, "read from msgpack: ", t2)
        print("msgpack size: ", sys.getsizeof(ms))
        mst1 = t1
        mst2 = t2
        t1 = time.time()
        js = json.dumps(formatted_result)
        t1 = time.time() - t1
        t2 = time.time()
        temp = json.loads(js)
        t2 = time.time() - t2
        print("time to compress to json: ", t1, "read from json: ", t2)
        print("msgpack size: ", sys.getsizeof(js))
        print(" ")
        print("time difference to compress: ", mst1 - t1, "time difference to read: ", mst2 - t2)
        print("size difference: ", sys.getsizeof(ms) - sys.getsizeof(js))
        print("--------------------------")
    print("==========================")



# t = time.time()
# result = pd.read_msgpack('msp.msg')
# print(time.time() - t)

# # print(b)
# print(sys.getsizeof(b))
# print(result)
# print(sys.getsizeof(result))