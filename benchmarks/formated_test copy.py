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
for u in range(4,5):
    for x in range(1,2):
        s = random.randint(1, 500)
        r = 10**(u+3) + s
        print("testing for range ", s, r)
        result, _ = f.getRange('chr1', s, r)
        lis = result.values.tolist()
        # formatted_result = format_result(result, params)
        # print(formatted_result)
        # print("size of formatted result")
        # print(sys.getsizeof(formatted_result))
        # print(format_result)
        print("original DF size")
        print(sys.getsizeof(result))
        t1 = time.time()
        # ms = umsgpack.packb(formatted_result)
        ms = umsgpack.packb(lis)
        t1 = time.time() - t1
        t2 = time.time()
        temp = umsgpack.unpackb(ms)
        df = pd.DataFrame(temp, columns =['chr', 'start', 'end', 'score']) 
        t2 = time.time() - t2
        # disk = str(10**(u+3)+x) + ".msg.testfile"
        # with open(disk, 'wb') as wr:
        #     wr.write(bytearray(ms))
        #     wr.close()
        print("time to compress to msgpack: ", t1, "read from msgpack: ", t2)
        print("msgpack size: ", sys.getsizeof(ms))
        mst1 = t1
        mst2 = t2
        t1 = time.time()
        # js = json.dumps(formatted_result)
        js = json.dumps(lis)
        t1 = time.time() - t1
        t2 = time.time()
        temp = json.loads(js)
        df = pd.DataFrame(temp, columns =['chr', 'start', 'end', 'score']) 
        t2 = time.time() - t2
        print("time to compress to json: ", t1, "read from json: ", t2)
        print("msgpack size: ", sys.getsizeof(js))

        import os
        import sys
        sys.path.append(os.path.join(os.path.dirname(__file__), '../python'))

        import flatbuffers
        import datas
        import entry

        ft1 = time.time()
        te = time.time()
        builder = flatbuffers.Builder(0)
        bilder2 = flatbuffers.Builder(0)
        arr = []
        print(time.time() - te)
        te = time.time()
        for x in range(0, len(lis)):
            string = builder.CreateString(lis[x][0])
            entry.entryStart(builder)
            entry.entryAddChr(builder, string)
            entry.entryAddStart(builder, int(lis[x][1]))
            entry.entryAddEnd(builder, int(lis[x][2]))
            entry.entryAddValue(builder, lis[x][3])
            ent1 = entry.entryEnd(builder)
            arr.insert(0, ent1)

        print(time.time() - te)
        te = time.time()
        datas.datasStartValueVector(builder, len(lis))
        for x in range(0, len(lis)):
            builder.PrependUOffsetTRelative(arr[x])   
        valvec = builder.EndVector(len(lis))

        print(time.time() - te)
        te = time.time()
        datas.datasStart(builder)
        datas.datasAddValue(builder, valvec)
        d = datas.datasEnd(builder)
        builder.Finish(d)

        print(time.time() - te)
        te = time.time()
        buf = builder.Output()

        ft1 = time.time() - ft1
        ft2 = time.time()
        # print(buf)
        d = datas.datas.GetRootAsdatas(buf, 0)
        l = d.ValueLength()
        # ent1 = d.Value(0)
        arr = []
        for x in range(0, l):
            e = d.Value(x)
            arr.append((e.Chr().decode('ascii'), e.Start(), e.End(), e.Value()))
        df = pd.DataFrame(arr, columns =['chr', 'start', 'end', 'score']) 
        ft2 = time.time() - ft2
        print("time to compress to FlatBuffer: ", ft1, " time to read from Flatbuffer: ", ft2)
        print("flatbuffer size: ", sys.getsizeof(buf))
        print(" ")
        print("time difference to compress (mspk - js): ", mst1 - t1, "time difference to read: ", mst2 - t2)
        print("time difference to compress (mspk - flat): ", mst1 - ft1, "time difference to read: ", mst2 - ft2)
        print("--------------------------")
    print("==========================")



# t = time.time()
# result = pd.read_msgpack('msp.msg')
# print(time.time() - t)

# # print(b)
# print(sys.getsizeof(b))
# print(result)
# print(sys.getsizeof(result))