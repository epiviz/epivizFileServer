file = "https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig"
from parser import *
import pysam
import msgpack
import sys
import pandas as pd
import time
import json
import random
from server.utils import format_result

params = {
    "datasource" : "39033",
    "metadata": None,
    "measurement": ["39033"]
}

f = BigWig(file)
for u in range(1,5):
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
        ms = msgpack.packb(formatted_result, use_bin_type=True)
        t1 = time.time() - t1
        t2 = time.time()
        temp = msgpack.unpackb(ms, raw=False)
        t2 = time.time() - t2
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