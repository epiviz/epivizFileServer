file = "https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig"
from parser import *
import pysam
import umsgpack
import sys
import pandas as pd
import random
import time
# here is magic
from py_mini_racer import py_mini_racer
ctx = py_mini_racer.MiniRacer()

f = BigWig(file)

f = BigWig(file)
for u in range(1,5):
    for x in range(1,5):
# for u in range(1,2):
#     for x in range(1,2):
        s = random.randint(1, 500)
        r = 10**(u+3) + s
        print("testing for range ", s, r)
        result, _ = f.getRange('chr1', s, r)
        print("original DF size")
        print(sys.getsizeof(result))
        t1 = time.time()
        ms = result.to_msgpack()
        disk = str(10**(u+3)+x) + ".msg.testfile"
        t1 = time.time() - t1
        t2 = time.time()
        temp = pd.read_msgpack(ms)
        with open(disk, 'wb') as wr:
            wr.write(bytearray(ms))
            wr.close()
        t2 = time.time() - t2
        print("time to compress to msgpack: ", t1, "read from msgpack: ", t2)
        print("msgpack size: ", sys.getsizeof(ms))
        mst1 = t1
        mst2 = t2
        t1 = time.time()
        js = result.to_json()
        t1 = time.time() - t1
        t2 = time.time()
        disk = str(10**(u+3)+x) + ".json.testfile"
        temp = pd.read_json(js)
        with open(disk, 'w') as wr:
            wr.write(js)
            wr.close()
        t2 = time.time() - t2
        print("time to compress to json: ", t1, "read from json: ", t2)
        print("msgpack size: ", sys.getsizeof(js))
        print(" ")
        print("time difference to compress: ", mst1 - t1, "time difference to read: ", mst2 - t2)
        print("size difference: ", sys.getsizeof(ms) - sys.getsizeof(js))
        print("--------------------------")
    print("==========================")

