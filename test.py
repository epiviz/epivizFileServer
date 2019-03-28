file = "https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig"
from parser import *
import pysam
import umsgpack
import sys
import pandas as pd
import time
# f = pysam.AlignmentFile(file, 'rb')

# iter = f.pileup('CHROMOSOME_IV', 1, 1000)
# # iter = f.fetch('c1', 0, 10000000)
# # iter = f.pileup()
# result = []
# chrTemp = startTemp = endTemp = valueTemp = None
# for x in iter:
#     if valueTemp is None:
#         chrTemp = x.reference_name
#         startTemp = x.reference_pos
#         valueTemp = x.get_num_aligned()
#     elif valueTemp is not x.get_num_aligned():
#         result.append((chrTemp, startTemp, endTemp, valueTemp))
#         chrTemp = x.reference_name
#         startTemp = x.reference_pos
#         valueTemp = x.get_num_aligned()

#     endTemp = x.reference_pos+1
#     # result.append((x.get_num_aligned(), x.reference_name, x.reference_pos))
#     # print(x)
f = BigWig(file)
for x in range(1,5):
    r = 10**(x+3)
    print("testing for range ", r)
    result, _ = f.getRange('chr1', 1, r)
    print("original DF size")
    print(sys.getsizeof(result))
    t1 = time.time()
    ms = result.to_msgpack()
    t1 = time.time() - t1
    t2 = time.time()
    temp = pd.read_msgpack(ms)
    t2 = time.time() - t2
    print("time to compress to msgpack: ", t1, "read from msgpack: ", t2)
    print("msgpack size: ", sys.getsizeof(ms))
    mst1 = t1
    mst2 = t2
    t1 = time.time()
    js = result.to_json()
    t1 = time.time() - t1
    t2 = time.time()
    temp = pd.read_json(js)
    t2 = time.time() - t2
    print("time to compress to json: ", t1, "read from json: ", t2)
    print("msgpack size: ", sys.getsizeof(js))
    print(" ")
    print("time difference to compress: ", mst1 - t1, "time difference to read: ", mst2 - t2)
    print("size difference: ", sys.getsizeof(ms) - sys.getsizeof(js))
    print("--------------------------")




# t = time.time()
# result = pd.read_msgpack('msp.msg')
# print(time.time() - t)

# # print(b)
# print(sys.getsizeof(b))
# print(result)
# print(sys.getsizeof(result))