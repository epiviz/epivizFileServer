import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '../python'))

import flatbuffers
import datas
import entry

builder = flatbuffers.Builder(0)

string = builder.CreateString("chr1")
entry.entryStart(builder)
entry.entryAddChr(builder, string)
entry.entryAddStart(builder, 1)
entry.entryAddEnd(builder, 20)
entry.entryAddValue(builder, 1.0)
ent1 = entry.entryEnd(builder)

string = builder.CreateString("chr2")
entry.entryStart(builder)
entry.entryAddChr(builder, string)
entry.entryAddStart(builder, 5)
entry.entryAddEnd(builder, 222)
entry.entryAddValue(builder, 1.9)
ent2 = entry.entryEnd(builder)
# builder.Finish(ent1)

# datas.datasAddValue(builder, ent1)

datas.datasStartValueVector(builder, 2)
# Note: Since we prepend the data, prepend the weapons in reverse order.
builder.PrependUOffsetTRelative(ent1)
builder.PrependUOffsetTRelative(ent2)
valvec = builder.EndVector(2)

datas.datasStart(builder)
datas.datasAddValue(builder, valvec)
d = datas.datasEnd(builder)
builder.Finish(d)

buf = builder.Output()
print(buf)
d = datas.datas.GetRootAsdatas(buf, 0)
l = d.ValueLength()
# ent1 = d.Value(0)
for x in range(0, l):
	e = d.Value(x)
	print(e.Chr())
	print(e.Start())
