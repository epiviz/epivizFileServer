# automatically generated by the FlatBuffers compiler, do not modify

# namespace: Woekspace

import flatbuffers

class entry(object):
    __slots__ = ['_tab']

    @classmethod
    def GetRootAsentry(cls, buf, offset):
        n = flatbuffers.encode.Get(flatbuffers.packer.uoffset, buf, offset)
        x = entry()
        x.Init(buf, n + offset)
        return x

    # entry
    def Init(self, buf, pos):
        self._tab = flatbuffers.table.Table(buf, pos)

    # entry
    def Chr(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(4))
        if o != 0:
            return self._tab.String(o + self._tab.Pos)
        return None

    # entry
    def Start(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(6))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 0

    # entry
    def End(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(8))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Int32Flags, o + self._tab.Pos)
        return 0

    # entry
    def Value(self):
        o = flatbuffers.number_types.UOffsetTFlags.py_type(self._tab.Offset(10))
        if o != 0:
            return self._tab.Get(flatbuffers.number_types.Float64Flags, o + self._tab.Pos)
        return 0.0

def entryStart(builder): builder.StartObject(4)
def entryAddChr(builder, chr): builder.PrependUOffsetTRelativeSlot(0, flatbuffers.number_types.UOffsetTFlags.py_type(chr), 0)
def entryAddStart(builder, start): builder.PrependInt32Slot(1, start, 0)
def entryAddEnd(builder, end): builder.PrependInt32Slot(2, end, 0)
def entryAddValue(builder, value): builder.PrependFloat64Slot(3, value, 0.0)
def entryEnd(builder): return builder.EndObject()
