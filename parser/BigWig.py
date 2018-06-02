from .BaseFile import BaseFile

class BigWig(BaseFile):
    """
        File BigWig class
    """

    def __init__(self, file):
        super(BigWig, self).__init__(file)

    def parse_header(self):
        # read fixed header
        header_bin = self.read_bytes(0, self.HEADER_TOTAL_SIZE)
        header_values = self.HEADER_STRUCT.unpack(header_bin)
        # print(header_values)

        # read summary header
        # print(header_values[9])
        summary_bin = self.read_bytes(
            header_values[9], self.SUMMARY_TOTAL_SIZE)
        summary_values = self.SUMMARY_STRUCT.unpack(summary_bin)
        # print(summary_values)

        # read data header
        # print(header_values[4])
        # summary_bin = self.read_bytes(
        #     header_values[4], 24)
        # parser = {}
        # data_keys = {
        #     "datacount": 4,
        #     "dataheader": 24
        # }
        # with open(self.file, "rb") as bin_file:
        #     count = header_values[4]
        #     bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize("<I"))
        #     print("datacount")
        #     print(struct.unpack("<I", bin_value))

        #     block_data_value = bin_file.read(24)
        #     print(header_values[10])
        #     block_data = zlib.decompress(
        #         block_data_value, bufsize=header_values[10])
        #     print(struct.unpack("<5I2BH", block_data))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize("<I"))
        #     print("chromId")
        #     print(struct.unpack("<I", bin_value))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize("<I"))
        #     print("chromStart")
        #     print(struct.unpack("<I", bin_value))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize("<I"))
        #     print("chromEnd")
        #     print(struct.unpack("<I", bin_value))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize("<I"))
        #     print("itemStep")
        #     print(struct.unpack("<I", bin_value))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize("<I"))
        #     print(bin_value)
        #     print("itemSpan")
        #     print(struct.unpack("<I", bin_value))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize(">B"))
        #     print("type")
        #     print(bin_value)
        #     print(struct.unpack(">B", bin_value))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize(">B"))
        #     print("reserved")
        #     print(bin_value)
        #     print(struct.unpack(">B", bin_value))

        #     # bin_file.seek(header_values[4])
        #     bin_value = bin_file.read(struct.calcsize("<H"))
        #     print("itemCount")
        #     print(bin_value)
        #     print(struct.unpack("<H", bin_value))

        # bin_file.seek(header_values[4] + 4)
        # bin_value = bin_file.read(24)
        # print("dataheader")
        # print(struct.calcsize("<5I2BH"))
        # print(struct.unpack("<5I2BH", bin_value))

        # bin_file.seek(header_values[3])
        # bin_value = bin_file.read(32)
        # print("chromheader")
        # print(struct.calcsize("<4I2Q"))
        # print(struct.unpack("<4I2Q", bin_value))

        # bin_file.seek(header_values[5])
        # bin_value = bin_file.read(48)
        # print("r tree offset")
        # print(struct.unpack("<2IQ4IQ2I", bin_value))

        # for key, value in data_keys.items():
        #     bin_file.seek(count)
        #     bin_value = bin_file.read(value)
        #     print(key)
        #     print(bin_value)
        #     print(struct.unpack("=IIIIIBBH", bin_value))
        #     count = count + key

        # return parser
