"""
    Genomics file classes
"""

import struct
import zlib
import requests
import ujson


class BaseFile(object):
    """
        File base class
    """

    HEADER_STRUCT = struct.Struct("<I2H3Q2H2QIQ")
    SUMMARY_STRUCT = struct.Struct("<Q4d")

    def __init__(self, file):
        self.file = file
        self.local = self.is_local(file)
        self.endian = "="
        self.compressed = True

    def is_local(self, file):
        if "http" in file or "ftp" in file:
            return False
        return True

    def parse_header(self):
        raise Exception("NotImplementedException")

    def get_data(self, chr, start, end):
        raise Exception("NotImplementedException")

    def remote_calc_byte_range(self, chr, start, end):
        raise Exception("NotImplementedException")

    def remote_get_byte_range(self, chr, start, end):
        raise Exception("NotImplementedException")

    def decompress_binary(self, bin_block):
        return zlib.decompress(bin_block)

    def formatAsJSON(self, data):
        return ujson.dumps(data)

    def get_bytes(self, offset, size):
        f = open(self.file, "rb")
        f.seek(offset)
        bin_value = f.read(size)
        f.close()
        return bin_value

    def read_bytes(self, start_index, byte_size):
        with open(self.file, "rb") as bin_file:
            bin_file.seek(start_index)
            bin_value = bin_file.read(byte_size)
            return bin_value

    def read_parse_bytes(self, start_index, header_keys):
        parser = {}
        with open(self.file, "rb") as bin_file:
            count = start_index
            for key, value in header_keys.items():
                bin_file.seek(count)
                bin_value = bin_file.read(value)
                count = count + value
                parser[key] = bin_value
        return parser