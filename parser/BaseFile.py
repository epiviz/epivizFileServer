"""
    Genomics file classes
"""

import struct
import zlib
import requests

class BaseFile(object):
    """
        File base class
    """
    HEADER_TOTAL_SIZE = 64
    HEADER_DICT = {
        "HEADER_MAGIC": 4,
        "HEADER_VERSION": 2,
        "HEADER_ZOOM_LEVEL": 2,
        "HEADER_CHROMOSOME_TREE_OFFSET": 8,
        "HEADER_FULL_DATA_OFFSET": 8,
        "HEADER_FULL_INDEX_OFFSET": 8,
        "HEADER_FIELD_COUNT": 2,
        "HEADER_DEFINED_FIELD_COUNT": 2,
        "HEADER_AUTO_SQL_OFFSET": 8,
        "HEADER_TOTAL_SUMMARY_OFFSET": 8,
        "HEADER_UNCOMPRESS_BUF_SIZE": 4,
        "HEADER_RESERVED": 8
    }

    HEADER_DICT_TYPES = {
        "HEADER_MAGIC": "int",
        "HEADER_VERSION": "int",
        "HEADER_ZOOM_LEVEL": "int",
        "HEADER_CHROMOSOME_TREE_OFFSET": "int",
        "HEADER_FULL_DATA_OFFSET": "int",
        "HEADER_FULL_INDEX_OFFSET": "int",
        "HEADER_FIELD_COUNT": "int",
        "HEADER_DEFINED_FIELD_COUNT": "int",
        "HEADER_AUTO_SQL_OFFSET": "int",
        "HEADER_TOTAL_SUMMARY_OFFSET": "int",
        "HEADER_UNCOMPRESS_BUF_SIZE": "int",
        "HEADER_RESERVED": "int"
    }

    SUMMARY_TOTAL_SIZE = 40
    TOTAL_SUMMARY_DICT = {
        "SUMMARY_BASES_COVERED": 8,
        "SUMMARY_MIN_VAL": 8,
        "SUMMARY_MAX_VAL": 8,
        "SUMMARY_SUM_DATA": 8,
        "SUMMARY_SUM_SQUARES": 8
    }

    TOTAL_SUMMARY_DICT_TYPES = {
        "SUMMARY_BASES_COVERED": "int",
        "SUMMARY_MIN_VAL": "float",
        "SUMMARY_MAX_VAL": "float",
        "SUMMARY_SUM_DATA": "float",
        "SUMMARY_SUM_SQUARES": "float"
    }

    HEADER_STRUCT = struct.Struct("<I2H3Q2H2QIQ")
    SUMMARY_STRUCT = struct.Struct("<Q4d")

    def __init__(self, file):
        self.file = file
        self.local = self.is_local(file)

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