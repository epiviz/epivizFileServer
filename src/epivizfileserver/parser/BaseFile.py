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
        if "http://" in file or "https://" in file or "ftp://" in file:
            return False
        return True

    def parse_header(self):
        raise Exception("NotImplementedException")

    def get_data(self, chr, start, end):
        raise Exception("NotImplementedException")

    def decompress_binary(self, bin_block):
        return zlib.decompress(bin_block)

    def formatAsJSON(self, data):
        return ujson.dumps(data)

    def get_bytes(self, offset, size):
        if self.local:
            f = open(self.file, "rb")
            f.seek(offset)
            bin_value = f.read(size)
            f.close()
            return bin_value
        else:
            headers = {"Range": "bytes=%d-%d" % (offset, offset+size) }
            resp = requests.get(self.file, headers=headers)
            # resp = session.get(self.file, headers=headers).result()
            # use requests.codes.ok instead
            if resp.status_code != 206:
                raise Exception("URLError")

            return resp.content[:size]