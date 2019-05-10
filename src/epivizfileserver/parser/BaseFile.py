"""
    Genomics file classes
"""

import struct
import zlib
import requests
import ujson

class BaseFile(object):
    """
    Base file class for parser module

    This class provides various useful functions

    Args:
        file: file location
    
    Attributes:
        local: if file is local or hosted on a public server
        endian: check for endianess

    """

    HEADER_STRUCT = struct.Struct("<I2H3Q2H2QIQ")
    SUMMARY_STRUCT = struct.Struct("<Q4d")

    def __init__(self, file):
        self.file = file
        self.local = self.is_local(file)
        self.endian = "="
        self.compressed = True

    def is_local(self, file):
        """Checks if file is local or hosted publicly

        Args:
            file: location of file
        """
        if "http://" in file or "https://" in file or "ftp://" in file:
            return False
        return True

    def parse_header(self):
        raise Exception("NotImplementedException")

    def get_data(self, chr, start, end):
        raise Exception("NotImplementedException")

    def decompress_binary(self, bin_block):
        """decompress a binary string

        Args:
            bin_block: binary string

        Returns:
            a zlib decompressed binary string
        """
        return zlib.decompress(bin_block)

    def formatAsJSON(self, data):
        """Encode a data object as JSON

        Args:
            data: any data object to encode

        Returns: 
            data encoded as JSON
        """
        return ujson.dumps(data)

    def get_bytes(self, offset, size):
        """Get bytes within a given range

        Args:
            offset (int): byte start position in file
            size (int): size of bytes to access from offset

        Returns:
            binary string from offset to (offset + size)
        """
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