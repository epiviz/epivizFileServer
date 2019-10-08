"""
    Genomics file classes
"""

import struct
import zlib
import requests
import ujson
import pandas as pd
import numpy as np

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

    def bin_rows(self, data, chr, start, end, columns=None, metadata=None, length = 300):
        """Bin genome by bin length and summarize the bin
        """
        freq = round((end-start)/length)
        if end - start < length:
            freq = 1

        data = data.set_index(['start', 'end'])
        data.index = pd.IntervalIndex.from_tuples(data.index)

        bins = pd.interval_range(start=start, end=end, freq=freq)
        bins_df = pd.DataFrame(index=bins)
        bins_df["chr"] = chr

        if metadata:
            for meta in metadata:
                bins_df[meta] = data[meta]

        for col in columns:
            bins_df[col] = None

        # map data to bins
        for index, row in bins_df.iterrows():
            temps = data[(data.index.left <= index.right) & (data.index.right > index.left)]
            for col in columns:
                row[col] = np.mean(temps[col].values)

        bins_df["start"] = bins_df.index.left
        bins_df["end"] = bins_df.index.right
        return bins_df, None
