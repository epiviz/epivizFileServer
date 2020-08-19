from ..handler import FileHandlerProcess
import parser

import pandas as pd
import requests
import numpy as np
from random import randrange
# import umsgpack
from aiocache import cached, Cache
from aiocache.serializers import JsonSerializer, PickleSerializer
# import logging

# logger = logging.getLogger(__name__)
from sanic.log import logger as logging


class Measurement(object):
    """
    Base class for managing measurements from files

    Args: 
        mtype: Measurement type, either 'file' or 'db'
        mid: unique id to use for this measurement
        name: name of the measurement
        source: location of the measurement, if mtype is 'db' use table name, if file, file location
        datasource: is the database name if mtype is 'db' use database name, else 'files'
        annotation: annotation for this measurement, defaults to None
        metadata: metadata for this measurement, defaults to None
        isComputed: True if this measurement is Computed from other measurements, defaults to False
        isGenes: True if this measurement is an annotation (for example: reference genome hg19), defaults to False
        minValue: min value of all values, defaults to None
        maxValue: max value of all values, defaults to None
        columns: column names for the file
    """
    def __init__(self, mtype, mid, name, source, datasource, genome=None, annotation=None, metadata=None, isComputed=False, isGenes=False, minValue=None, maxValue=None, columns=None):
        self.mtype = mtype      # measurement_type (file/db)
        self.mid = mid          # measurement_id (column name in db/file)
        self.name = name        # measurement_name
        self.source = source    # tbl name / file location
        self.datasource = datasource # dbname / "files"
        self.annotation = annotation
        self.metadata = metadata
        self.isComputed = isComputed
        self.isGenes = isGenes
        self.minValue = minValue
        self.maxValue = maxValue
        self.columns = columns
        self.genome = genome

        if self.annotation is None: 
            self.annotation = {}

        self.annotation["genome"] = genome

    def get_data(self, chr, start, end):
        """
        Get Data for this measurement

        Args:
            chr: chromosome
            start: genomic start
            end: genomic end
        """
        raise Exception("NotImplementedException")

    def get_status(self):
        """
        Get status of this measurement (most pertinent for files)
        """
        raise Exception("NotImplementedException")

    def get_measurement_name(self):
        """Get measurement name
        """
        return self.name

    def get_measurement_id(self):
        """Get measurement id
        """
        return self.mid
    
    def get_measurement_type(self):
        """Get measurement type
        """
        return self.mtype

    def get_measurement_source(self):
        """Get source
        """
        return self.source

    def get_measurement_annotation(self):
        """Get measurement annotation
        """
        return self.annotation

    def get_measurement_genome(self):
        """Get measurement genome
        """
        return self.genome
    
    def get_measurement_metadata(self):
        """Get measurement metadata
        """
        return self.metadata

    def get_measurement_min(self):
        """Get measurement min value
        """
        return self.minValue

    def get_measurement_max(self):
        """Get measurement max value
        """
        return self.maxValue

    def is_file(self):
        """Is measurement a file ?
        """
        if self.mtype is "db":
            return False
        return True

    def is_computed(self):
        """Is measurement computed ?
        """
        return self.isComputed

    def is_gene(self):
        """is the file a genome annotation ?
        """
        return self.isGenes

    def get_columns(self):
        """get columns from file
        """
        columns = []
        if self.metadata is not None:
            columns = self.metadata
        columns.append(self.mid)
        return columns

    def bin_rows(self, data, chr, start, end, bins = 2000):
        """Bin genome by bin length and summarize the bin

        Args:
            data: DataFrame from the file
            chr: chromosome
            start: genomic start
            end: genomic end
            length: max rows to summarize the data frame into

        Returns:
            a binned data frame whose max rows is length
        """

        logging.debug("Measurement: %s\t%s\t%s" %(self.mid, self.name, "bin_rows"))
        freq = round((end-start)/length)
        if end - start < length:
            freq = 1
            
        data = data.set_index(['start', 'end'])
        data.index = pd.IntervalIndex.from_tuples(data.index)

        bins = pd.interval_range(start=start, end=end, freq=freq)
        bins_df = pd.DataFrame(index=bins)
        bins_df["chr"] = chr
        if self.metadata:
            for meta in self.metadata:
                bins_df[meta] = data[meta]

        for col in self.get_columns():
            bins_df[col] = None

        # map data to bins
        for index, row in data.iterrows():
            for col in self.get_columns():
                bins_df.loc[index, col] = row[col]

        bins_df["start"] = bins_df.index.left
        bins_df["end"] = bins_df.index.right

        return pd.DataFrame(bins_df)

    def query(self, obj, query_params):
        """Query from db/source

        Args: 
            obj: db obj
            query_params: query parameters to search
        """
        raise Exception("NotImplementedException")

class DbMeasurement(Measurement):
    """
    Class representing a database measurement

    In addition to params from the base measurement class - 

    Args:
        dbConn: a database connection object

    Attributes:
        connection: a database connection object
    """
    def __init__(self, mtype, mid, name, source, datasource, dbConn, genome=None, annotation=None, metadata=None, isComputed=False, isGenes=False, minValue=None, maxValue=None, columns=None):
        super(DbMeasurement, self).__init__(mtype, mid, name, source, datasource, genome, annotation, metadata, isComputed, isGenes, minValue, maxValue, columns)
        self.query_range = "select distinct %s from %s where chr=%s and end >= %s and start < %s order by chr, start"
        self.query_all = "select distinct %s from %s order by chr, start"
        self.connection = dbConn

    def query(self, obj, params):
        """Query from db/source

        Args: 
            obj: the query string
            query_params: query parameters to search

        Returns:
            a dataframe of results from the database
        """
        query = obj % params
        df = pd.read_sql(query, con=self.connection)
        return df

    async def get_data(self, chr, start, end, bin=False):
        """Get data for a genomic region from database

        Args: 
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            bin (bool): True to bin the results, defaults to False

        Returns:
            a dataframe with results
        """
        query = None
        query_params = []
        query_ms = "id, chr, start, end, " + self.mid + " "

        if self.metadata is not None:
            metadata = ", ".join(self.metadata)
            query_ms = query_ms + ", " + metadata

        if self.isGenes:
            query_params = (
                str(query_ms) + ", strand",
                str(self.source),
                '"' + str(chr) + '"',
                int(start),
                int(end))

            query = self.query_range
        else:
            if chr is None:
                query_params = (
                    str(query_ms),
                    str(self.source))

                query = self.query_all
            else:
                query_params = (
                    str(query_ms),
                    str(self.source),
                    '"' + str(chr) + '"',
                    int(start),
                    int(end))

                query = self.query_range
        try:
            result = self.query(query, query_params)

            if bin:
                result = self.bin_rows(result, chr, start, end)

            return result, None
        except Exception as e:
            return {}, str(e)

class FileMeasurement(Measurement):
    """
    Class for file based measurement

    In addition to params from the base `Measurement` class

    Args:
        fileHandler: an optional file handler object to process query requests (uses dask)
    """

    def __init__(self, mtype, mid, name, source, datasource="files", genome=None, annotation=None, metadata=None, isComputed=False, isGenes=False, minValue=None, maxValue=None,fileHandler=None, columns=None):
        super(FileMeasurement, self).__init__(mtype, mid, name, source, datasource, genome, annotation, metadata, isComputed, isGenes, minValue, maxValue, columns)
        self.fileHandler = fileHandler
        self.columns = columns
        # ["chr", "start", "end"].append(mid)

    def create_parser_object(self, type, name, columns=None):
        """Create appropriate File class based on file format

        Args:
            type (str): format of file
            name (str): location of file
            columns ([str]): list of columns from file

        Returns:
            An file object
        """ 
        from ..parser.utils import create_parser_object as cpo
        return cpo(type, name, columns)

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="filesearchgene")
    async def search_gene(self, query, maxResults):
        """Get data for a genomic region from file

        Args: 
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end

        Returns:
            a array of matched genes
        """ 
        result = None
        err = None
        logging.debug("File Measurement: %s\t%s\t%s" %(self.mid, self.name, "file_gene_search"))

        try:
            if self.fileHandler is None:
                file = self.create_parser_object(self.mtype, self.source, self.columns)
                result, err = file.search_gene(query, maxResults)
            else:
                result, err = await self.fileHandler.handleSearch(self.source, self.mtype, query, maxResults)
         
            return result, str(err)
        except Exception as e:
            logging.error("File Measurement: %s\t%s\t%s" %(self.mid, self.name, "file_gene_search"), exc_info=True)
            return {}, str(e)

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="filegetdata")
    async def get_data(self, chr, start, end, bins, bin=True):
        """Get data for a genomic region from file

        Args: 
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            bin (bool): True to bin the results, defaults to False

        Returns:
            a dataframe with results
        """ 
        result = None
        err = None
        
        logging.debug("File Measurement: %s\t%s\t%s" %(self.mid, self.name, "file_get_data"))

        try:
            if self.fileHandler is None:
                file = self.create_parser_object(self.mtype, self.source, self.columns)
                result, err = file.getRange(chr, start, end, bins=bins)
            else:
                result, err = await self.fileHandler.handleFile(self.source, self.mtype, chr, start, end, bins=bins)

            # rename columns from score to mid for BigWigs
            if self.mtype in ["BigWig", "bigwig", "bw", "bigWig"]:
                result = result.rename(columns={'score': self.mid})
            elif self.mtype in ['Tabix', 'tabix', 'tbx'] and not self.isGenes:
                result.columns = ["chr", "start", "end"].extend(self.columns)
                cols = ["chr", "start", "end"]
                cols.append(self.mid)
                result = result[cols]   

            if bin and not self.isGenes: 
                result, err = await self.fileHandler.binFileData(self.source, self.mtype, result, chr, start, end, 
                                bins, columns=self.get_columns(), metadata=self.metadata)
            
            return result, str(err)
        except Exception as e:
            logging.error("File Measurement: %s\t%s\t%s" %(self.mid, self.name, "file_get_data"), exc_info=True)
            return {}, str(e)

    async def get_status(self):
        result = 0
        err = None
        logging.debug("File Measurement: %s\t%s\t%s" %(self.mid, self.name, "file_get_status"))

        file = self.create_parser_object(self.mtype, self.source, self.columns)
        result, err = file.get_status()
        return result, err

class ComputedMeasurement(Measurement):
    """
    Class for representing computed measurements

    In addition to params on base `Measurement` class -

    Args:
        computeFunc: a `NumPy` function to apply on our dataframe
        source: defaults to 'computed'
        datasource: defaults to 'computed'
    """
    def __init__(self, mtype, mid, name, measurements, source="computed", computeFunc=None, datasource="computed", genome=None, annotation={"group": "computed"}, metadata=None, isComputed=True, isGenes=False, fileHandler=None, columns=None, computeAxis=1):
        super(ComputedMeasurement, self).__init__(mtype, mid, name, source, datasource, genome, annotation, metadata, isComputed, isGenes, columns=columns)
        self.measurements = measurements
        self.computeFunc = computeFunc
        self.fileHandler = fileHandler
        self.computeAxis = computeAxis

    def get_columns(self):
        columns = []
        for m in self.measurements:
            columns.append(m.mid)
        return columns
    
    def computeWrapper(self, computeFunc, columns):
        """a wrapper for the 'computeFunc' function

        Args: 
            computeFunc: a `NumPy` compute function 
            columns: columns from file to apply
            
        Returns:
            a dataframe with results
        """ 
        def computeApply(row):
            rowVals = []
            for k in row.keys():
                if k in columns:
                    rowVals.append(row[k])
            if None in rowVals:
                return None
            return computeFunc(rowVals)
        return computeApply

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="computedgetdata")
    async def get_data(self, chr, start, end, bins, dropna=True):
        """Get data for a genomic region from files and apply the `computeFunc` function 

        Args: 
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            dropna (bool): True to dropna from a measurement since any computation is going to fail on this row

        Returns:
            a dataframe with results
        """ 
        
        logging.error("Computed Measurement: %s\t%s\t%s" %(self.mid, self.name, "file_get_data"))
        
        result = []
        err = None
        tbin = True
        if len(self.measurements) == 1:
            tbin = False

        futures = []
        for measurement in self.measurements:
            future = measurement.get_data(chr, start, end, bins, bin=tbin)
            futures.append(future)
        
        for future in futures:
            mea_result, err = await future
            result.append(mea_result)

        result = pd.concat(result, axis=1)
        result = result.loc[:,~result.columns.duplicated()]

        if dropna:
            result = result.dropna()

        try:
            if self.computeFunc:
                columns = self.get_columns()
                result_copy = result[columns]
                for c in columns:
                    result_copy[c] = result[c].apply(float)
                result[self.mid] = result_copy.apply(self.computeFunc, self.computeAxis)
                result[self.mid] = result[self.mid].apply(float)
                # result[self.mid].astype('int64')
                # result[self.mid] = result.apply(self.computeWrapper(self.computeFunc, columns), axis=1)
            return result, str(err)
        except Exception as e:
            logging.error("Computed Measurement: %s\t%s\t%s" %(self.mid, self.name, "file_get_data"), exc_info=True)
            return {}, str(e)

class WebServerMeasurement(Measurement):
    """
    Class representing a web server measurement

    In addition to params from the base measurement class, source is now server API endpoint
    """
    def __init__(self, mtype, mid, name, source, datasource, datasourceGroup, annotation=None, metadata=None, isComputed=False, isGenes=False, minValue=None, maxValue=None):
        super(WebServerMeasurement, self).__init__(mtype, mid, name, source, datasource, annotation, metadata, isComputed, isGenes, minValue, maxValue)
        self.version = 5
        self.datasourceGroup = datasourceGroup

    def get_data(self, chr, start, end, bin=False, requestId=randrange(1000)):
        """Get data for a genomic region from the API

        Args: 
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            bin (bool): True to bin the results, defaults to False

        Returns:
            a dataframe with results
        """

        params = {
            'requestId': requestId,
            'version': self.version,
            'action': 'getData',
            'datasourceGroup': self.datasourceGroup,
            'datasource': self.datasource,
            'measurement': self.mid,
            'seqName': chr,
            'start': start,
            'end': end
        }

        try:
            if self.annotation["datatype"] == "peak":
                params["action"] = "getRows"
                del params["measurement"]
                params["datasource"] = self.mid

            result = requests.get(self.source, params=params)
            # res = umsgpack.unpackb(result.content)
            res = result.json()

            data = res['data']
            dataF = None

            if self.annotation["datatype"] == "peak":
                start = np.cumsum(data['values']['start'])
                start = start.astype(int)
                end = np.cumsum(data['values']['end'])
                end = end.astype(int)
                chr = data['values']['chr']
                dataF = pd.DataFrame(list(zip(chr, start, end)), columns = ['chr', 'start', "end"]) 
            else:
                if data['rows']['useOffset']:
                    data['rows']['values']['start'] = np.cumsum(data['rows']['values']['start'])
                    data['rows']['values']['end'] = np.cumsum(data['rows']['values']['end'])

                # convert json to dataframe
                records = {}

                for key in data['rows']['values'].keys():
                    if key not in ["id", "strand", "metadata"]:
                        records[key] = data['rows']['values'][key]

                for key in data['rows']['values']['metadata'].keys():
                    records[key] = data['rows']['values']['metadata'][key]

                for key in data['values']['values'].keys():
                    records[key] = data['values']['values'][key]

                dataF = pd.DataFrame(records)
            return dataF, None
        except Exception as e:
            return {}, str(e)
