import pandas as pd
from handler import FileHandlerProcess
import parser

class Measurement(object):
    """
        Base class for measurement
    """
    def __init__(self, mtype, mid, name, source, datasource, annotation=None, metadata=None, isComputed=False, isGenes=False, minValue=None, maxValue=None):
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

    def get_data(self, chr, start, end):
        """
            Get Data for this measurement
        """
        raise Exception("NotImplementedException")

    def get_measurement_name(self):
        return self.name

    def get_measurement_id(self):
        return self.mid
    
    def get_measurement_type(self):
        return self.mtype

    def get_measurement_source(self):
        return self.source

    def get_measurement_annotation(self):
        return self.annotation
    
    def get_measurement_metadata(self):
        return self.metadata

    def get_measurement_min(self):
        return self.minValue

    def get_measurement_max(self):
        return self.maxValue

    def is_file(self):
        if self.mtype is "db":
            return False
        return True

    def is_computed(self):
        return self.isComputed

    def is_gene(self):
        return self.isGenes

    def get_columns(self):
        columns = []
        if self.metadata is not None:
            columns = self.metadata
        columns.append(self.mid)
        return columns

    def bin_rows(self, data, chr, start, end, length = 2000):
        """
            bin result rows
        """
        freq = round((end-start)/length)
        if end - start < length:
            freq = 1
            
        data = data.set_index(['start', 'end'])
        data.index = pd.IntervalIndex.from_tuples(data.index)

        bins = pd.interval_range(start=start, end=end, freq=freq)
        # print(bins)
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
        """
            query from db/source
        """
        raise Exception("NotImplementedException")

class DbMeasurement(Measurement):
    """
        Class for database measurement
    """
    def __init__(self, mtype, mid, name, source, datasource, dbConn, annotation=None, metadata=None, isComputed=False, isGenes=False, minValue=None, maxValue=None):
        super(DbMeasurement, self).__init__(mtype, mid, name, source, datasource, annotation, metadata, isComputed, isGenes, minValue, maxValue)
        self.query_range = "select distinct %s from %s where chr=%s and end >= %s and start < %s order by chr, start"
        self.query_all = "select distinct %s from %s order by chr, start"
        self.connection = dbConn

    def query(self, obj, params):
        query = obj % params
        df = pd.read_sql(query, con=self.connection)
        return df

    async def get_data(self, chr, start, end, bin=False):
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
    """

    def __init__(self, mtype, mid, name, source, datasource="files", annotation=None, metadata=None, isComputed=False, isGenes=False, minValue=None, maxValue=None,fileHandler=None):
        super(FileMeasurement, self).__init__(mtype, mid, name, source, datasource, annotation, metadata, isComputed, isGenes, minValue, maxValue)
        self.fileHandler = fileHandler
        self.columns = None
        # ["chr", "start", "end"].append(mid)

    def create_parser_object(self, type, name, columns=None):
        from parser.utils import create_parser_object as cpo
        return cpo(type, name, columns)

    async def get_data(self, chr, start, end, bin=False):
        try:
            if self.fileHandler is None:
                file = self.create_parser_object(self.mtype, self.source, self.columns)
                result, _ = file.getRange(chr, start, end)
            else:
                result, _ = await self.fileHandler.handleFile(self.source, self.mtype, chr, start, end)
            # result = pd.DataFrame(result, columns = ["chr", "start", "end", self.mid])   

            # rename columns from score to mid for BigWigs
            if self.mtype in ["BigWig", "bigwig", "bw"]:
                result = result.rename(columns={'score': self.mid})

            if bin: 
                result = self.bin_rows(result, chr, start, end)
            return result, None
        except Exception as e:
            return {}, str(e)

class ComputedMeasurement(Measurement):
    """
        Class for file based measurement
    """
    def __init__(self, mtype, mid, name, measurements, source=None, computeFunc=None, datasource="computed", annotation=None, metadata=None, isComputed=True, isGenes=False):
        super(ComputedMeasurement, self).__init__(mtype, mid, name, source, datasource, annotation, metadata, isComputed, isGenes)
        self.measurements = measurements
        self.computeFunc = computeFunc

    def get_columns(self):
        columns = []
        for m in self.measurements:
            columns.append(m.mid)
        return columns
    
    def computeWrapper(self, computeFunc, columns):
        def computeApply(row):
            rowVals = []
            for k in row.keys():
                if k in columns:
                    rowVals.append(row[k])
            if None in rowVals:
                return None
            return computeFunc(rowVals)
        return computeApply

    async def get_data(self, chr, start, end, dropna=True):

        result = []
        for measurement in self.measurements:
            mea_result, _ = await measurement.get_data(chr, start, end, bin=True)
            # result = [result, mea_result]
            result.append(mea_result)

        result = pd.concat(result, axis=1)
        result = result.loc[:,~result.columns.duplicated()]

        if dropna:
            result = result.dropna()

        try:
            if self.computeFunc:
                columns = self.get_columns()
                result[self.mid] = result.apply(self.computeWrapper(self.computeFunc, columns), axis=1)
            return result, None
        except Exception as e:
            return {}, str(e)