from aiocache import cached, SimpleMemoryCache
from aiocache.serializers import JsonSerializer
import pandas as pd
from .measurementClass import DbMeasurement, FileMeasurement, ComputedMeasurement
import ujson

class MeasurementManager(object):
    """
        Base class for measurements
    """

    def __init__(self):
        # self.measurements = pd.DataFrame()
        self.measurements = []

    def import_dbm(self, dbConn):
        query = "select * from measurements_index"
        with dbConn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()

        for rec in result:
            isGene = False
            if "genes" in rec["location"]:
                isGene = True

            annotation = None
            if rec["annotation"] is not None:
                annotation = ujson.loads(rec["annotation"])

            tempDbM = DbMeasurement("db", rec["column_name"], rec["measurement_name"],
                            rec["location"], rec["location"], dbConn=dbConn, 
                            annotation=annotation, metadata=rec["metadata"],
                            isGenes=isGene
                        )
            self.measurements.append(tempDbM)

    def import_files(self, fileSource, fileHandler=None):
        json_data = open(fileSource, "r")
        result = ujson.loads(json_data.read())

        for rec in result:
            isGene = False
            if "annotation" in rec["datatype"]:
                isGene = True

            tempFileM = FileMeasurement(rec.get("file_type"), rec.get("name"), rec.get("name"), 
                            rec.get("url"), annotation=rec.get("annotation"),
                            metadata=rec.get("metadata"), minValue=0, maxValue=5,
                            isGenes=isGene, fileHandler=fileHandler
                        )
            self.measurements.append(tempFileM)

    def add_computed_measurement(self, mtype, mid, name, measurements, computeFunc):
        tempComputeM = ComputedMeasurement(mtype, mid, name, measurements=name, computeFunc=computeFunc)
        self.measurements.append(tempComputeM)

    def get_measurements(self):
        return self.measurements

