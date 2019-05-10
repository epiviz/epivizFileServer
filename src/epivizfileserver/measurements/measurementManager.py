from aiocache import cached, SimpleMemoryCache
from aiocache.serializers import JsonSerializer
import pandas as pd
from .measurementClass import DbMeasurement, FileMeasurement, ComputedMeasurement
import ujson

class MeasurementManager(object):
    """
    Measurement manager class

    Attributes:
        measurements: list of all measurements managed by the system
    """

    def __init__(self):
        # self.measurements = pd.DataFrame()
        self.measurements = []

    def import_dbm(self, dbConn):
        """Import measurements from a database.The database 
        needs to have a `measurements_index` table with 
        information of files imported into the database.

        Args: 
            dbConn: a database connection
        """ 
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
                            annotation=annotation, metadata=ujson.loads(rec["metadata"]),
                            isGenes=isGene
                        )
            self.measurements.append(tempDbM)

    def import_files(self, fileSource, fileHandler=None):
        """Import measurements from a file. 


        Args: 
            fileSource: location of the configuration file to load
            fileHandler: an optional filehandler to use
        """ 
        json_data = open(fileSource, "r")
        result = ujson.loads(json_data.read())
        measurements = []

        for rec in result:
            isGene = False
            if "annotation" in rec["datatype"]:
                isGene = True

            tempFileM = FileMeasurement(rec.get("file_type"), rec.get("id"), rec.get("name"), 
                            rec.get("url"), annotation=rec.get("annotation"),
                            metadata=rec.get("metadata"), minValue=0, maxValue=5,
                            isGenes=isGene, fileHandler=fileHandler
                        )
            measurements.append(tempFileM)
            self.measurements.append(tempFileM)
        
        return(measurements)

    def import_ahub(self, ahub, handler=None):
        """Import measurements from annotationHub objects. 

        Args: 
            ahub: list of file records from annotationHub
            handler: an optional filehandler to use
        """
        measurements = []
        for i, row in ahub.iterrows():
            if "EpigenomeRoadMapPreparer" in row["preparerclass"]:
                tempFile = FileMeasurement(row["source_type"], row["ah_id"], row["title"],
                                row["sourceurl"])
                self.measurements.append(tempFile)
                measurements.append(tempFile)
        return measurements

    def add_computed_measurement(self, mtype, mid, name, measurements, computeFunc):
        """Add a Computed Measurement

        Args: 
            mtype: measurement type, defaults to 'computed'
            mid: measurement id
            name: name for this measurement
            measurements: list of measurement to use 
            computeFunc: `NumPy` function to apply

        Returns:
            a `ComputedMeasurement` object
        """
        
        tempComputeM = ComputedMeasurement(mtype, mid, name, measurements=measurements, computeFunc=computeFunc)
        self.measurements.append(tempComputeM)
        return tempComputeM

    def get_measurements(self):
        """Get all available measurements
        """
        return self.measurements

