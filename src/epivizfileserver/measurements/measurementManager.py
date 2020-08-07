from aiocache import cached, SimpleMemoryCache
from aiocache.serializers import JsonSerializer
import pandas as pd
from .measurementClass import DbMeasurement, FileMeasurement, ComputedMeasurement
from ..trackhub import TrackHub
from ..parser import GtfParsedFile, TbxFile
import ujson
import requests

class MeasurementManager(object):
    """
    Measurement manager class

    Attributes:
        measurements: list of all measurements managed by the system
    """

    def __init__(self):
        # self.measurements = pd.DataFrame()
        self.genomes = {}
        self.measurements = []
        self.emd_endpoint = None
        self.stats = {
            "getRows": {},
            "getValues": {},
            "search": {}
        }

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

    def import_files(self, fileSource, fileHandler=None, genome=None):
        """Import measurements from a file. 


        Args: 
            fileSource: location of the configuration file to load
            fileHandler: an optional filehandler to use
        """
        with open(fileSource, 'r') as f:
            json_string = f.read()

        records = ujson.loads(json_string)
        self.import_records(records, fileHandler=fileHandler, genome=genome)

    def import_records(self, records, fileHandler=None, genome=None):
        """Import measurements from a list of records (usually from a decoded json string)


        Args: 
            fileSource: location of the configuration json file to load
            fileHandler: an optional filehandler to use
        """ 
        measurements = []

        for rec in records:
            isGene = False
            if "annotation" in rec["datatype"]:
                isGene = True

            if rec.get("genome") is None and genome is None: 
                raise Exception("all files must be annotated with its genome build")

            tgenome = rec.get("genome")
            if tgenome is None:
                tgenome = genome

            tempFileM = FileMeasurement(rec.get("file_type"), rec.get("id"), rec.get("name"), 
                            rec.get("url"), genome=tgenome, annotation=rec.get("annotation"),
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

    def get_from_emd(self, url=None):
        """Make a GET request to a metadata api

        Args:
            url: the url of the epiviz-md api. If none the url on self.emd_endpoint is used if available (None) 

        """
        if url is None:
            url = self.emd_endpoint

        if url is None:
            raise Exception("Error reading measurements from emd endpoint: missing url")


        r = requests.get(url + "/ms/")
        if r.status_code != 200:
            raise Exception("Error importing measurements {}".format(r.text))
        records = r.json()

        # this is not elegant but... the epiviz-md api returns an 'id' which is the
        # database id, we want the id of the record to be the 'measurement_id' as returned 
        # by the epiviz-md api endpoint, so let's do that bit of surgery
        for rec in records:
            rec['id'] = rec.get('measurement_id')
            del rec['measurement_id']

        return records

    def import_emd(self, url, fileHandler=None, listen=True):
        """Import measurements from an epiviz-md metadata service api.

        Args:
            url: the url of the epiviz-md api
            handler: an optional filehandler to use
            listen: activate 'updateCollections' endpoint to add measurements from the service upon request
        """
        if listen:
            self.emd_endpoint = url

        records = self.get_from_emd(url)
        self.import_records(records, fileHandler=fileHandler)

    def add_computed_measurement(self, mtype, mid, name, measurements, computeFunc, genome=None, annotation=None, metadata=None, computeAxis=1):
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
        
        tempComputeM = ComputedMeasurement(mtype, mid, name, measurements=measurements, computeFunc=computeFunc, genome=genome, annotation=annotation, metadata=metadata, computeAxis=computeAxis)
        self.measurements.append(tempComputeM)
        return tempComputeM

    def add_genome(self, genome, url="http://obj.umiacs.umd.edu/genomes/", type=None, fileHandler=None):
        """Add a genome to the list of measurements. The genome has to be tabix indexed for the file server
           to make remote queries. Our tabix indexed files are available at https://obj.umiacs.umd.edu/genomes/index.html

        Args: 
            genome: for example : hg19 if type = "tabix" or full location of gtf file if type = "gtf"
            genome_id: required if type = "gtf"
            url: url to the genome file
        """
        isGene = True
        tempGenomeM = None

        if type == "tabix":
            gurl = url + genome + "/" + genome + ".txt.gz"
            tempGenomeM = FileMeasurement("tabix", genome, genome, 
                            gurl, genome, annotation={"group": "genome"},
                            metadata=["geneid", "exons_start", "exons_end", "gene"], minValue=0, maxValue=5,
                            isGenes=isGene, fileHandler=fileHandler, columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"]
                        )
            # self.genomes.append(tempGenomeM)
            # gtf_file = TbxFile(gurl)
            # self.genomes[genome] = gtf_file
            self.measurements.append(tempGenomeM)
        elif type == "efs-tsv":
            gurl = url
            tempGenomeM = FileMeasurement("gtfparsed", genome, genome, 
                            gurl, genome=genome, annotation={"group": "genome"},
                            metadata=["geneid", "exons_start", "exons_end", "gene"], minValue=0, maxValue=5,
                            isGenes=isGene, fileHandler=fileHandler, columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"]
                        )

            gtf_file = GtfParsedFile(gurl)
            self.genomes[genome] = gtf_file
            self.measurements.append(tempGenomeM)
        elif type == "gtf":
            gurl = url
            tempGenomeM = FileMeasurement("gtf", genome, genome, 
                            gurl, genome=genome, annotation={"group": "genome"},
                            metadata=["geneid", "exons_start", "exons_end", "gene"], minValue=0, maxValue=5,
                            isGenes=isGene, fileHandler=fileHandler, columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"]
                        )

            gtf_file = GtfFile(gurl)
            self.genomes[genome] = gtf_file
            self.measurements.append(tempGenomeM)
            
        return(tempGenomeM)

    def get_measurements(self):
        """Get all available measurements
        """
        return self.measurements

    def get_genomes(self):
        """Get all available genomes
        """
        return self.genomes

    def import_trackhub(self, hub, handler=None):
        """Import measurements from annotationHub objects. 

        Args: 
            ahub: list of file records from annotationHub
            handler: an optional filehandler to use
        """
        measurements = []
        trackhub = TrackHub(hub)
        if handler is not None:
            for m in trackhub.measurments:
                # TODO: this looks wrong
                m.fileHandler = fileHandler
                measurements.append(m)
        self.measurements.append(measurements)
        return measurements

    def update_collections(self, handler=None):
        result = []

        if self.emd_endpoint is None:
            return result, "Measurement manager is not listening"

        try:
            records = self.get_from_emd()
        except Exception as e:
            return result, "Error getting measurements from emd api {}".format(e)

        current_measurement_ids = [ms.mid for ms in self.get_measurements()]
        print(current_measurement_ids)
        new_records = [rec for rec in records if rec.get('id') not in current_measurement_ids]

        try:
            self.import_records(new_records, fileHandler=handler)
        except Exception as e:
            return "", "Error inserting new measurements from emd api {}".format(e)

        return new_records, ""