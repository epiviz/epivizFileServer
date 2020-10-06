from aiocache import cached, SimpleMemoryCache
from aiocache.serializers import JsonSerializer
import pandas as pd
from .measurementClass import DbMeasurement, FileMeasurement, ComputedMeasurement
from ..trackhub import TrackHub
from ..parser import GtfParsedFile, TbxFile, BigBed
import ujson
import requests
import pandas as pd

from sanic.log import logger as logging

class EMDMeasurementMap(object):
    """
    Manage mapping between measuremnts in EFS and metadata service

    """
    def __init__(self, url, fileHandler):
        self.emd_endpoint = url
        self.handler = fileHandler

        # collection records from emd
        self.collections = dict()

        # map { emd id => efs measurement id }
        self.measurement_map = dict()

    def init(self):
        logging.debug("Initializing from emd at {}".format(self.emd_endpoint))
        self.init_collections()
        records = self.init_measurements()
        logging.debug("Done initializing from emd")
        return records

    def init_collections(self):
        req_url = self.emd_endpoint + "/collections/"
        logging.debug("Initializing collections from emd")
        r = requests.get(req_url)
        if r.status_code != 200:
            raise Exception("Error initializing collections from emd {}: {}".format(req_url, r.text))

        collection_records = r.json()    
        for rec in collection_records:
            # map database id to efs id
            self.collections[rec['id']] = rec['collection_id']
        logging.debug("Done initializing collections from emd")

    def process_emd_record(self, rec):
        # this is not elegant but... the epiviz-md api returns an 'id' which is the
        # database id, we want the id of the record to be the 'measurement_id' as returned 
        # by the epiviz-md api endpoint, so let's do that bit of surgery
        # we keep a map between ids here
        self.measurement_map[rec['id']] = rec['measurement_id']

        rec['id'] = rec['measurement_id']
        del rec['measurement_id']

        collection_id = rec['collection_id']
        del rec['collection_id']
        collection_name = self.collections[collection_id]

        current_annotation = rec['annotation']
        if current_annotation is None:
            current_annotation = { "collection": collection_name }
        else:
            current_annotation['collection'] = collection_name
        rec['annotation'] = current_annotation

    def init_measurements(self):
        req_url = self.emd_endpoint + "/ms/"
        logging.debug("Initializing measurements from emd")
        r = requests.get(req_url)
        if r.status_code != 200:
            raise Exception("Error initializing measurements from emd {}: {}".format(req_url, r.text))

        records = r.json()

        for rec in records:
            self.process_emd_record(rec)

        logging.debug("Done initializing measurements")
        return records

    def sync(self, current_ms):
        logging.debug("Syncing with emd at {}".format(self.emd_endpoint))

        # this will remove deleted collections from
        # the collection id map
        new_collections = self.sync_collections()
        new_records_from_collections = self.add_new_collections(new_collections)

        # this will remove measurements in current_ms
        # no longer in the emd db
        new_measurements = self.sync_measurements(current_ms)
        new_records = self.add_new_measurements(new_measurements)
        logging.debug("Done syncing with emd")
        return new_records_from_collections + new_records

    def sync_collections(self):
        req_url = self.emd_endpoint + "/collections/ids"
        logging.debug("Syncing collections from emd")
        r = requests.get(req_url)
        if r.status_code != 200:
            raise Exception("Error getting collection ids to sync from emd {}: {}".format(req_url, r.text))

        emd_ids = r.json()
        new_ids = list(set(emd_ids) - set(self.collections.values()))
        del_ids = [ k for k, v in self.collections.items() if v not in emd_ids ]

        for id in del_ids:
            del self.collections[id]

        return new_ids

    def add_new_collections(self, new_collection_ids):
        logging.debug("Adding new collections from emd")
        all_records = []

        for collection_id in new_collection_ids:
            req_url = self.emd_endpoint + "/collections/" + collection_id
            r = requests.get(req_url)
            if r.status_code != 200:
                raise Exception("Error getting collection with id {} from {}: {}".format(collection_id, req_url, r.text))

            rec = r.json()

            # map emd db id to efs id
            self.collections[rec['id']] = rec['collection_id']
            logging.debug("Added new collection {} from emd".format(rec['collection_id']))
            
            logging.debug("Adding measurements from collection {} from emd".format(rec['collection_id']))
            req_url = self.emd_endpoint + "/collections/" + collection_id + "/ms"
            r = requests.get(req_url)
            if r.status_code != 200:
                raise Exception("Error getting records for collection with id {} from {}: {}".format(collection_id, req_url, r.text))

            records = r.json()
            for rec in records:
                self.process_emd_record(rec)

            logging.debug("Done adding measurements from new collection")
            all_records.extend(records)

        logging.debug("Done adding new collections from emd")
        return all_records

    def sync_measurements(self, current_ms):
        req_url = self.emd_endpoint + "/ms/ids"
        logging.debug("Syncing measurements from emd")
        r = requests.get(req_url)
        if r.status_code != 200:
            raise Exception("Error getting ms ids to sync from emd {}: {}".format(req_url, r.text))

        ms_ids = r.json()
        new_ids = list(set(ms_ids) - set(self.measurement_map.values()))
        del_ids = [ k for k, v in self.measurement_map.items() if v not in ms_ids]

        for id in del_ids:
            ms_id = self.measurement_map[id]
            del current_ms[ms_id]

            if id in self.measurement_map:
                del self.measurement_map[id]
            else:
                logging.debug("Tried to del ms map {}: not found".format(id))

        return new_ids

    def add_new_measurements(self, new_ms_ids):
        logging.debug("Adding new ms from emd")
        all_records = []
        
        for ms_id in new_ms_ids:
            req_url = self.emd_endpoint + "/ms/" + ms_id
            r = requests.get(req_url)
            if r.status_code != 200:
                raise Exception("Error getting ms with id {} from {}: {}".format(ms_id, req_url, r.text))

            rec = r.json()
            self.process_emd_record(rec)
            all_records.append(rec)

        logging.debug("Done adding new ms from emd")
        return all_records

class MeasurementSet(object):
    def __init__(self):
        self.measurements = {}

    def append(self, ms):
        self.measurements[ms.mid] = ms

    def __delitem__(self, key):
        if key in self.measurements:
            del self.measurements[key]
        else:
            logging.debug("Tried to del ms {}: not found".format(key))

    def get(self, key):
        return self.measurements[key] if key in self.measurements else None

    def get_measurements(self):
        return self.measurements.values()

    def get_mids(self):
        return self.measurements.keys()
    
class MeasurementManager(object):
    """
    Measurement manager class

    Attributes:
        measurements: list of all measurements managed by the system
    """

    def __init__(self):
        # self.measurements = pd.DataFrame()
        self.genomes = {}
        self.measurements = MeasurementSet()
        self.emd_endpoint = None
        self.emd_map = None
        self.tiledb = []
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

        num_records = len(records)

        for i, rec in enumerate(records):
            format_args = { "i": i,
                "num_records": num_records,
                "datatype": rec['datatype'],
                "file_type": rec['file_type']
            }
            logging.debug("Importing record {i}/{num_records} with datatype {datatype} and file type {file_type}".format(**format_args))

            isGene = False
            if "annotation" in rec["datatype"]:
                isGene = True

            if rec.get("genome") is None and genome is None: 
                raise Exception("all files must be annotated with its genome build")

            tgenome = rec.get("genome")
            if tgenome is None:
                tgenome = genome

            if rec.get("file_type") == "tiledb":
                # its expression dataset 
                samples = pd.read_csv(rec.get("url") + "/cols", sep="\t", index_col=0)
                sample_names = samples.index.values

                rows = pd.read_csv(rec.get("url") + "/rows", sep="\t", index_col=False, nrows=10)
                metadata = rows.columns.values
                metadata = [ m for m in metadata if m not in ['chr', 'start', 'end'] ]

                for samp, (index, row) in zip(sample_names, samples.iterrows()):
                    anno = row.to_dict()
                    anno["_filetype"] = rec.get("file_type")
                    for key in rec.get("annotation").keys():
                        anno[key] = rec.get("annotation").get(key)
                    tempFileM = FileMeasurement(rec.get("file_type"), samp, 
                            samp + "_" + rec.get("name"), 
                            rec.get("url"), genome=tgenome, annotation=anno,
                            metadata=metadata, minValue=0, maxValue=20,
                            isGenes=isGene, fileHandler=fileHandler
                        )
                    measurements.append(tempFileM)
                    self.measurements.append(tempFileM)
                           
            elif rec.get("file_type").lower() in ["gwas", "bigbed"]:
                anno = rec.get("annotation")

                if anno is None:
                    anno = {}
                
                bw = BigBed(rec.get("url"))
                metadata = bw.get_autosql()

                if metadata and len(metadata) > 3:
                    metadata = metadata[3:]
                else:
                    metadata = []


                anno["_filetype"] = rec.get("file_type")
                tempFileM = FileMeasurement(rec.get("file_type"), rec.get("id"), rec.get("name"), 
                            rec.get("url"), genome=tgenome, annotation=anno,
                            metadata=metadata, minValue=0, maxValue=5,
                            isGenes=isGene, fileHandler=fileHandler
                        )
                measurements.append(tempFileM)
                self.measurements.append(tempFileM)

            else:
                anno = rec.get("annotation")

                if anno is None:
                    anno = {}

                anno["_filetype"] = rec.get("file_type")
                tempFileM = FileMeasurement(rec.get("file_type"), rec.get("id"), rec.get("name"), 
                            rec.get("url"), genome=tgenome, annotation=anno,
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

        req_url = url + "/collections/"
        r = requests.get(req_url)
        if r.status_code != 200:
            raise Exception("Error getting collections from emd {}".format(req_url))

        collection_records = r.json()
        collections = {}
    
        for rec in collection_records:
            collections[rec['id']] = rec['collection_id']


        req_url = url + "/ms/"
        r = requests.get(req_url)
        if r.status_code != 200:
            raise Exception("Error importing measurements from collection {} with url {}: {}".format(collection_record['collection_id'], req_url, r.text))


        records = r.json()

        # this is not elegant but... the epiviz-md api returns an 'id' which is the
        # database id, we want the id of the record to be the 'measurement_id' as returned 
        # by the epiviz-md api endpoint, so let's do that bit of surgery
        for rec in records:
            rec['id'] = rec['measurement_id']
            del rec['measurement_id']

            collection_id = rec['collection_id']
            del rec['collection_id']
            collection_name = collections[collection_id]

            current_annotation = rec['annotation']
            if current_annotation is None:
                current_annotation = { "collection": collection_name }
            else:
                current_annotation['collection'] = collection_name
            rec['annotation'] = current_annotation

        return records

    def use_emd(self, url, fileHandler=None):
        """Delegate all getMeasurement calls to an epiviz-md metdata service api

        Args:
            url: the url of the epiviz-md api
            fileHandler: an optional filehandler to use
        """
        logging.debug("Will be using emd api at {}".format(url))
        self.emd_map = EMDMeasurementMap(url, fileHandler)
        records = self.emd_map.init()
        self.import_records(records, fileHandler = fileHandler)

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
        if self.emd_map is not None:
            # this will remove measurements in self.measureemnts
            # that are not in the emd dbs any more
            logging.debug("Getting mesurements. Cur ms {}".format(list(self.measurements.get_mids())))
            new_records = self.emd_map.sync(self.measurements)
            self.import_records(new_records, fileHandler = self.emd_map.handler)
        return self.measurements.get_measurements()

    def get_measurement(self, ms_id):
        """Get a specific measurement
        """
        return self.measurements.get(ms_id)

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
