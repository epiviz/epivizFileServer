from urllib.request import urlopen
from ..measurements import MeasurementManager, FileMeasurement

class TrackHub (object):
    """
    Base class for managing trackhub files
    TrackHub documentation is available at
    https://genome.ucsc.edu/goldenPath/help/hgTrackHubHelp.html

    Args: 
        file: location of trackhub directory
    """
    def __init__(self, file):
        self.file = file
        self.hub = self.parse_hub()
        self.mMgr = MeasurementManager()
        self.genomes = self.parse_genome()
        self.parse_genomeTracks()

    def parse_hub(self):
        hub_loc = self.file + "/hub.txt"
        hub = {}
        hub_count = 0
        # possible can be 
        # hub, shortLabel, longLabel,
        # genomesFile, email, descriptionUrl
        for line in urlopen(hub_loc):
            line = line.decode('ascii').strip()
            if len(line) > 0:
                [key, value] = line.split(" ", 1)
                key = key.strip()
                value = value.strip()
                if key in ["hub", "shortLabel", "longLabel",
                        "genomesFile", "email", "descriptionUrl"]:
                    hub[key] = value
                    if key is "hub":
                        hub_count += 1
                        if hub_count > 0:
                            print("hub.txt contains multiple hubs")
                else: 
                    print("key %s not valid in hub.txt" % (key))
        return hub

    def parse_genome(self):
        genome_loc = self.file + "/" + self.hub["genomesFile"]
        genomes = []
        genome = None
        
        # keys can be 
        # genome, trackDb, metaDb, metaTab,
        # twoBitPath, groups, description, 
        # organism, defaultPos, orderKey, htmlPath
        # scientificName (don't know why this is not documented)
        for line in urlopen(genome_loc):
            line = line.decode('ascii').strip()
            if len(line) > 0:
                [key, value] = line.split(" ", 1)
                key = key.strip()
                value = value.strip()
                if key == "genome":
                    if genome is not None:
                        genomes.append(genome)
                    genome = {}
                        # genome_obj[key] = value
                # elif key in ["trackDb", "metaDb", "metaTab",
                #         "twoBithPath", "groups", "description",
                #         "organism", "defaultPos", "orderKey",
                #         "htmlPath", "scientificName"]:
                genome[key] = value
                # else: 
                #     print("key %s not valid in genomes.txt" % (key))
        genomes.append(genome)
                
        return genomes

    def parse_genomeTracks(self):
        for genome in self.genomes:
            self.mMgr.add_genome(genome["genome"])
            track_loc = self.file + "/" + genome["trackDb"]
            tracks = self.parse_trackDb(track_loc)
            genome["trackDbParsed"] = tracks

            for track in tracks:
                track_type = track["type"].split(" ")[0]
                file_type = None
                file_ext = None
                
                if track_type in [ "bigBed", "bigWig"]:
                    # epiviz hanldes bigbeds and bigwigs
                    self.mMgr.measurements.append(FileMeasurement(
                        track_type, track["track"], track["shortLabel"], 
                        track["bigDataUrl"], annotation=None,
                        metadata=[], minValue=0, maxValue=5,
                        isGenes=False, fileHandler=None, columns=None)
                    )
                elif track_type in ["altGraphX", "bam", "bed", 
                    "bed5FloatScore", "bedGraph", 
                    "bedRnaElements", "bigBarChart",
                    "bigInteract", "bigPsl", "bigChain", "bigMaf", 
                    "broadPeak", "chain", "clonePos", "coloredExon", 
                    "ctgPos", "downloadsOnly", "encodeFiveC", "expRatio", 
                    "factorSource", "genePred", "gvf", "ld2", "narrowPeak", 
                    "netAlign", "peptideMapping", "psl", "rmsk", "snake", 
                    "vcfTabix", "wig", "wigMaf"]:
                    # infer file type from the extension in the url
                    file_ext = track["bigDataUrl"].split(".")
                    file_ext = file_ext[len(file_ext)-1]
                    
                    if file_ext.lower() in ["bb", "bigbed"]:
                        file_type = "bigBed"
                    elif file_ext.lower() in ["bw", "bigwig"]:
                        file_type = "bigWig"
                    elif file_ext.lower() in ["tbi", "tbx", "tabix"]:
                        file_type = "tabix"

                    self.mMgr.measurements.append(FileMeasurement(
                        file_type, track["track"], track["shortLabel"], 
                        track["bigDataUrl"], annotation=None,
                        metadata=[], minValue=0, maxValue=5,
                        isGenes=False, fileHandler=None, columns=[])
                    )

    def parse_trackDb(self, track_loc):
        # required fields in the track file
        # track, bigDataUrl, shortLabel, longLabel, type
        tracks = []
        track = None
        for line in urlopen(track_loc):
            line = line.decode('ascii').strip()
            if len(line) > 0:
                [key, value] = line.split(" ", 1)
                key = key.strip()
                value = value.strip()
                if key == "track":
                    if track is not None:
                        tracks.append(track)
                    track = {}

                track[key] = value

        tracks.append(track)

        # validate required fields
        fields = ["track", "bigDataUrl", "shortLabel", "longLabel", "type"]
        for key in fields:
            if not track[key]:
                print("Error in trackDb file located at %s, %s does not exist" % (track, key))

        return tracks