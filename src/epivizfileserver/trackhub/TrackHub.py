from urllib.request import urlopen
from ..measurements import FileMeasurement

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
        self.measurements = []
        self.genomes = self.parse_genome()
        self.parse_genomeTracks()

    def parse_hub(self):
        hub_loc = self.file + "/hub.txt"
        hub = {}
        hub_count = 0
        # fields can be
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
            url = "http://obj.umiacs.umd.edu/genomes/"
            gurl = url + genome["genome"] + "/" + genome["genome"] + ".txt.gz"
            tempGenomeM = FileMeasurement("tabix", genome["genome"], genome["genome"], 
                        gurl, annotation=None,
                        metadata=["GENEID", "exons_start", "exons_end", "gene"], minValue=0, maxValue=5,
                        isGenes=True, fileHandler=None, columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"]
                    )
            # self.measurements.append(tempGenomeM)
            
            track_loc = self.file + "/" + genome["trackDb"]
            tracks = self.parse_trackDb(track_loc)

            genome["trackDbParsed"] = tracks

            for track in tracks:
                if "container" not in track:
                    track_type = track["type"].split(" ")[0]
                    file_type = None
                    file_ext = None
                    
                    if track_type in [ "bigBed", "bigWig"]:
                        isgene = False
                        if track_type == "bigBed":
                            isgene = True
                        # epiviz hanldes bigbeds and bigwigs
                        self.measurements.append(FileMeasurement(
                            track_type, track["parent"] + "_" + track["track"], track["longLabel"], 
                            track["bigDataUrl"], annotation=None,
                            metadata=[], minValue=0, maxValue=5,
                            isGenes=isgene, fileHandler=None, columns=None)
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
                        file_columns = []
                        
                        if file_ext.lower() in ["bb", "bigbed"]:
                            file_type = "bigBed"
                            file_columns = track["barChartBars"].split(" ")

                            # print(file_columns)

                            for fcol in file_columns:
                                self.measurements.append(FileMeasurement(
                                    file_type, fcol, track["shortLabel"] + "-" + fcol, 
                                    track["bigDataUrl"], annotation=None,
                                    metadata=[], minValue=0, maxValue=5,
                                    isGenes=False, fileHandler=None, columns=file_columns)
                                )

                        elif file_ext.lower() in ["bw", "bigwig"]:
                            file_type = "bigWig"
                        elif file_ext.lower() in ["tbi", "tbx", "tabix"]:
                            file_type = "tabix"

                        self.measurements.append(FileMeasurement(
                            file_type, track["track"], track["shortLabel"], 
                            track["bigDataUrl"], annotation=None,
                            metadata=[], minValue=0, maxValue=5,
                            isGenes=False, fileHandler=None, columns=file_columns)
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