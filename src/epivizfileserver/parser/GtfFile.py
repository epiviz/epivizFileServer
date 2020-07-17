import pysam
from .utils import toDataFrame
from .Helper import get_range_helper
import pandas as pd
from aiocache import cached, Cache
from aiocache.serializers import JsonSerializer, PickleSerializer

class GtfFile(object):
    """
    GTF File Class to parse gtf/gff files 

    Args:
        file (str): file location can be local (full path) or hosted publicly
        columns ([str]) : column names for various columns in file
    
    Attributes:
        file: a pysam file object
        fileSrc: location of the file
        cacheData: cache of accessed data in memory
        columns: column names to use
    """
    def __init__(self, file, columns=["chr", "source", "feature", "start", "end", "score", "strand", "frame", "group"]):
        self.fileSrc = file
        self.columns = columns

        print("Loading annotations", file)
        gtf = pd.read_csv(file, sep="\t", names = columns)
        
        print("Parsing gene names")
        gtf["gene_id"] = gtf["group"].apply(self.parse_attribute, key="gene_id").replace('"', "")
        # self.file["gene_idx"] = self.file["gene_id"].replace('"', "")

        print("Parsing transcript ids")
        gtf["transcript_id"] = gtf["group"].apply(self.parse_attribute, key="transcript_id")
        # self.file = self.file.set_index("gene_idx")

        # print("Groupby genes and collapse exon positions")
        # genes = gtf.groupby("gene_id")
        self.file = gtf

        # self.file = pd.DataFrame(columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"])

        # for name, gdf in genes:
        #     gdf_exons = gdf[(gdf["feature"].str.contains("exon", case=False, regex=True))]
            
        #     if len(gdf_exons) == 0:
        #         gdf_exons = gdf

        #     rec = {
        #         "chr": gdf["chr"].unique()[0],
        #         "start": gdf["start"].values.min(),
        #         "end": gdf["end"].values.max(),
        #         "width": gdf["end"].values.max() - gdf["start"].values.min(),
        #         "strand": gdf["strand"].unique()[0],
        #         "geneid": name.replace('"', ""),
        #         "exon_starts": ",".join(str(n) for n in gdf_exons["start"].values),
        #         "exon_ends": ",".join(str(n) for n in gdf_exons["end"].values),
        #         "gene": name.replace('"', "")
        #     }
        #     self.file = self.file.append(rec, ignore_index=True)

        print("Indexing by intervals")
        self.file["start_idx"] = self.file["start"]
        self.file["end_idx"] = self.file["end"]
        self.file = self.file.set_index(['start_idx', 'end_idx'])
        self.file.index = pd.IntervalIndex.from_tuples(self.file.index)

        print("Parsing chromsomes and their lengths")
        chromosomes = []
        groupByChr = self.file.groupby("chr")

        for name, gdf in groupByChr:
            chromosomes.append([name, 1, int(gdf["end"].values.max())])

        self.chromosomes = chromosomes

    def parse_attribute(self, item, key):
        if key in item:
            tstr = item.split(key, 1)
            tstrval = tstr[1].split(";", 1)
            return tstrval[0][1:]
        else:
            return None

    def search_gene(self, query, maxResults = 5):
        result = []
        err = None

        try:
            if len(query) > 1:
                genome = self.file[self.file["gene_id"].str.contains(query, na=False, case=False)]

                genes = genome.groupby("gene_id")
                for name, gdf in genes:
                    rec = {
                        "chr": gdf["chr"].unique()[0],
                        "start": int(gdf["start"].values.min()),
                        "end": int(gdf["end"].values.max()),
                        "gene": name.replace('"', "")            
                    }
                    result.append(rec)

                # for row, index in genes.head(maxResults):
                #     rec = {
                #         "chr": row["chr"],
                #         "start": row["start"],
                #         "end": row["end"],
                #         "gene": row["gene_id"]
                #     }
                #     result.append(rec)
                
                return result, err
        except Exception as e:
            return {}, str(e)

    def get_col_names(self):
        return self.columns

    def getRange(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        """Get data for a given genomic location

        Args:
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            respType (str): result format type, default is "DataFrame

        Returns:
            result
                a DataFrame with matched regions from the input genomic location if respType is DataFrame else result is an array
            error 
                if there was any error during the process
        """
        result = pd.DataFrame(columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"])

        try:
            grange = self.file[(self.file.index.left <= end) & (self.file.index.right >= start) & (self.file["chr"] == chr)]
            # grange = grange.sort_values(by=["start", "end"])

            if len(grange) > 0:
                genes = grange.groupby("gene_id")

                for name, gdf in genes:
                    gdf_exons = gdf[(gdf["feature"].str.contains("exon", case=False, regex=True))]
                    
                    if len(gdf_exons) == 0:
                        gdf_exons = gdf

                    rec = {
                        "chr": gdf["chr"].unique()[0],
                        "start": int(gdf["start"].values.min()),
                        "end": int(gdf["end"].values.max()),
                        "width": int(gdf["end"].values.max()) - int(gdf["start"].values.min()),
                        "strand": gdf["strand"].unique()[0],
                        "geneid": name.replace('"', ""),
                        "exon_starts": ",".join(str(int(n)) for n in gdf_exons["start"].values),
                        "exon_ends": ",".join(str(int(n)) for n in gdf_exons["end"].values),
                        "gene": name.replace('"', "")
                    }
                    result = result.append(rec, ignore_index=True)

                return result, None    
            else:
                return result, "no genes in the current region"

        except Exception as e:
            return result, str(e)

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="gtfsearchgene")
    async def searchGene(self, query, maxResults = 5):
        return self.search_gene(query, maxResults)
    
    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="gtfgetdata")
    async def get_data(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        return self.getRange(chr, start, end, bins=bins, zoomlvl=zoomlvl, metric=metric, respType=respType)
