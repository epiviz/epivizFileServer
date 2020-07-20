import pysam
from .utils import toDataFrame
from .Helper import get_range_helper
import pandas as pd
from aiocache import cached, Cache
from aiocache.serializers import JsonSerializer, PickleSerializer

class GtfParsedFile(object):
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
    def __init__(self, file, columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"]):
        self.fileSrc = file
        self.columns = columns

        print("Loading annotations", file)
        self.file = pd.read_csv(file, sep="\t", names = columns)
        self.file["gene_idx"] = self.file["gene"]
        self.file = self.file.set_index("gene_idx")

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
                matched = self.file[self.file["gene"].str.contains(query, na=False, case=False)]

                counter = 0
                for index, row in matched.iterrows():
                    rec = {
                        "chr": row["chr"],
                        "start": int(row["start"]),
                        "end": int(row["end"]),
                        "gene": row["gene"]
                    }
                    result.append(rec)
                    counter += 1
                    if counter >= int(maxResults):
                        break
                
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
            result = self.file[(self.file["start"] <= end) & (self.file["end"] >= start) & (self.file["chr"] == chr)]

            # removing RNA genes
            # result = result[~result["gene"].str.startswith("LOC")]
            # result = result[~result["gene"].str.startswith("LIN")]
            # result = result[result["width"] < 500000]


            result = result.sort_values(by=["chr", "start", "end"])

            # print(result)
            # if len(grange) > 0:
            #     result = grange.to_dict(orient="records")

            #     print(result)

            return result, None    
            # else:
            #     return result, "no genes in the current region"

        except Exception as e:
            return result, str(e)

    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="gtfsearchgene")
    async def searchGene(self, query, maxResults = 5):
        return self.search_gene(query, maxResults)
    
    @cached(ttl=None, cache=Cache.MEMORY, serializer=PickleSerializer(), namespace="gtfgetdata")
    async def get_data(self, chr, start, end, bins=2000, zoomlvl=-1, metric="AVG", respType = "DataFrame"):
        return self.getRange(chr, start, end, bins=bins, zoomlvl=zoomlvl, metric=metric, respType=respType)
