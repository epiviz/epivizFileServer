import pysam
from .utils import toDataFrame
from .Helper import get_range_helper
import pandas as pd


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
        self.file = pd.read_csv(file, sep="\t", names = columns)
        self.file["gene_id"] = self.file["group"].apply(self.parse_attribute, key="gene_id")
        self.file["transcript_id"] = self.file["group"].apply(self.parse_attribute, key="transcript_id")

        self.fileSrc = file
        self.columns = columns

    def parse_attribute(self, item, key):
        if key in item:
            tstr = item.split(key, 1)
            tstrval = tstr[1].split(";", 1)
            return tstrval[0][1:]
        else:
            return None

    def search_gene(self, query):
        try:
            if len(query) > 1:
                genome = self.file[self.file['gene_id'].str.contains(query, na=False, case=False)]

                if len(genome) > 0:
                    for index, row in genome.head():
                        result.append({"gene": row["gene"], "chr": row["chr"], "start": row["start"], "end": row["end"]})

                return result, None
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
        final_genes = pd.DataFrame(columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"])

        try:
            grange = self.file[(self.file["chr"] == chr) & (self.file["start"] <= end) & (self.file["end"] >= start)]

            if len(grange) > 0:
                genes = grange.groupby("gene_id")

                for name, gdf in genes:
                    gdf_exons = gdf[(gdf["feature"].str.contains("exon", case=False, regex=True))]
                    
                    if len(gdf_exons) == 0:
                        gdf_exons = gdf

                    rec = {
                        "chr": gdf["chr"].unique()[0],
                        "start": gdf["start"].values.min(),
                        "end": gdf["end"].values.max(),
                        "width": gdf["end"].values.max() - gdf["start"].values.min(),
                        "strand": gdf["strand"].unique()[0],
                        "geneid": name.replace('"', ""),
                        "exon_starts": ",".join(str(n) for n in gdf_exons["start"].values),
                        "exon_ends": ",".join(str(n) for n in gdf_exons["end"].values),
                        "gene": name.replace('"', "")
                    }
                    final_genes = final_genes.append(rec, ignore_index=True)

                return final_genes, None    
            else:
                return final_genes, "no genes in the current region"

        except Exception as e:
            return final_genes, str(e)
