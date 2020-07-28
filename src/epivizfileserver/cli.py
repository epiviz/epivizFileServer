"""Download and Build Genome files for use with Epiviz File Server. either --ucsc or --gtf must be provided.

Usage:
  efs.py build_genome (--ucsc=<genome> | --gtf=<file>) [--output=<output>]

Options:
  --ucsc=<genome> genome build to download and parse from ucsc, eg: mm10
  --gtf=<file> local gtf file
  --output=<output> output prefix of file to save, defaults to current directory and saves result as output.tsv eg: ./mm10
  -h --help     Show this screen.
"""

import pandas as pd
import os
from docopt import docopt
from tqdm import tqdm
# import concurrent.futures

def parse_attribute(item, key):
    if key in item:
        tstr = item.split(key, 1)
        tstrval = tstr[1].split(";", 1)
        return tstrval[0][1:]
    else:
        return None

def parse_group(name, chrm, gdf):
    gdf_exons = gdf[(gdf["feature"].str.contains("exon", case=False, regex=True))]
    gdf_exons = gdf_exons.sort_values(by=["chr", "start", "end"])

    if len(gdf_exons) == 0:
        gdf_exons = gdf

    rec = {
        "chr": chrm,
        "start": gdf["start"].values.min(),
        "end": gdf["end"].values.max(),
        "width": gdf["end"].values.max() - gdf["start"].values.min(),
        "strand": gdf["strand"].unique()[0],
        "geneid": name.replace('"', ""),
        "exon_starts": ",".join(str(n) for n in gdf_exons["start"].values),
        "exon_ends": ",".join(str(n) for n in gdf_exons["end"].values),
        "gene": name.replace('"', "")
    }
    return rec

def main():
    args = docopt(__doc__)
    
    genome = args["--ucsc"]
    gtf = args["--gtf"]
    output = args["--output"]

    # if genome is None and gtf is None:
    #     raise("either --ucsc or --gtf must be provided")
    full_path = None
    if genome is not None:
        path = "http://hgdownload.cse.ucsc.edu/goldenPath/" + genome + "/bigZips/genes/"
        file = genome + ".refGene.gtf.gz"
        full_path = path + file
    elif gtf is not None:
        full_path = gtf

    if output is None:
        if genome is not None:
            output = os.getcwd() + "/" + genome + ".tsv.gz"
        else:
            output = os.getcwd() + "/output.tsv.gz"
    else:
        output = output + ".tsv.gz"


    if full_path is None:
        raise("either --ucsc or --gtf must be provided")

    print("Reading File - ", full_path)
    df = pd.read_csv(full_path, sep="\t", names = ["chr", "source", "feature", "start", "end", "score", "strand", "frame", "group"])

    print("Parsing Gene names")
    df["gene_id"] = df["group"].apply(parse_attribute, key="gene_id").replace('"', "")
    
    print("Parsing transcript_ids")
    df["transcript_id"] = df["group"].apply(parse_attribute, key="transcript_id").replace('"', "")
    df["gene_idx"] = df["gene_id"]
    df["chr_idx"] = df["chr"]
    df = df.set_index(["gene_idx", "chr_idx"])

    # print("removing RNA genes")
    # df = df[~df["gene_id"].str.startswith("LOC")]

    print("Group by genes and collapsing exons positions")
    genes = df.groupby(["gene_idx", "chr_idx"])
    res = pd.DataFrame(columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"])

    # pool = mp.Pool(threads=int(mp.cpu_count()/2))
    # res = pool.map( parse_group, [(name, chrm, group) for (name, chrm),group in genes])
    # res = pd.concat(res)

    print("iterating genes ... ")
    for (name, chrm), gdf in tqdm(genes):
        gdf_exons = gdf[(gdf["feature"].str.contains("exon", case=False, regex=True))]
        gdf_exons = gdf_exons.sort_values(by=["chr", "start", "end"])

        if len(gdf_exons) == 0:
            gdf_exons = gdf

        rec = {
            "chr": chrm,
            "start": gdf["start"].values.min(),
            "end": gdf["end"].values.max(),
            "width": gdf["end"].values.max() - gdf["start"].values.min(),
            "strand": gdf["strand"].unique()[0],
            "geneid": name.replace('"', ""),
            "exon_starts": ",".join(str(n) for n in gdf_exons["start"].values),
            "exon_ends": ",".join(str(n) for n in gdf_exons["end"].values),
            "gene": name.replace('"', "")
        }
        res = res.append(rec, ignore_index=True)

    print("Sorting by chromosome") 
    res = res.sort_values(by=["chr", "start", "end"])
    
    print("Write to disk - ", output)
    res.to_csv(output, header = False, index = False, sep="\t")

if __name__ == '__main__':
    main()