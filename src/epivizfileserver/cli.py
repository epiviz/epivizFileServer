"""Download and/or Build Genome or Transcript files for use with Epiviz File Server. either --ucsc or --gtf must be provided.
   To generate a genome file,       efs build_genome --ucsc=mm10 --output=mm10
   To generate a transcripts file,  efs build_transcript --ucsc=mm10 --output=mm10 (transcript files are prepended with `transcripts.`)
   To generate both files,          efs build_both --ucsc=mm10 --output=mm10

Usage:
  efs.py (build_genome | build_transcript | build_both) (--ucsc=<genome> | --gtf=<file>) [--compressed] [--output=<output>]

Options:
  --ucsc=<genome>   genome build to download and parse from ucsc, eg: mm10
  --gtf=<file>  local gtf file
  -c --compressed   File is gzip compressed
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

def parse_gtf(full_path, compressed):
    print("Reading File - ", full_path)
    if compressed:
        df = pd.read_csv(full_path, sep="\t", names = ["chr", "source", "feature", "start", "end", "score", "strand", "frame", "group"], compression="gzip")
    else:
        df = pd.read_csv(full_path, sep="\t", names = ["chr", "source", "feature", "start", "end", "score", "strand", "frame", "group"])

    print("Parsing Gene names")
    df["gene_id"] = df["group"].apply(parse_attribute, key="gene_id").replace('"', "")
    
    print("Parsing transcript_ids")
    df["transcript_id"] = df["group"].apply(parse_attribute, key="transcript_id").replace('"', "")
    
    return df

def parse_genome(full_path, compressed):
    df = parse_gtf(full_path, compressed)

    df["gene_idx"] = df["gene_id"]
    df["chr_idx"] = df["chr"]
    df = df.set_index(["gene_idx", "chr_idx"])

    # print("removing LOC genes")
    # df = df[~df["gene_id"].str.startswith("LOC")]

    print("Group by genes and collapsing exons positions")
    genes = df.groupby(["gene_idx", "chr_idx"])
    res = pd.DataFrame(columns=["chr", "start", "end", "width", "strand", "geneid", "exon_starts", "exon_ends", "gene"])

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
    res = res.sort_values(by=["chr", "start"])

    return res

def parse_transcript(full_path, compressed):
    df = parse_gtf(full_path, compressed)

    cols = ["chr", "start", "end", "feature", "transcript_id", "score", "strand", "gene_id"]

    df = df[cols]
    df = df.sort_values(by=["chr", "start"])

    # print(df.head())

    # print("sanitize gene symbols and transcript names/ids")
    # df["transcript_id"] = [x.replace('"', "") for x in df["transcript_id"].values]
    # df["gene_id"] = [x.replace('"', "") for x in df["gene_id"].values]

    # print("remove unannotated chr's")
    # df = df[~df["chr"].str.contains("_")]
    # df.head()

    print("group transcripts and collapse exon positions")
    transcripts = df.groupby(["transcript_id", "chr"])

    res = pd.DataFrame(columns=["chr", "start", "end", "width", "strand", 
                            "transcript_id", 
                            "exon_starts", "exon_ends" "gene"])

    for (name, chr), gdf in transcripts:
        gdf_exons = gdf[(gdf["feature"].str.contains("exon", case=False, regex=True))]
        gdf_exons = gdf_exons.sort_values(by=["start", "end"])

        if len(gdf_exons) == 0:
            gdf_exons = gdf
        
        rec = {
            "chr": chr,
            "start": gdf["start"].values.min(),
            "end": gdf["end"].values.max(),
            "width": gdf["end"].values.max() - gdf["start"].values.min(),
            "strand": gdf["strand"].unique()[0],
            "transcript_id": name.replace('"', ""),
            "exon_starts": ",".join(str(n) for n in gdf_exons["start"].values),
            "exon_ends": ",".join(str(n) for n in gdf_exons["end"].values),
            "gene": gdf["gene_id"].unique()[0].replace('"', "")
        }
        res = res.append(rec, ignore_index=True)

    cols = ['chr', 'start', 'end', 'strand', 'transcript_id',
       'exon_starts', 'exon_ends', 'gene']
    res = res[cols]

    res = res.sort_values(by=["chr", "start"])

    return res

def main():
    args = docopt(__doc__)
    
    genome = args["--ucsc"]
    gtf = args["--gtf"]
    output = args["--output"]
    compressed = args["--compressed"]
    build_genome = args["build_genome"]
    build_transcript = args["build_transcript"]
    build_both = args["build_both"]

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

    if build_both:
        res = parse_genome(full_path, compressed)

        print("Write genomes to disk - ", output)
        res.to_csv(output, header = False, index = False, sep="\t")

        res = parse_transcript(full_path, compressed)

        print("Write transcripts to disk - ", output)
        res.to_csv("transcripts." + output, header = False, index = False, sep="\t")

    else:
        if build_genome:
            res = parse_genome(full_path, compressed)

            print("Write to disk - ", output)
            res.to_csv(output, header = False, index = False, sep="\t")

        elif build_transcript:
            res = parse_transcript(full_path, compressed)

            print("Write to disk - ", "transcripts." + output)
            res.to_csv("transcripts." + output, header = False, index = False, sep="\t")

if __name__ == '__main__':
    main()