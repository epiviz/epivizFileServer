Compute and Query Parser for Genomic Files

Epiviz file Server is a Python library, to query genomic files, not only for visualization but also for transformation. The library provides various modules to perform various tasks - 1) Parser to read various genomic file formats, 2) Query to access only necessary bytes of file, 3) Compute to apply transformations on data, 4) Server to instantly convert the datasets into an API and 5) Visualization. 


A quick overview of the library and its features, are described in an IPython notebook available at - 

https://epiviz.github.io/post/2019-02-04-epiviz-fileserver/

Note: 
1.  The library requires the server hosting the data files to support HTTP range requests so that the file server's parser module can only request the necessary byte-ranges needed to process the query
2. The library currently supports indexed genomic file formats like BigWig, BigBed, Bam (with bai), Sam (with sai) or any data set that can be indexed using tabix.