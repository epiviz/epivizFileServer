file = "https://obj.umiacs.umd.edu/bigwig-files/hg38.gtf.gz"
from parser import GtfFile
f = GtfFile(file)
f.getRange("11", 9998000, 9999000)