from parser import BigWig


# local File
test = BigWig("/home/evan/Desktop/epiviz/39033.bigwig")
print(test.getRange("chr9", 10550488, 11554489, 10))

# remote File
# test = BigWig("http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/pval/E071-H3K27me3.pval.signal.bigwig")
# print(test.getRange("chr9", 11550488, 11554489, 10))
