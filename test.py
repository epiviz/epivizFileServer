from parser import BigWig
import cProfile

# local File
print("############ local file initialization ####################")
cProfile.run('test = BigWig("/home/jayaram/.AnnotationHub/39033.bigwig")')
print("############ local file getRange ####################")
cProfile.run('print(test.getRange("chr9", 11550488, 11554489, 10))')

# remote File
print("############ remote file initialization ####################")
# cProfile.run('test = BigWig("http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/pval/E071-H3K27me3.pval.signal.bigwig")')
cProfile.run('test = BigWig("https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig")')
print("############ remote file getRange ####################")
cProfile.run('print(test.getRange("chr9", 11550488, 11554489, 10))')
