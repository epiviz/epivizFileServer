from parser import BigWig
from parser.BigBed import BigBed
import cProfile

# local File

print("############ local file initialization ####################")
# cProfile.run('test = BigWig("/home/jayaram/.AnnotationHub/39033.bigwig")')
cProfile.run('test = BigWig("/Users/evan/python/justBioThings/39033.bigwig")')
# 
# cProfile.run('test = BigBed("/home/evan/Desktop/epiviz/100transcripts.bb")')

# print("############ local file getRange ####################")
cProfile.run('print(test.getRange("chr9", 10550488, 11554489, 10, -1))')

# cProfile.run('print(test.getRange("chrI", 87262, 87854))')

# # remote File
# print("############ remote file initialization ####################")
# cProfile.run('test = BigWig("https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig")')

# cProfile.run('test = BigBed("https://obj.umiacs.umd.edu/bigwig-files/100transcripts.bb")')
# print("############ remote file getRange ####################")
# cProfile.run('print(test.getRange("chr9", 10550488, 11554489, 10, -1))')

# cProfile.run('print(test.getRange("chrI", 87262, 87854))')