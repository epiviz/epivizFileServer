from parser import BigWig
from parser.BigBed import BigBed
import cProfile
import asyncio
from handler import FileHandlerProcess

# local File

print("############ local file initialization ####################")
# cProfile.run('test = BigWig("/home/jayaram/.AnnotationHub/39033.bigwig")')
cProfile.run('ph = FileHandlerProcess(900, 1500, 10)')
# 
# cProfile.run('test = BigBed("/home/evan/Desktop/epiviz/100transcripts.bb")')

# print("############ local file getRange ####################")
cProfile.run("result = await ph.handleBigWig('https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig', 'chr9', 10550488, 11554489, 3)")
cProfile.run('print(result)')

# cProfile.run('print(test.getRange("chrI", 87262, 87854))')

# # remote File
# print("############ remote file initialization ####################")
# cProfile.run('test = BigWig("https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig")')

# cProfile.run('test = BigBed("https://obj.umiacs.umd.edu/bigwig-files/100transcripts.bb")')
# print("############ remote file getRange ####################")
# cProfile.run('print(test.getRange("chr9", 10550488, 11554489, 10, 3))')
