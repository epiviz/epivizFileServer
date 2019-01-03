from parser import BigWig, BigBed
# from parser.BigBed import BigBed
import cProfile
# import asyncio
# from handler import FileHandlerProcess

# local File

# print("############ local file initialization ####################")
# cProfile.run('test = BigWig("/home/jayaram/.AnnotationHub/39033.bigwig")')
# cProfile.run('ph = FileHandlerProcess(900, 10)')
# 
# cProfile.run('test = BigBed("/home/evan/Desktop/epiviz/100transcripts.bb")')

# print("############ local file getRange ####################")
# cProfile.run("result = await ph.handleFile('https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig', 'chr8', 115343360, 115443360)")
# cProfile.run('print(result)')

# ph = FileHandlerProcess(900, 10)
test = BigWig('https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig')
result,_ = test.getRange('chr8', 115343360, 115443360)
print(result)
# cProfile.run('print(test.getRange("chrI", 87262, 87854))')

# # remote File
# print("############ remote file initialization ####################")
# cProfile.run('test = BigWig("https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig")')

# cProfile.run('test = BigBed("https://obj.umiacs.umd.edu/bigwig-files/100transcripts.bb")')
# print("############ remote file getRange ####################")
# cProfile.run('print(test.getRange("chr9", 10550488, 11554489, 10, 3))')


test = BigBed("/home/jayaram/Desktop/projects/file-server-paper/epivizFileParser/data/test.bigBed")
# test = BigBed("https://obj.umiacs.umd.edu/bigwig-files/100transcripts.bb")
result, _ = test.getRange("chr1", 1, 11554489)
print(result)