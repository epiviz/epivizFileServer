from parser import BigWig, BigBed
import cProfile

# local File
# print("############ local file initialization ####################")
# test = BigWig("/home/jayaram/.AnnotationHub/39033.bigwig")
# print("############ local file getRange ####################")
# print(test.getRange("chr9", 9550488, 11554489, 10))

# print("############ remote file initialization ####################")
# test = BigWig("https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig")
# print("############ remote file getRange ####################")
# print(test.getRange("chr9", 9550488, 11554489, 10))

# print("############ remote file initialization ####################")
# test = BigBed("data/test.bigBed")
# print("############ remote file getRange ####################")
# print(test.getRange("chr1", 100000000, 110000000))

print("############ remote file initialization ####################")
test = BigBed("https://obj.umiacs.umd.edu/bigwig-files/100transcripts_bed4plus_bonus_as.bb")
print("############ remote file getRange ####################")
print(test.getRange("chrI", 1, 110000000))