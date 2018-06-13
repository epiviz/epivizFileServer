from parser import BigWig
import cProfile

# local File

print("############ local file initialization ####################")
cProfile.run('test = BigWig("/home/jayaram/.AnnotationHub/39033.bigwig")')
# cProfile.run('test = BigWig("/home/evan/Desktop/epiviz/39033.bigwig")')
print("############ local file getRange ####################")
cProfile.run('print(test.getRange("chr9", 10550488, 11554489, 10))')

# remote File
print("############ remote file initialization ####################")
cProfile.run('test = BigWig("https://obj.umiacs.umd.edu/bigwig-files/39033.bigwig")')
print("############ remote file getRange ####################")
cProfile.run('print(test.getRange("chr9", 10550488, 11554489, 10))')
