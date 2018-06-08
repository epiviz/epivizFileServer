from parser import BigWig

test = BigWig("/home/jayaram/.AnnotationHub/39033.bigwig")
print(test.getRange("chrY", 0, 30, 2))


