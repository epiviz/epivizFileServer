from parser import BigWig

test = BigWig("../biology-related-python-files/39033.bigwig")
print(test.getRange("chr9", 11550488, 11554489, 10))


