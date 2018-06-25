from flask import Flask, request, g
from handler import FileHandler
app = Flask(__name__)
ph = None

@app.before_first_request
def startup():
    global ph
    ph = FileHandler()
    print("FileHandler created")

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/getBigWigData")
def getBigWigData():
	fileName = request.args.get('name')
	chrom = request.args.get('chr')
	startIndex = int(request.args.get('start'))
	endIndex = int(request.args.get('end'))
	points = int(request.args.get('points')) if request.args.get('points') != None else 2000
	result = ph.handleBigWig(fileName, chrom, startIndex, endIndex, points)
	return str(result)

@app.route("/getBigBedData")
def getBigBedData():
	fileName = request.args.get('name')
	chrom = request.args.get('chr')
	startIndex = int(request.args.get('start'))
	endIndex = int(request.args.get('end'))
	result = ph.handleBigBed(fileName, chrom, startIndex, endIndex)
	return str(result)

@app.route("/manager")
def printManager():
	return ph.printManager()