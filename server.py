from flask import Flask, request, g
from handler import ProcessHandler
app = Flask(__name__)

@app.before_first_request
def startup():
    g.ph = ProcessHandler()
    print("ProcessHandler created")

@app.route("/")
def hello():
    return "Hello World!"

@app.route("/getdata")
def getBigWigData():
	fileName = request.args.get('name')
	chrom = request.args.get('chr')
	startIndex = int(request.args.get('start'))
	endIndex = int(request.args.get('end'))
	points = int(request.args.get('points')) if request.args.get('points') != None else 2000
	result = g.ph.handleBigWig(fileName, chrom, startIndex, endIndex, points)
	
	return str(result)
	# return str(fileName, chrom, startIndex, endIndex, points)
	# return request.query_string