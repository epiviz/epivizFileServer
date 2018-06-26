# from flask import Flask, request, g
# 
# app = Flask(__name__)

# @app.before_first_request
# def startup():
#     g.ph = FileHandler()
#     print("FileHandler created")

# @app.route("/")
# def hello():
#     return "Hello World!"

# @app.route("/getBigWigData")
# def getBigWigData():
# 	fileName = request.args.get('name')
# 	chrom = request.args.get('chr')
# 	startIndex = int(request.args.get('start'))
# 	endIndex = int(request.args.get('end'))
# 	points = int(request.args.get('points')) if request.args.get('points') != None else 2000
# 	result = g.ph.handleBigWig(fileName, chrom, startIndex, endIndex, points)
	
# 	return str(result)
# 	# return str(fileName, chrom, startIndex, endIndex, points)
# 	# return request.query_string

# @app.route("/getBigBedData")
# def getBigWigData():
# 	fileName = request.args.get('name')
# 	chrom = request.args.get('chr')
# 	startIndex = int(request.args.get('start'))
# 	endIndex = int(request.args.get('end'))
# 	result = g.ph.handleBigBed(fileName, chrom, startIndex, endIndex)
	
# 	return str(result)
# 	# return str(fileName, chrom, startIndex, endIndex, points)
# 	# return request.query_string

from sanic import Sanic
from sanic.response import json
from handler import FileHandler

app = Sanic()

@app.route("/")
async def test(request):
    return json({"hello": "world"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)