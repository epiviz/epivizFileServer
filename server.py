from sanic import Sanic, Blueprint, response
from sanic.response import json
from handler import FileHandler
app = Sanic()
ph = None
bp = Blueprint('my_blueprint')
app.blueprint(bp)

@app.listener('before_server_start')
async def setup_connection(app, loop):
    global ph
    ph = FileHandler()
    print("FileHandler created")

@app.route("/")
async def test(request):
    return json({"hello": "world"})

@app.route("/getBigWigData")
def getBigWigData(request):
	fileName = request.args.get('name')
	chrom = request.args.get('chr')
	startIndex = int(request.args.get('start'))
	endIndex = int(request.args.get('end'))
	points = int(request.args.get('points')) if request.args.get('points') != None else 2000
	result = ph.handleBigWig(fileName, chrom, startIndex, endIndex, points)
	return response.text(str(result))

@app.route("/getBigBedData")
def getBigBedData(request):
	fileName = request.args.get('name')
	chrom = request.args.get('chr')
	startIndex = int(request.args.get('start'))
	endIndex = int(request.args.get('end'))
	result = ph.handleBigBed(fileName, chrom, startIndex, endIndex)
	return response.text(str(result))

@app.route("/manager")
def printManager(request):
	return response.text(ph.printManager())

@app.route("/request")
def printRequest(request):
	return response.text(str(request))


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
