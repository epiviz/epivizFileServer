from sanic import Sanic, Blueprint, response
from sanic.response import json
from handler import FileHandlerProcess
import asyncio
app = Sanic()
ph = None
bp = Blueprint('my_blueprint')
app.blueprint(bp)
timePeriod = 2 # s

async def schedulePickle():
    while True:
        await asyncio.sleep(timePeriod)
        cleanQue = ph.clean()
        for stuff in cleanQue:
            asyncio.ensure_future(stuff)
        print('Server scheduled cleaning!')

@app.listener('before_server_start')
async def setup_connection(app, loop):
    global ph
    ph = FileHandlerProcess(timePeriod)
    print("FileHandler created")
    print('Server successfully started!')

# @app.listener('before_server_stop')
# async def shutdownPrep(app, loop):
#     global schedule
#     schedule.cancel()

@app.route("/")
async def test(request):
    return json({"hello": "world"})

@app.route("/getBigWigData")
async def getBigWigData(request):
    fileName = request.args.get('name')
    chrom = request.args.get('chr')
    startIndex = int(request.args.get('start'))
    endIndex = int(request.args.get('end'))
    points = int(request.args.get('points')) if request.args.get('points') != None else 2000
    result = await ph.handleBigWig(fileName, chrom, startIndex, endIndex, points)
    return response.text(str(result))

@app.route("/getBigBedData")
async def getBigBedData(request):
    fileName = request.args.get('name')
    chrom = request.args.get('chr')
    startIndex = int(request.args.get('start'))
    endIndex = int(request.args.get('end'))
    result = await ph.handleBigBed(fileName, chrom, startIndex, endIndex)
    return response.text(str(result))

@app.route("/records")
def printRecords(request):
    return response.text(ph.printRecords())

@app.route("/request")
def printRequest(request):
    return response.text(str(request))

schedule = schedulePickle()
app.add_task(schedule)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
