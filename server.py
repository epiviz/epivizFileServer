from sanic import Sanic, Blueprint, response
from sanic.response import json
from handler import FileHandlerProcess
import asyncio
app = Sanic()
ph = None
# the length of time that server schedules a file obj pickle
fileTime = 900 # s
# the length of time that server cleans unused records from db
recordTime = 1500 # s
MAXWORKER = 10
async def schedulePickle():
    while True:
        await asyncio.sleep(fileTime)
        cleanQue = ph.cleanFileOBJ()
        for stuff in cleanQue:
            asyncio.ensure_future(stuff)
        print('Server scheduled file OBJ cleaning!')

async def cleanDB():
    while True:
        await asyncio.sleep(recordTime)
        asyncio.ensure_future(ph.cleanDbRecord())
        print('Server scheduled DB cleaning!')

@app.listener('before_server_start')
async def setup_connection(app, loop):
    global ph
    ph = FileHandlerProcess(fileTime, recordTime, MAXWORKER)
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

app.add_task(schedulePickle())
app.add_task(cleanDB())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
