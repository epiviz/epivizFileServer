from sanic import Sanic, Blueprint, response
from sanic.response import json
from ..handler import FileHandlerProcess
from ..trackhub import TrackHub
# import asyncio
import ujson
from .request import create_request
from sanic_cors import CORS, cross_origin
import os
import sys

app = Sanic()
CORS(app)
fileTime = 4
MAXWORKER = 10

"""
The server module allows users to instantly create a REST API from the list of measuremensts.
The API can then be used to interactive exploration of data or build various applications.
"""

def setup_app(measurementsManager):
    """Setup the Sanic Rest API

    Args: 
        measurementsManager: a measurements manager object

    Returns:
        a sanic app object
    """
    global app
    app.epivizMeasurementsManager = measurementsManager
    app.epivizFileHandler = None
    return app

def create_fileHandler():
    """create a dask file handler if one doesn't exist
    """
    global app
    app.epivizFileHandler = None
    return app.epivizFileHandler

async def schedulePickle():
    """Sanic task to regularly pickle file objects from memory
    """
    while True:
        await asyncio.sleep(fileTime)
        cleanQue = app.epivizFileHandler.cleanFileOBJ()
        for stuff in cleanQue:
            asyncio.ensure_future(stuff)
        print('Server scheduled file OBJ cleaning!')

@app.listener('before_server_start')
async def setup_connection(app, loop):
    """Sanic callback for app setup before the server starts
    """
    app.epivizFileHandler = FileHandlerProcess(fileTime, MAXWORKER)
    for rec in app.epivizMeasurementsManager.get_measurements():
        if rec.datasource == "files" or rec.datasource == "computed":
            rec.fileHandler = app.epivizFileHandler
    print("FileHandler created")
    print('Server successfully started!')
    # also create a folder caled cache
    if not os.path.exists(os.getcwd() + "/cache"):
        os.mkdir('cache')

@app.listener('before_server_stop')
def clean_up(app, loop):
    folder = os.getcwd() + "/cache/"
    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
        #elif os.path.isdir(file_path): shutil.rmtree(file_path)
    except Exception as e:
        print(e)
    print("cache cleaned")

# async def clean_tasks(app, loop):
#     for task in asyncio.Task.all_tasks():
#         task.cancel()
# app.add_task(schedulePickle())

@app.route("/", methods=["POST", "OPTIONS", "GET"])
async def process_request(request):
    """
    Process am API request

    Args: 
        request: a sanic request object

    Returns:
        a JSON result
    """

    param_action = request.args.get("action")
    param_id = request.args.get("requestId")
    version = request.args.get("version")

    epiviz_request = create_request(param_action, request.args)
    result, error = await epiviz_request.get_data(request.app.epivizMeasurementsManager)
    # return response.raw(umsgpack.packb({"requestId": int(param_id),
    #                         "type": "response",
    #                         "error": error,
    #                         "data": result,
    #                         "version": 5
    #                     }),
    #                 status=200)
    return response.json({"requestId": int(param_id),
                            "type": "response",
                            "error": error,
                            "data": result,
                            "version": 5
                        },
                    status=200)

@app.route("/addData", methods=["POST", "OPTIONS", "GET"])
async def process_request(request):
    """
    API Endpoint to add new datasets to an instance

    API Params:
        file: location of the json or hub file
        filetype: 'hub' if trackhub or 'json' if configuration file

    Args: 
        request: a sanic request object

    Returns:
        success/fail after adding measurements
    """

    file = request.args.get("file")
    type = request.args.get("filetype")

    if type is "json":
        request.app.epivizMeasurementsManager.import_files(file, request.app.epivizFileHandler)
    elif type is "hub":
        request.app.epivizMeasurementsManager.import_trackhub(file, request.app.epivizFileHandler)

    return response.raw(umsgpack.packb({"requestId": int(param_id),
                            "type": "response",
                            "error": None,
                            "data": True,
                            "version": 5
                        }),
                    status=200)

@app.route("/file", methods=["POST", "OPTIONS", "GET"])
async def process_file_request(request):
    """
    Process an API request

    Args: 
        request: a sanic request object

    Returns:
        a JSON result
    """

    param_action = request.args.get("action")
    param_id = request.args.get("requestId")
    version = request.args.get("version")

    result = None
    error = None

    if param_action == "getSeqInfos":
        result = {"hg18":[["chr1",1,247249719],["chr2",1,242951149],["chr3",1,199501827],["chr4",1,191273063],["chr5",1,180857866],["chr6",1,170899992],["chr7",1,158821424],["chr8",1,146274826],["chr9",1,140273252],["chr10",1,135374737],["chr11",1,134452384],["chr12",1,132349534],["chr13",1,114142980],["chr14",1,106368585],["chr15",1,100338915],["chr16",1,88827254],["chr17",1,78774742],["chr18",1,76117153],["chr19",1,63811651],["chrX",1,154913754],["chrY",1,57772954]],"hg19":[["chr1",1,249250621],["chr2",1,243199373],["chr3",1,198022430],["chr4",1,191154276],["chr5",1,180915260],["chr6",1,171115067],["chr7",1,159138663],["chr8",1,146364022],["chr9",1,141213431],["chr10",1,135534747],["chr11",1,135006516],["chr12",1,133851895],["chr13",1,115169878],["chr14",1,107349540],["chr15",1,102531392],["chr16",1,90354753],["chr17",1,81195210],["chr18",1,78077248],["chr19",1,59128983],["chrX",1,155270560],["chrY",1,59373566]],"mm9":[["chr1",1,197195432],["chr2",1,181748087],["chr3",1,159599783],["chr4",1,155630120],["chr5",1,152537259],["chr6",1,149517037],["chr7",1,152524553],["chr8",1,131738871],["chr9",1,124076172],["chr10",1,129993255],["chr11",1,121843856],["chr12",1,121257530],["chr13",1,120284312],["chr14",1,125194864],["chr15",1,103494974],["chr16",1,98319150],["chr17",1,95272651],["chr18",1,90772031],["chr19",1,61342430],["chrX",1,166650296],["chrY",1,15902555]],"mm10":[["chr1",1,195471971],["chr2",1,182113224],["chr3",1,160039680],["chr4",1,156508116],["chr5",1,151834684],["chr6",1,149736546],["chr7",1,145441459],["chr8",1,129401213],["chr9",1,124595110],["chr10",1,130694993],["chr11",1,122082543],["chr12",1,120129022],["chr13",1,120421639],["chr14",1,124902244],["chr15",1,104043685],["chr16",1,98207768],["chr17",1,94987271],["chr18",1,90702639],["chr19",1,61431566],["chrX",1,171031299],["chrY",1,91744698]]}
    elif param_action == "getMeasurements":
        result = {
            "annotation": [],
            "datasourceGroup": [],
            "datasourceId": [],
            "defaultChartType": [],
            "id": [],
            "maxValue": [],
            "minValue": [],
            "name": [],
            "type": [],
            "metadata": []
        } 
    elif param_action in ["getData", "getCombined", "getRows", "getValues"]:
        param_action = "fileQuery"
        epiviz_request = create_request(param_action, request.args)
        result, error = await epiviz_request.get_data(request.app.epivizFileHandler)
    
    return response.json({"requestId": int(param_id),
                            "type": "response",
                            "error": error,
                            "data": result,
                            "version": 5
                        },
                    status=200)

@app.route("/trackhub", methods=["POST", "OPTIONS", "GET"])
async def process_trackhub_request(request):
    """
    Process an API request

    Args: 
        request: a sanic request object

    Returns:
        a JSON result
    """

    hub_url = request.args.get("hub")
    trackhub = TrackHub(hub_url)
    measurements = []


    # self.mtype = mtype      # measurement_type (file/db)
    # self.mid = mid          # measurement_id (column name in db/file)
    # self.name = name        # measurement_name
    # self.source = source    # tbl name / file location
    # self.datasource = datasource # dbname / "files"
    # self.annotation = annotation
    # self.metadata = metadata
    # self.isComputed = isComputed
    # self.isGenes = isGenes
    # self.minValue = minValue
    # self.maxValue = maxValue
    # self.columns = columns

    for ms in trackhub.measurements: 
        measurements.append({
            "annotation": ms.annotation,
            "datasourceGroup": ms.datasource,
            "datasourceId": ms.source,
            "defaultChartType": "scatter-plot",
            "id": ms.mid,
            "maxValue": 0,
            "minValue": 5,
            "name": ms.name,
            "type": "annotation" if ms.isGenes else "bp",
            "metadata": ms.metadata
        })

    return response.json({
                            "type": "response",
                            "error": None,
                            "data": measurements,
                            "version": 5
                        },
                    status=200)
