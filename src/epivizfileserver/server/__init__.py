from sanic import Sanic, response
from sanic.log import logger as logging
from ..handler import FileHandlerProcess
# import asyncio
import ujson
from .request import create_request, StatusRequest, UpdateCollectionsRequest
from sanic_cors import CORS, cross_origin
import os
import sys
import asyncio
from tornado.platform.asyncio import BaseAsyncIOLoop, to_asyncio_future
from dask.distributed import Client
# import logging
import time
import traceback

app = Sanic(__name__)
CORS(app)
fileTime = 4
MAXWORKER = 10
# logger = logging.getLogger(__name__)

# logging.basicConfig(filename= os.getcwd() + 'efs.log', 
#                 format='%(asctime)s\t%(name)s\t%(levelname)s\t%(message)s',
#                 level=logging.DEBUG)

# for handler in logging.root.handlers[:]:
#     logging.root.removeHandler(handler)

"""
The server module allows users to instantly create a REST API from the list of measuremensts.
The API can then be used to interactive exploration of data or build various applications.
"""

def setup_app(measurementsManager, dask_scheduler = None):
    """Setup the Sanic Rest API

    Args: 
        measurementsManager: a measurements manager object

    Returns:
        a sanic app object
    """
    print("This is a testpip")
    global app
    app.epivizMeasurementsManager = measurementsManager
    app.epivizFileHandler = None
    app.dask_scheduler = dask_scheduler
    logging.info("Initialized Setup App")
    # traceback.print_stack()
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
        logging.info("Updating cache, pickle file objects")

@app.listener('before_server_start')
async def setup_connection(app, loop):
    """Sanic callback for app setup before the server starts
    """
    # app.epivizFileHandler = FileHandlerProcess(fileTime, MAXWORKER)
    # for rec in app.epivizMeasurementsManager.get_measurements():
    #     if rec.datasource == "files" or rec.datasource == "computed":
    #         rec.fileHandler = app.epivizFileHandler
    logging.info('Server successfully started!')
    # also create a cache folder
    if not os.path.exists(os.getcwd() + "/cache"):
        os.mkdir('cache')

@app.listener('after_server_start')
async def setup_after_connection(app, loop):
    logging.info("after server start")
    # configure tornado use asyncio's loop
    ioloop = BaseAsyncIOLoop(loop)
    logging.info("after ioloop")
    # ioloop = asyncio.get_running_loop()

    # init distributed client
    # cluster = LocalCluster(asynchronous=True, scheduler_port=8786, nanny=False, n_workers=2, 
    #         threads_per_worker=1)
    logging.info("setting up dask client with scheduler", app.dask_scheduler)
    if app.dask_scheduler is None:
        app.client = await Client(asynchronous=True, nanny=False, loop=ioloop)
    else:
        app.client = await Client(address = app.dask_scheduler, asynchronous=True)
        
    print(app.client)
    logging.info("setup client")
    app.epivizFileHandler = FileHandlerProcess(fileTime, MAXWORKER)
    app.epivizFileHandler.client = app.client
    for rec in app.epivizMeasurementsManager.get_measurements():
        if rec.datasource == "files" or rec.datasource == "computed":
            rec.fileHandler = app.epivizFileHandler
    logging.info("FileHandler created")
    logging.info("starting client")
    # await to_asyncio_future(app.client._start())

@app.listener('before_server_stop')
async def clean_up(app, loop):
    folder = os.getcwd() + "/cache/"
    file_path = None

    logging.info("cache cleaned")

    for the_file in os.listdir(folder):
        file_path = os.path.join(folder, the_file)
    try:
        if os.path.isfile(file_path):
            os.unlink(file_path)
        #elif os.path.isdir(file_path): shutil.rmtree(file_path)
        # await to_asyncio_future(app.client._shutdown())
        await app.client.close()

    except Exception as e:
        print(e)
        # await to_asyncio_future(app.client._shutdown())

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

    logging.debug("Request received: %s" %(request.args))
    start = time.time()
    epiviz_request = create_request(param_action, request.args)
    result, error = await epiviz_request.get_data(request.app.epivizMeasurementsManager)
    logging.debug("Request total time: %s" %(time.time() - start))
    logging.debug("Request processed: %s" %(param_id))

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

    return response.json({"requestId": int(param_id),
                            "type": "response",
                            "error": None,
                            "data": True,
                            "version": 5
                        },
                    status=200)


@app.route("/status", methods=["GET"])
async def process_request(request):
    # response = {
    #     "requestId": -1, 
    #     "type": "response",
    #     "error": None,
    #     "version": 5,
    #     "data": {
    #         "message": "EFS up",
    #         "stats": request.app.epivizMeasurementsManager.stats
    #     }
    # }

    return response.json({
        "requestId": -1, 
        "type": "response",
        "error": None,
        "version": 5,
        "data": {
            "message": "EFS up",
            "stats": request.app.epivizMeasurementsManager.stats
        }
    }, status=200)

@app.route("/status/<datasource>", methods=["GET"])
async def process_request(request, datasource):
    epiviz_request = StatusRequest(request, datasource)
    result, error = await epiviz_request.get_status(request.app.epivizMeasurementsManager)

    result = "ok: {} bytes read".format(result) if result > 0 else "fail"

    res = {
        "requestId": -1,
        "type": "response",
        "error": error,
        "version": 5,
        "data": {
            "message": "check status of datasource " + datasource + ": " + result
        }
    }

    if datasource in request.app.epivizMeasurementsManager.stats["getRows"]:
        data = request.app.epivizMeasurementsManager.stats["getRows"][datasource]
        res["data"]["getRows"] = data
        mean = data["sum"] / data["count"]
        res["data"]["SD"] = (data["sumSquares"] + (data["count"] * (mean ** 2)) - 2 * mean * data["sum"])/data["count"]

    if datasource in request.app.epivizMeasurementsManager.stats["getValues"]:
        data = request.app.epivizMeasurementsManager.stats["getValues"][datasource]
        res["data"]["getValues"] = data
        mean = data["sum"] / data["count"]
        res["data"]["SD"] = (data["sumSquares"] + (data["count"] * (mean ** 2)) - 2 * mean * data["sum"])/data["count"]
    
    if datasource in request.app.epivizMeasurementsManager.stats["search"]:
        data = request.app.epivizMeasurementsManager.stats["search"][datasource]
        res["data"]["search"] = data
        mean = data["sum"] / data["count"]
        res["data"]["SD"] = (data["sumSquares"] + (data["count"] * (mean ** 2)) - 2 * mean * data["sum"])/data["count"]

    return response.json(res, status=200)

@app.route("/updateCollections", methods=["POST"])
async def process_request(request):
    epiviz_request = UpdateCollectionsRequest(request)
    result, error = await epiviz_request.update_collections(request.app.epivizMeasurementsManager, request.app.epivizFileHandler)
    status_code = 201 if len(error) == 0 else 501
        
    return response.json({
            "requestId": -1,
            "type": "response",
            "error": error,
            "version": 5,
            "data": result},
        status = status_code)
