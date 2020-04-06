from sanic import Sanic, Blueprint, response
from sanic.response import json
from ..handler import FileHandlerProcess
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

