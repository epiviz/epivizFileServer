from sanic import Sanic, Blueprint, response
from sanic.response import json
from handler import FileHandlerProcess
import asyncio
import ujson
from server.request import create_request
# from aiocache import caches, cached
from sanic_cors import CORS, cross_origin
import sys


app = Sanic()
CORS(app)
ph = None
fileTime = 1 # s
MAXWORKER = 10

async def schedulePickle():
    while True:
        await asyncio.sleep(fileTime)
        cleanQue = ph.cleanFileOBJ()
        for stuff in cleanQue:
            asyncio.ensure_future(stuff)
        print('Server scheduled file OBJ cleaning!')

@app.listener('before_server_start')
async def setup_connection(app, loop):
    global ph
    ph = FileHandlerProcess(fileTime, MAXWORKER)
    print("FileHandler created")
    print('Server successfully started!')

@app.route("/", methods=["POST", "OPTIONS", "GET"])
async def process_request(request):
    """
        routes the request to the appropriate function based on the request `action` parameter.

        Returns:
            JSON result
    """

    param_action = request.args.get("action")
    param_id = request.args.get("requestId")
    version = request.args.get("version")

    epiviz_request = create_request(param_action, request.args)
    result, error = await epiviz_request.get_data(ph)

    return response.json({"requestId": int(param_id),
                            "type": "response",
                            "error": error,
                            "data": result,
                            "version": 5
                        },
                    status=200)

# @app.listener('before_server_stop')
# async def clean_tasks(app, loop):
#     for task in asyncio.Task.all_tasks():
#         task.cancel()

app.add_task(schedulePickle())