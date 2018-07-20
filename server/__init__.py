from sanic import Sanic, Blueprint, response
from sanic.response import json
from handler import FileHandlerProcess
import asyncio
from ujson import dumps
from server.request import create_request
# from aiocache import caches, cached

app = Sanic()
ph = None
fileTime = 900 # s
recordTime = 1500 # s
MAXWORKER = 10

@app.listener('before_server_start')
async def setup_connection(app, loop):
    global ph
    ph = FileHandlerProcess(fileTime, recordTime, MAXWORKER)
    print("FileHandler created")
    print('Server successfully started!')

@app.route("/", methods=["POST", "OPTIONS", "GET"])
async def process_request(request):
    """
        routes the request to the appropriate function based on the request `action` parameter.

        Returns:
            JSON result
    """

    if request.method == "OPTIONS":
        res = dumps({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = '*'
        return res

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
