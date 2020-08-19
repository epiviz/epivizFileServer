from dask.distributed import Client
import os

# change address where dask-scheduler is running and the worker to remove!
c = Client("tcp://127.0.0.1:35263")
worker = "tcp://127.0.0.1:32923"
c.run(lambda: os._exit(0), workers=[worker])
