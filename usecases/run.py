"""
    Entry script to start Epiviz File Server
"""

from server import setup_app, create_fileHandler
from measurements import MeasurementManager
import pymysql
import os

if __name__ == "__main__":
    # create measurements manager
    mMgr = MeasurementManager()

    # create db connectiion
    dbConn = pymysql.connect(host='localhost',
                            user='<USERNAME>',
                            password='<PASSWORD>',
                            db='<DBNAME>',
                            charset='utf8mb4',
                            cursorclass=pymysql.cursors.DictCursor)

    # import measurements from db
    mMgr.import_dbm(dbConn)

    # create file handler
    mHandler = create_fileHandler()

    # import measurements from json
    mMgr.import_files(os.getcwd() + "/files.json", mHandler)

    app = setup_app(mMgr)
    app.run(host="0.0.0.0", port=8000, workers=1)
    