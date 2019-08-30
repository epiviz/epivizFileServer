from epivizfileserver import setup_app, create_fileHandler, MeasurementManager
import os
import numpy
import pickle

if __name__ == "__main__":
    # create measurements manager
    mMgr = MeasurementManager()

    # create file handler
    mHandler = create_fileHandler()

    rfile = open(os.getcwd() + "/roadmap.pickle", "rb")
    roadmap = pickle.load(rfile)

    # import measurements from json
    roadmap = mMgr.import_ahub(roadmap, mHandler)

    #filter measurements for "H3k4me3"
    froadmap = out = [m for m in roadmap if m.name.find("H3K4me2") != -1 and m.name.find("fc") != -1 ]

    # number of files
    len(froadmap)

    # add a computed measurement
    computed_measurement = mMgr.add_computed_measurement("computed", "Average_H3K4me2_Expression", "Average H3K4me2 Expression",
                                    measurements=froadmap, computeFunc=numpy.mean)

    app = setup_app(mMgr)
    app.run(host="0.0.0.0", port=8000)
    