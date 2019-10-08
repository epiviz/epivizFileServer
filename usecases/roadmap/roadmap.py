from epivizfileserver import setup_app, create_fileHandler, MeasurementManager
import os
import numpy
import pickle
import itertools

if __name__ == "__main__":
    # create measurements manager
    mMgr = MeasurementManager()

    # create file handler
    mHandler = create_fileHandler()

    # add genome
    genome = mMgr.add_genome("hg19")

    rfile = open(os.getcwd() + "/roadmap.pickle", "rb")
    roadmap = pickle.load(rfile)

    # import all roadmap datasets
    # roadmap = mMgr.import_ahub(roadmap, mHandler)

    # load all brain samples
    for b in ["E071", "E074", "E068", "E069", "E072", "E067", "E073", "E070"]:
        # filter out brain samples
        brain = roadmap[roadmap.title.str.contains(b)]
        brain = mMgr.import_ahub(brain, mHandler)

    roadmap = mMgr.measurements
    
    # roadmap metadata file is located at https://egg2.wustl.edu/roadmap/web_portal/meta.html
    # filter for all brain datasets and compute difference in histone modification 
    brain = [
        {
            "eid": "E071",
            "name": "Brain Hippocampus Middle",
        },
                {
            "eid": "E074",
            "name": "Brain Substantia Nigra",
        },
                {
            "eid": "E068",
            "name": "Brain Anterior Caudate",
        },
                {
            "eid": "E069",
            "name": "Brain Cingulate Gyrus",
        },
                {
            "eid": "E072",
            "name": "Brain Inferior Temporal Lobe",
        },
                {
            "eid": "E067",
            "name": "Brain Angular Gyrus",
        },
                {
            "eid": "E073",
            "name": "Brain_Dorsolateral_Prefrontal_Cortex",
        },
                {
            "eid": "E070",
            "name": "Brain Germinal Matrix",
        }
    ];

    markers = ["H3K27me3", "H3K36me3", "H3k4me1", "H3k4me3"]

    pairs = itertools.combinations(brain, 2)

    for (p1, p2) in pairs:
        for mkr in markers:
            froadmap = out = [m for m in roadmap if m.source in ["http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/" + p1["eid"] + "-" + mkr + ".fc.signal.bigwig", "http://egg2.wustl.edu/roadmap/data/byFileType/signal/consolidated/macs2signal/foldChange/" + p2["eid"] + "-" + mkr + ".fc.signal.bigwig"]]
            if len(froadmap) == 2:
                computed_measurement = mMgr.add_computed_measurement("computed", "Diff_" + mkr + "_Signal_" + p1["eid"] + "_" + p2["eid"] , "Diff_" + mkr + "_Signal_" + p1["eid"] + "_" + p2["eid"],
                                    measurements=froadmap, computeFunc=numpy.diff)

    app = setup_app(mMgr)
    app.run(host="0.0.0.0", port=8000)
    
