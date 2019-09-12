#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
import sys
import os

from epivizfileserver.measurements import MeasurementManager, FileMeasurement
from epivizfileserver.server import create_fileHandler

__author__ = "Jayaram Kancherla"
__copyright__ = "Jayaram Kancherla"
__license__ = "mit"


# Data location
# data_path = sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '/data')))
data_path = os.getcwd() + "/tests/data"
mMgr = MeasurementManager()
mHandler = create_fileHandler()

file_config = [
    {
        "url": data_path + "test.bw",
        "file_type": "bigwig",
        "datatype": "bp",
        "name": "test-bw",
        "id": "test-bw",
        "annotation": {
            "file_type": "bigwig"
        },
        "metadata": []
    }, {
        "url": data_path + "test.bigBed",
        "file_type": "bigbed",
        "datatype": "bp",
        "name": "test-bb",
        "id": "test-bb",
        "annotation": {
            "file_type": "bigbed"
        },
        "metadata": []
    }
]

# mts = mMgr.import_files(os.getcwd() + "/files.json", mHandler)

for rec in file_config:
    isGene = False
    if "annotation" in rec["datatype"]:
        isGene = True
    tempFileM = FileMeasurement(rec.get("file_type"), rec.get("id"), rec.get("name"), 
                    rec.get("url"), annotation=rec.get("annotation"),
                    metadata=rec.get("metadata"), minValue=0, maxValue=5,
                    isGenes=isGene, fileHandler=mHandler
                )
    mMgr.measurements.append(tempFileM)

def test_bigwig_measurement():
    assert mMgr.measurements[0].get_data("1", 1, 1000)

def test_bigbed_measurement():
    assert mMgr.measurements[1].get_data("chr1", 1, 1000)
