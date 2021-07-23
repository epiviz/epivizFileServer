#!/usr/bin/env python
# -*- coding: utf-8 -*-

import pytest
from epivizFileParser.BigBed import BigBed
from epivizFileParser.BigWig import BigWig
import sys
import os

__author__ = "Jayaram Kancherla"
__copyright__ = "Jayaram Kancherla"
__license__ = "mit"


# Data location
# data_path = sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '/data')))
data_path = os.getcwd() + "/tests/data"

def test_bigwig():
    file = BigWig(data_path + "/test.bw")
    assert file.getRange("1", 1, 1000)

def test_bigbed():
    file = BigBed(data_path + "/test.bigBed")
    assert file.getRange("chr1", 1, 1000)
