# -*- coding: utf-8 -*-
from pkg_resources import get_distribution, DistributionNotFound
from .handler import FileHandlerProcess
from .measurements import DbMeasurement, FileMeasurement, ComputedMeasurement, MeasurementManager
from .parser import BaseFile, BigBed, BigWig, SamFile, BamFile, TbxFile, GtfFile, HDF5File, GtfParsedFile
from .server import setup_app, create_fileHandler
from .trackhub import TrackHub

try:
    # Change here if project is renamed and does not equal the package name
    dist_name = __name__
    __version__ = get_distribution(dist_name).version
except DistributionNotFound:
    __version__ = 'unknown'
finally:
    del get_distribution, DistributionNotFound
