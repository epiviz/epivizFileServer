import h5py
from scipy.sparse import csc_matrix
import numpy as np

class HDF5File(object):
    """
    HDF5 File Class to parse only local hdf5 files 

    Args:
        file (str): file location can be local (full path) or hosted publicly
        columns ([str]) : column names for various columns in file
    
    Attributes:
        file: a pysam file object
        fileSrc: location of the file
        cacheData: cache of accessed data in memory
        columns: column names to use
    """
    def __init__(self, file):
        self.f = h5py.File(file, 'r')


    def read_10x_hdf5(self, chr, query_names):
        """read a 10xGenomics hdf5 file

        Args:
            chr (str): chromosome 
            query_names ([str]): genes to filter

        Returns:
            result
                a DataFrame with matched regions from the input genomic location if respType is DataFrame else result is an array
            error 
                if there was any error during the process
        """
        folder = self.f['matrix']
        self.matrix = a = csc_matrix((folder['data'][()], folder['indices'][()], folder['indptr'][()]), shape=(folder['shape'][0],folder['shape'][1]))
        genes = folder['features']['genome'][()]
        names = folder['features']['name'][()]
        # using np sorter to extract index
        sorter = np.argsort(names)
        indecis = sorter[np.searchsorted(names, query_names, sorter=sorter)]
        result = {}
        # need to handle missing query
        for query,index in zip(query_names, indecis):
            result[query] = self.matrix[index, :].toarray()

        return result


    def getRange(self, chr, start = None, end = None, row_names = None):
        """Get data for a given genomic location

        Args:
            chr (str): chromosome 
            start (int): genomic start
            end (int): genomic end
            respType (str): result format type, default is "DataFrame

        Returns:
            result
                a DataFrame with matched regions from the input genomic location if respType is DataFrame else result is an array
            error 
                if there was any error during the process
        """
        pass