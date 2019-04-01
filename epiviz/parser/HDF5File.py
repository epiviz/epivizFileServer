import h5py
from scipy.sparse import csc_matrix
import numpy as np

class HDF5File(object):

    def __init__(self, file):
        self.f = h5py.File(file, 'r')

    # both chr and query_names are of type array
    def read_10x_hdf5(self, chr, query_names):
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
        pass