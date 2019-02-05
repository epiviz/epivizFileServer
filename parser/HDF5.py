import h5py

class HDF5(Object):

	def init(self, file):
		# self.f = h5py.File(file, 'r')
		self.f= h5py.File('/Users/evan/Downloads/filtered_feature_bc_matrix.h5', 'r')

	def read_10x(self, chr, query_names):
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
			result[query] = self.matrix[index, :]

		return result