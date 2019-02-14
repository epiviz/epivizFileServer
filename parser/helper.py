class Helper(object):
	"""docstring for Helper"""
	def __init__(self, arg):
		super(Helper, self).__init__()
		self.arg = arg

		# returns columns, result, None?
	def get_range_helper(get_bin, get_col_names, chr, start, end, file_iter, columns):
		result = []
        for x in file_iter:
            cols = get_bin(x)
            result.append(cols)

        columns = get_col_names(columns)

        if respType is "DataFrame":
            result = toDataFrame(result, columns)
        return columns, result, None
