from .utils import toDataFrame
# returns columns, result, None?
def get_range_helper(toDF, get_bin, get_col_names, chr, start, end, file_iter, columns, respType):
    result = []
    for x in file_iter:
        cols = get_bin(x)
        result.append(cols)

    if get_col_names is not None:
        columns = get_col_names(result[0])
    else:
        columns = None

    if respType is "DataFrame":
        result = toDataFrame(result, columns)
    return result, None
