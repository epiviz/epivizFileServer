from .utils import toDataFrame
# returns columns, result, None?
def get_range_helper(get_bin, get_col_names, chr, start, end, file_iter, columns, respType):
    result = []
    for x in file_iter:
        print(x)
        cols = get_bin(x)
        result.append(cols)

    print(result)

    columns = get_col_names(columns, result[0])

    if respType is "DataFrame":
        result = toDataFrame(result, columns)
    return columns, result, None
