import urllib


def flatten_dict_column(df, keys, col_name):
    """
    Extract the specified Pandas DataFrame dict column ('col_name'), creating new columns from the given keys.
    :param pd.DataFrame df: the Pandas DataFrame whose dict column to be flattened
    :param list keys: the keys of the dict column which will be extracted into new columns
    :param str col_name: the name of the Pandas DataFrame column whose content will be flattened
    :return: pd.DataFrame
    """
    for key in keys:
        df[col_name + '_' + key] = df[col_name].apply(lambda col: col[key])
    df.drop(columns=[col_name], inplace=True)
    return df


def get_server_netloc(server_address):
    return urllib.parse.urlparse(server_address).netloc
