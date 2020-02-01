import urllib


def flatten_dict_column(df, keys, col_name):
    for key in keys:
        df[col_name + '_' + key] = df[col_name].apply(lambda col: col[key])
    df.drop(columns=[col_name], inplace=True)
    return df


def get_server_netloc(server_address):
    return urllib.parse.urlparse(server_address).netloc
