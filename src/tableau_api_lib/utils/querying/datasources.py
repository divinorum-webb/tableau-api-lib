"""
Helper functions for querying REST API data for datasources
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_datasource_fields(conn):
    all_datasources = extract_pages(conn.query_data_sources, parameter_dict={'fields': 'fields=_all_'})
    return all_datasources


def get_datasources_dataframe(conn):
    datasources_df = pd.DataFrame(get_all_datasource_fields(conn))
    return datasources_df
