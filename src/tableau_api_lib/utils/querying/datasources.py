"""
Helper functions for querying REST API data for datasources
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_datasource_fields(conn) -> list:
    """
    Queries all available datasource fields from Tableau Server.
    :param class conn: the Tableau Server connection
    :return: list
    """
    all_datasources = extract_pages(conn.query_data_sources, parameter_dict={'fields': 'fields=_all_'})
    return all_datasources


def get_datasources_dataframe(conn) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame of all available Tableau Server datasource fields.
    :param class conn: the Tableau Server connection
    :return: pd.DataFrame
    """
    datasources_df = pd.DataFrame(get_all_datasource_fields(conn))
    return datasources_df


def get_datasource_connections_dataframe(conn, datasource_id) -> pd.DataFrame:
    datasource_connections_json = conn.query_data_source_connections(datasource_id).json()['connections']['connection']
    datasource_connections_df = pd.DataFrame(datasource_connections_json)
    return datasource_connections_df
