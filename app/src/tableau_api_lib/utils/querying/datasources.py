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
    all_datasources = extract_pages(conn.query_data_sources, parameter_dict={'fields': 'fields=_default_'})
    return all_datasources


def get_datasources_dataframe(conn, datasource_names=None) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame of all available Tableau Server datasource fields.
    :param class conn: the Tableau Server connection
    :param list datasource_names: a list of datasource names to filter the results by
    :return: pd.DataFrame
    """
    datasources_df = pd.DataFrame(get_all_datasource_fields(conn))
    if datasource_names:
        datasources_df = datasources_df[datasources_df['name'].isin(datasource_names)]
    return datasources_df


def get_datasource_connections_dataframe(conn, datasource_id) -> pd.DataFrame:
    try:
        datasource_connections_json = conn.query_data_source_connections(datasource_id).json()['connections']['connection']
    except KeyError:
        datasource_connections_json = {}
    datasource_connections_df = pd.DataFrame(datasource_connections_json)
    return datasource_connections_df
