"""
Helper functions for querying REST API data for flows
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_flow_fields(conn) -> list:
    """
    Queries all available flow fields from Tableau Server.
    :param class conn: the Tableau Server connection
    :return: list
    """
    all_flows = extract_pages(conn.query_flows_for_site, parameter_dict={'fields': 'fields=_default_'})
    return all_flows


def get_flows_dataframe(conn) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame of all available Tableau Server flow fields.
    :param class conn: the Tableau Server connection
    :return: pd.DataFrame
    """
    flows_df = pd.DataFrame(get_all_flow_fields(conn))
    return flows_df
