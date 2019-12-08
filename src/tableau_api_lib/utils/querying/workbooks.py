"""
Helper functions for querying REST API data for workbooks and views
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_workbook_fields(conn):
    all_workbooks = extract_pages(conn.query_workbooks_for_site, parameter_dict={'fields': 'fields=_all_'})
    return all_workbooks


def get_workbooks_dataframe(conn):
    workbooks_df = pd.DataFrame(get_all_workbook_fields(conn))
    return workbooks_df


def get_all_view_fields(conn):
    all_workbooks = extract_pages(conn.query_views_for_site, parameter_dict={'fields': 'fields=_all_'})
    return all_workbooks


def get_views_dataframe(conn):
    views_df = pd.DataFrame(get_all_view_fields(conn))
    return views_df
