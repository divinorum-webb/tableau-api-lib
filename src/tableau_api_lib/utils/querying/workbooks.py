"""
Helper functions for querying REST API data for workbooks and views
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages, flatten_dict_column
from tableau_api_lib.utils.querying.sites import get_active_site_id
from tableau_api_lib.exceptions.tableau_server_exceptions import ContentNotFound


def get_all_workbook_fields(conn):
    all_workbooks = extract_pages(conn.query_workbooks_for_site, parameter_dict={'fields': 'fields=_all_'})
    return all_workbooks


def get_workbooks_dataframe(conn):
    workbooks_df = pd.DataFrame(get_all_workbook_fields(conn))
    return workbooks_df


def get_all_view_fields(conn, site_id):
    all_views = extract_pages(conn.query_views_for_site, content_id=site_id, parameter_dict={'fields': 'fields=_all_'})
    return all_views


def get_views_dataframe(conn, site_id=None):
    if not site_id:
        site_id = conn.site_id
    views_df = pd.DataFrame(get_all_view_fields(conn, site_id))
    views_df = flatten_dict_column(views_df, keys=['totalViewCount'], col_name='usage')
    return views_df


def extract_datasource_details(df, keys):
    for key in keys:
        df['datasource_' + key] = df['datasource'].apply(lambda project: project[key])
    df.drop(columns=['datasource'], inplace=True)
    return df


def get_workbook_connections_dataframe(conn, workbook_id):
    try:
        connections_json = conn.query_workbook_connections(workbook_id).json()['connections']['connection']
        connections_df = pd.DataFrame(connections_json)
        connections_df = flatten_dict_column(connections_df, keys=['id', 'name'], col_name='datasource')
        return connections_df
    except KeyError:
        raise ContentNotFound('workbook', workbook_id)


def get_embedded_datasources_dataframe(conn, workbooks_df, workbook_ids=None):
    """
    Creates a Pandas DataFrame of all embedded workbook datasources, or specific workbooks if specified.
    :param TableauServerConnection conn: the Tableau Server connection
    :param pd.DataFrame workbooks_df: the workbook DataFrame containing details for all workbooks
    :param list workbook_ids: a list of workbook IDs whose embedded datasources will be queried
    :return:
    """
    workbook_ids = workbook_ids.to_list() if isinstance(workbook_ids, pd.core.series.Series) else workbook_ids
    workbook_ids = workbook_ids or []
    if any(workbook_ids):
        workbooks_df = workbooks_df[workbooks_df['id'].isin(workbook_ids)]
    embedded_datasources_df = pd.DataFrame()
    for index, workbook in workbooks_df.iterrows():
        workbook_connections_df = get_workbook_connections_dataframe(conn=conn, workbook_id=workbook['id'])
        workbook_connections_df['workbook_name'] = workbook['name']
        workbook_connections_df['site_name'] = conn.site_name
        embedded_datasources_df = embedded_datasources_df.append(workbook_connections_df, ignore_index=True, sort=True)
    return embedded_datasources_df
