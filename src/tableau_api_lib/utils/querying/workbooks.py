"""
Helper functions for querying REST API data for workbooks and views
"""


from io import StringIO
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.exceptions.tableau_server_exceptions import ContentNotFound
from tableau_api_lib.utils import extract_pages, flatten_dict_column


def get_all_workbook_fields(conn: TableauServerConnection) -> List[Dict[str, Any]]:
    """Returns a list of JSON / dicts describing all available workbooks."""
    all_workbooks = extract_pages(conn.query_workbooks_for_site, parameter_dict={"fields": "fields=_all_"})
    return all_workbooks


def get_workbooks_dataframe(conn: TableauServerConnection) -> pd.DataFrame:
    """Returns a DataFrame describing all available workbooks. If none are available, an empty DataFrame is returned"""
    try:
        workbooks_df = pd.DataFrame(get_all_workbook_fields(conn))
    except ContentNotFound:
        workbooks_df = pd.DataFrame()
    return workbooks_df


def get_all_view_fields(conn: TableauServerConnection, site_id: str) -> List[Dict[str, Any]]:
    """Returns a list of JSON / dicts describing all available views."""
    all_views = extract_pages(conn.query_views_for_site, content_id=site_id, parameter_dict={"fields": "fields=_all_"})
    return all_views


def get_views_dataframe(conn: TableauServerConnection, site_id: Optional[str] = None) -> pd.DataFrame:
    """Returns a DataFrame describing all available views. If none are available, an empty DataFrame is returned."""
    if not site_id:
        site_id = conn.site_id
    views_df = pd.DataFrame(get_all_view_fields(conn, site_id))
    if not views_df.empty:
        views_df = flatten_dict_column(views_df, keys=["totalViewCount"], col_name="usage")
    return views_df


def get_view_data_dataframe(
    conn: TableauServerConnection, view_id: str, parameter_dict: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """Returns a DataFrame containing the data downloaded from a Tableau view."""
    view_data = conn.query_view_data(view_id=view_id, parameter_dict=parameter_dict)
    view_df = pd.read_csv(StringIO(view_data.text))
    return view_df


def extract_datasource_details(df: pd.DataFrame, keys: List[str]) -> pd.DataFrame:
    """Returns a DataFrame with datasource details unpacked from their original nested column."""
    for key in keys:
        df["datasource_" + key] = df["datasource"].apply(lambda project: project[key])
    df.drop(columns=["datasource"], inplace=True)
    return df


def get_workbook_connections_dataframe(conn: TableauServerConnection, workbook_id: str) -> pd.DataFrame:
    """Returns a DataFrame describing the connections associated with the specified workbook."""
    try:
        connections_json = conn.query_workbook_connections(workbook_id).json()["connections"]["connection"]
        connections_df = pd.DataFrame(connections_json)
        connections_df = flatten_dict_column(connections_df, keys=["id", "name"], col_name="datasource")
    except KeyError:
        connections_df = pd.DataFrame()
    return connections_df


def get_embedded_datasources_dataframe(
    conn: TableauServerConnection,
    workbooks_df: pd.DataFrame,
    workbook_ids: Optional[Union[List[str], pd.Series]] = None,
    id_col: Optional[str] = "id",
    name_col: Optional[str] = "name",
    new_col_prefix: Optional[str] = "",
) -> pd.DataFrame:
    """
    Creates a Pandas DataFrame of all embedded workbook datasources.
    If a subset of workbook IDs are specified, then only embedded connections for those workbooks are returned.

    Args:
        conn: the Tableau Server connection
        workbooks_df: the workbook DataFrame containing details for all workbooks
        workbook_ids: a list of workbook IDs whose embedded datasources will be queried
        id_col: the name of the column containing the workbook ID; defaults to 'id'
        name_col: the name of the column containing the workbook name; defaults to 'name'
        new_col_prefix: the prefix that will be present in all new column names
    """
    workbook_ids = workbook_ids.to_list() if isinstance(workbook_ids, pd.Series) else workbook_ids
    workbook_ids = workbook_ids or []
    if any(workbook_ids):
        workbooks_df = workbooks_df[workbooks_df[id_col].isin(workbook_ids)]
    embedded_datasources_df = pd.DataFrame()
    for index, workbook in workbooks_df.iterrows():
        workbook_connections_df = get_workbook_connections_dataframe(conn=conn, workbook_id=workbook[id_col])
        workbook_connections_df[new_col_prefix + "workbook_name"] = workbook[name_col]
        workbook_connections_df[new_col_prefix + "workbook_id"] = workbook[id_col]
        workbook_connections_df[new_col_prefix + "site_name"] = conn.site_name
        embedded_datasources_df = embedded_datasources_df.append(workbook_connections_df, ignore_index=True, sort=True)
    return embedded_datasources_df
