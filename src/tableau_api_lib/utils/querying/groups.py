"""This module defines helper functions for querying REST API data for groups."""

from typing import Any, Dict, List, Union

import pandas as pd
from typeguard import typechecked

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import extract_pages
from tableau_api_lib.utils.querying import get_users_dataframe


@typechecked
def get_all_group_fields(conn: TableauServerConnection) -> List[Dict[str, Any]]:
    """Returns details for all groups in the Tableau Server environment, including all queryable fields."""
    all_groups = extract_pages(conn.query_groups, parameter_dict={"fields": "fields=_default_"})
    return all_groups


@typechecked
def get_group_users(conn: TableauServerConnection, group_id: str) -> List[Dict[str, Any]]:
    """Returns details of users belonging to the specified Tableau group."""
    all_group_users = extract_pages(
        conn.get_users_in_group, content_id=group_id, parameter_dict={"fields": "fields=_default_"}
    )
    return all_group_users


@typechecked
def get_all_group_names(conn: TableauServerConnection) -> List[str]:
    """Returns a list of all groups available in the Tableau Server environment."""
    all_groups = get_all_group_fields(conn)
    all_groupnames = [group["name"] for group in all_groups]
    return all_groupnames


@typechecked
def get_all_group_domain_names(conn: TableauServerConnection) -> List[str]:
    """Returns a list of domain names for all groups in the Tableau Server environment."""
    all_groups = get_all_group_fields(conn)
    all_group_roles = [group["domain"]["name"] for group in all_groups]
    return all_group_roles


@typechecked
def get_groups_dataframe(conn: TableauServerConnection) -> pd.DataFrame:
    """Returns a Pandas DataFrame containing details for all groups on the active site."""
    groups_df = pd.DataFrame(get_all_group_fields(conn))
    groups_df["domain"] = groups_df["domain"].apply(lambda x: x["name"])
    return groups_df


@typechecked
def get_group_users_dataframe(conn: TableauServerConnection, group_id: str) -> pd.DataFrame:
    """Returns a Pandas DataFrame describing all users belonging to the specified Tableau group."""
    group_users_df = pd.DataFrame(get_group_users(conn, group_id))
    users_df = get_users_dataframe(conn)
    users_columns = list(users_df.columns)
    if group_users_df.empty is True:
        return group_users_df
    else:
        all_group_users_df = group_users_df.merge(
            users_df, how="left", left_on="id", right_on="id", suffixes=("delete", None)
        )
        return all_group_users_df[users_columns]


@typechecked
def get_groups_for_a_user_dataframe(conn: TableauServerConnection, user_id: str) -> pd.DataFrame:
    """Returns a Pandas DataFrame containing all groups a user belongs to."""
    groups_for_user_df = pd.DataFrame(extract_pages(conn.get_groups_for_a_user, content_id=user_id))
    return groups_for_user_df
