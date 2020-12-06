"""
Helper functions for querying REST API data for groups
"""


import pandas as pd
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import extract_pages
from tableau_api_lib.utils.querying import get_users_dataframe


def get_all_group_fields(conn):
    all_groups = extract_pages(conn.query_groups, parameter_dict={'fields': 'fields=_all_'})
    return all_groups


def get_group_users(conn, group_id):
    all_group_users = extract_pages(conn.get_users_in_group,
                                    content_id=group_id,
                                    parameter_dict={'fields': 'fields=_all_'})
    return all_group_users


def get_all_group_names(conn):
    all_groups = get_all_group_fields(conn)
    all_groupnames = [group['name'] for group in all_groups]
    return all_groupnames


def get_all_group_domain_names(conn):
    all_groups = get_all_group_fields(conn)
    all_group_roles = [group['domain']['name'] for group in all_groups]
    return all_group_roles


def get_groups_dataframe(conn) -> pd.DataFrame:
    """
    Get a Pandas DataFrame containing details for all groups on the active site.
    :param TableauServerConnection conn: the Tableau Server connection
    :return: pd.DataFrame
    """
    groups_df = pd.DataFrame(get_all_group_fields(conn))
    groups_df['domain'] = groups_df['domain'].apply(lambda x: x['name'])
    return groups_df


def get_group_users_dataframe(conn, group_id) -> pd.DataFrame:
    """
    Gets a Pandas DataFrame containing all users belonging to the specified group.
    :param TableauServerConnection conn: the Tableau Server connection
    :param str group_id: the ID for the group whose users are being queried
    :return: pd.DataFrame
    """
    group_users_df = pd.DataFrame(get_group_users(conn, group_id))
    users_df = get_users_dataframe(conn)
    users_columns = list(users_df.columns)
    all_group_users = group_users_df.merge(users_df,
                                           how='left',
                                           left_on='id',
                                           right_on='id',
                                           suffixes=('delete', None))
    return all_group_users[users_columns]


def get_groups_for_a_user_dataframe(conn: TableauServerConnection, user_id: str) -> pd.DataFrame:
    """Returns a Pandas DataFrame containing all groups a user belongs to."""
    groups_for_user_df = pd.DataFrame(extract_pages(conn.get_groups_for_a_user, content_id=user_id))
    return groups_for_user_df
