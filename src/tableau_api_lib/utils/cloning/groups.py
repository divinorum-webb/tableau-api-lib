"""
Logical Flow:
1) get groups from source connection
2) get groups from target connection
3) get overlapping groups
4) if overwrite policy = 'overwrite', then delete the overlapping groups from the target connection
5) if overwrite policy = None, then raise an exception
6) create groups on the target connection to mirror those on the source connection
7) * for future: figure out how to best populate the groups... configurable for Active Directory, by users, etc.
"""


import pandas as pd

from tableau_api_lib.exceptions import ContentOverwriteDisabled
from tableau_api_lib.utils.querying import get_groups_dataframe, get_group_users_dataframe
from tableau_api_lib.utils.querying.users import get_users_dataframe


def get_source_group_df(conn_source, group_names=None) -> pd.DataFrame:
    group_df = get_groups_dataframe(conn_source)
    if group_names:
        group_df = group_df[group_df['name'].isin(group_names)]
    group_df.rename(columns={
        'name': 'source_name',
        'id': 'source_id',
        'domain': 'source_domain',
        'userCount': 'source_user_count'
    }, inplace=True)
    return group_df


def get_target_group_df(conn_source, group_names=None) -> pd.DataFrame:
    group_df = get_groups_dataframe(conn_source)
    if group_names:
        group_df = group_df[group_df['name'].isin(group_names)]
    group_df.rename(columns={
        'name': 'target_name',
        'id': 'target_id',
        'domain': 'target_domain',
        'userCount': 'target_user_count'
    }, inplace=True)
    return group_df


def get_overlapping_group_names(source_group_df, target_group_df) -> list:
    source_group_names = set(source_group_df['source_name'])
    target_group_names = set(target_group_df['target_name'])
    overlapping_group_names = source_group_names.intersection(target_group_names)
    return list(overlapping_group_names)


def delete_groups(conn, target_group_df) -> list:
    print("deleting overlapping target groups...")
    group_ids = target_group_df['target_id']
    responses = [conn.delete_group(group_id) for group_id in group_ids]
    print("overlapping target groups deleted")
    return responses


def create_groups(conn_target, source_group_df) -> list:
    responses = []
    for i, _ in enumerate(source_group_df['source_name']):
        response = conn_target.create_group(new_group_name=source_group_df['source_name'][i])
        responses.append(response)
    return responses


def clone_group_users(conn_source, conn_target, source_group_df, target_group_df):
    combined_group_df = source_group_df.merge(target_group_df,
                                              how='left',
                                              left_on='source_name',
                                              right_on='target_name',
                                              suffixes=(None, None))
    target_users_df = get_users_dataframe(conn_target)
    for group_index, group in combined_group_df.iterrows():
        if group['source_name'] == 'All Users':
            continue
        source_group_users_df = get_group_users_dataframe(conn_source, group['source_id'])
        combined_group_users_df = source_group_users_df.merge(target_users_df,
                                                              how='left',
                                                              left_on='name',
                                                              right_on='name',
                                                              suffixes=(None, '_target'))
        for user_index, user in combined_group_users_df.iterrows():
            print("adding user to group: id={}  name={}".format(group['target_id'], group['target_name']))
            conn_target.add_user_to_group(group_id=group['target_id'], user_id=user['id_target'])


def process_overlapping_group_names(conn_target,
                                    target_group_df,
                                    overlapping_group_names,
                                    overwrite_policy) -> None:
    if any(overlapping_group_names) and not overwrite_policy:
        raise ContentOverwriteDisabled('groups')
    if any(overlapping_group_names) and overwrite_policy == 'overwrite':
        delete_groups(conn_target, target_group_df[target_group_df['target_name'].isin(overlapping_group_names)])
    pass


def reset_connection(conn):
    conn.sign_out()
    conn.sign_in()
    return conn


def clone_groups(conn_source,
                 conn_target,
                 group_names=None,
                 populate_users=False,
                 group_mapping_file=None,
                 overwrite_policy=None) -> list:
    source_group_df = get_source_group_df(conn_source, group_names)
    target_group_df = get_target_group_df(conn_target, group_names)
    overlapping_group_names = get_overlapping_group_names(source_group_df, target_group_df)
    process_overlapping_group_names(conn_target, target_group_df, overlapping_group_names, overwrite_policy)
    cloned_groups = create_groups(conn_target, source_group_df)
    if populate_users:
        reset_connection(conn_target)
        clone_group_users(conn_source, conn_target, source_group_df, get_target_group_df(conn_target, group_names))
    return cloned_groups
