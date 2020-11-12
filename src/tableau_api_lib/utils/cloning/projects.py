"""
The functions below enable you to clone the projects from Server A to Server B.
This is particularly useful when doing site exports / imports.
Functions to import: clone_projects
"""


import pandas as pd

from tableau_api_lib.utils import extract_pages
from tableau_api_lib.utils.querying import get_users_dataframe
from tableau_api_lib.exceptions import ContentOverwriteDisabled


def get_source_project_df(conn_source, project_names=None):
    """
    Query details for all projects on the source site, or only the projects listed in 'project_names'.
    :param class conn_source: the Tableau Server connection object
    :param list project_names: a list of the desired project names whose details will be queried
    :return: Pandas DataFrame
    """
    project_df = pd.DataFrame(extract_pages(conn_source.query_projects))
    if project_names:
        project_df = project_df[project_df['name'].isin(project_names)]
    project_df['source_owner_id'] = project_df['owner'].apply(extract_project_owner_id)
    project_df.rename(columns={
        'id': 'source_project_id',
        'name': 'source_project_name',
        'description': 'source_project_description',
        'parentProjectId': 'source_project_parent_id'
    }, inplace=True)
    return project_df


def get_target_project_df(conn_target, project_names=None):
    """
    Query details for all projects on the target site, or only the projects listed in 'project_names'.
    :param class conn_target: the Tableau Server connection object
    :param list project_names: a list of the desired project names whose details will be queried
    :return: Pandas DataFrame
    """
    project_df = pd.DataFrame(extract_pages(conn_target.query_projects))
    if project_names:
        project_df = project_df[project_df['name'].isin(project_names)]
    project_df['target_owner_id'] = project_df['owner'].apply(extract_project_owner_id)
    project_df.rename(columns={
        'id': 'target_project_id',
        'name': 'target_project_name',
        'description': 'target_project_description',
        'parentProjectId': 'target_project_parent_id'
    }, inplace=True)
    return project_df


def get_project_names(project_df, column_name) -> list:
    """
    Get the project names for the active site on the specified Tableau Server connection.
    :param DataFrame project_df: the project details DataFrame for the server
    :param str column_name: the name of the column containing project names [source_project_name, target_project_name]
    :return: project names
    """
    project_names = list(project_df[column_name])
    project_names = [project_name for project_name in project_names]
    return project_names


def get_overlapping_project_names(source_project_df, target_project_df) -> list:
    source_project_names = set(get_project_names(source_project_df, 'source_project_name'))
    target_project_names = set(get_project_names(target_project_df, 'target_project_name'))
    overlapping_project_names = source_project_names.intersection(target_project_names)
    return list(overlapping_project_names)


def extract_project_owner_id(owner_dict, key='id'):
    """
    Extract the user ID for the owner of the project.
    :param dict owner_dict: dictionary containing the project owner ID key-value pair
    :param str key: the key for the project owner ID key-value pair
    :return str: project_owner_id
    """
    project_owner_id = owner_dict[key]
    return project_owner_id


def get_source_user_df(conn):
    """
    Get a DataFrame with user details from the source site.
    :param class conn:
    :return DataFrame: source_users_df
    """
    cols = ['email', 'fullName', 'id']
    source_users_df = get_users_dataframe(conn)
    source_users_df = source_users_df[cols]
    source_users_df.rename(columns={
        'email': 'source_email',
        'fullName': 'source_full_name',
        'id': 'source_owner_id'
    }, inplace=True)
    return source_users_df


def get_target_user_df(conn):
    """
    Get a DataFrame with user details from the target site.
    :param class conn:
    :return DataFrame: target_users_df
    """
    cols = ['email', 'fullName', 'id']
    target_users_df = get_users_dataframe(conn)
    target_users_df = target_users_df[cols]
    target_users_df.rename(columns={
        'email': 'target_email',
        'fullName': 'target_full_name',
        'id': 'target_owner_id'
    }, inplace=True)
    return target_users_df


def join_source_projects_and_users(source_project_df, source_user_df, target_user_df):
    """
    Join the source project details, source user details, and target user details.
    :param DataFrame source_project_df: the source project details
    :param DataFrame source_user_df: the source user details
    :param DataFrame target_user_df: the target user details
    :return DataFrame: joined_df
    """
    joined_df = source_project_df.merge(source_user_df, left_on='source_owner_id', right_on='source_owner_id')
    joined_df = joined_df.merge(target_user_df, left_on='source_email', right_on='target_email', how='left')
    return joined_df


def get_source_to_target_df(conn_source, conn_target, project_names=None):
    """
    Get a DataFrame with source project details, source user details, and source target details combined.
    :param class conn_source: the source site connection
    :param class conn_target: the target site connection
    :param list project_names: (optional) the list of desired projects to clone
    :return DataFrame: project_details_df
    """
    project_df = get_source_project_df(conn_source, project_names)
    source_user_df = get_source_user_df(conn_source)
    target_user_df = get_target_user_df(conn_target)
    project_details_df = join_source_projects_and_users(project_df, source_user_df, target_user_df)
    return project_details_df


def get_child_projects_df(project_details_df):
    """
    Get details for projects that have an associated parent project ID.
    :param DataFrame project_details_df: the details for all projects
    :return DataFrame: child_projects_df
    """
    child_projects_df = project_details_df[project_details_df['source_project_parent_id'].notna()]
    return child_projects_df


def get_parent_projects_df(project_details_df):
    """
    Get details for projects that have an associated child project.
    :param DataFrame project_details_df: the details for all projects
    :return DataFrame: parent_projects_df
    """
    parent_projects_df = project_details_df[project_details_df['source_project_parent_id'].isna()]
    return parent_projects_df


def add_target_project_ids(project_details_df, conn_target):
    """
    Join the project details with the target site's project IDs.
    :param DataFrame project_details_df: the project details DataFrame
    :param class conn_target: the target server's connection
    :return: DataFrame
    """
    target_project_df = get_target_project_df(conn_target)[['target_project_id', 'target_project_name']]
    target_project_df.rename(columns={'id': 'target_project_id'}, inplace=True)
    project_details_df = project_details_df.merge(target_project_df,
                                                  left_on='source_project_name',
                                                  right_on='target_project_name',
                                                  how='left')
    return project_details_df


def add_source_parent_project_names(project_details_df):
    """
    Join the parent project names to the details for each project.
    :param DataFrame project_details_df: the project details DataFrame
    :return: DataFrame
    """
    temp_df = project_details_df[['source_project_name', 'source_project_id']].copy()
    temp_df.rename(columns={'source_project_name': 'source_project_parent_name'}, inplace=True)
    project_details_df = project_details_df.merge(temp_df,
                                                  left_on='source_project_parent_id',
                                                  right_on='source_project_id',
                                                  how='left',
                                                  suffixes=(None, '_delete',))
    return project_details_df.drop(columns=['source_project_id_delete'])


def add_target_parent_project_ids(project_details_df):
    """
    Join the parent project IDs to the details for each project.
    :param DataFrame project_details_df: the project details DataFrame
    :return: DataFrame
    """
    temp_df = project_details_df[['target_project_name', 'target_project_id']].copy()
    temp_df.rename(columns={'target_project_id': 'target_project_parent_id'}, inplace=True)
    project_details_df = project_details_df.merge(temp_df,
                                                  left_on='source_project_parent_name',
                                                  right_on='target_project_name',
                                                  how='left',
                                                  suffixes=(None, '_delete'))
    return project_details_df.drop(columns=['target_project_name_delete'])


def create_final_target_df(conn_source, conn_target):
    """
    Create a project details DataFrame intended for use after creating projects on the target server.
    :param class conn_source: the source server connection
    :param class conn_target: the target server connection
    :return: DataFrame
    """
    project_details_df = get_source_to_target_df(conn_source, conn_target)
    project_details_df = add_target_project_ids(project_details_df, conn_target=conn_target)
    project_details_df = add_source_parent_project_names(project_details_df)
    project_details_df = add_target_parent_project_ids(project_details_df)
    return project_details_df


def create_projects(project_details_df, conn_target):
    """
    Create projects on the target server, based on project details from a source server.
    :param DataFrame project_details_df: the project details DataFrame
    :param class conn_target: the target server connection
    :return: list of HTTP responses
    """
    responses = []
    for index, project in project_details_df.iterrows():
        response = conn_target.create_project(
            project_name=project['source_project_name'],
            project_description=project['source_project_description'],
            content_permissions=project['contentPermissions'],
            parent_project_id=None)
        responses.append(response)
    return responses


def update_project_hierarchies(project_details_df, conn_target):
    """
    Update projects on the target server to match the project hierarchies defined on the source server.
    :param DataFrame project_details_df: the project details DataFrame
    :param class conn_target: the target server connection
    :return: list of HTTP responses
    """
    responses = []
    child_projects_df = get_child_projects_df(project_details_df)
    for index, project in child_projects_df.iterrows():
        response = conn_target.update_project(
            project_id=project['target_project_id'],
            parent_project_id=project['target_project_parent_id']
        )
        responses.append(response)
    return responses


def validate_inputs(overwrite_policy):
    valid_overwrite_policies = [None, 'overwrite']
    if overwrite_policy not in valid_overwrite_policies:
        raise ValueError("Invalid overwrite policy provided: '{}'".format(overwrite_policy))


def delete_projects(conn, project_details_df, project_names):
    print("deleting overlapping target projects...")
    responses = []
    projects_to_delete = project_details_df[project_details_df['target_project_name'].isin(project_names)]
    for index, project in projects_to_delete.iterrows():
        responses.append(conn.delete_project(project_id=project['target_project_id']))
    print("overlapping target projects deleted")
    return conn


def clone_projects(conn_source, conn_target, project_names=None, overwrite_policy=None):
    """
    Clones projects from the source server to the target server.
    :param class conn_source: the source server connection
    :param class conn_target: the target server connection
    :param list project_names: (optional) a list of project names to clone; specifying no names clones all projects
    :param str overwrite_policy: (optional) set to 'overwrite' to overwrite content; defaults to not overwriting
    :return: None
    """
    validate_inputs(overwrite_policy)
    source_project_df = get_source_project_df(conn_source=conn_source, project_names=project_names)
    target_project_df = get_target_project_df(conn_target=conn_target, project_names=project_names)
    overlapping_project_names = get_overlapping_project_names(source_project_df=source_project_df,
                                                              target_project_df=target_project_df)
    if any(overlapping_project_names) and not overwrite_policy:
        raise ContentOverwriteDisabled('project')
    if any(overlapping_project_names) and overwrite_policy == 'overwrite':
        delete_projects(conn_target, project_details_df=target_project_df, project_names=overlapping_project_names)
    project_details_df = get_source_to_target_df(conn_source, conn_target, project_names)
    create_projects(project_details_df, conn_target)
    cloned_projects = update_project_hierarchies(create_final_target_df(conn_source, conn_target), conn_target)
    return cloned_projects
