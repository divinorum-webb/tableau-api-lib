"""
workflow logic:
1) get workbook details for the source site
2) get workbook details for the target site
3) get overlapping workbooks (source & target)
4) if overwrite_policy == 'overwrite', then delete the overlapping workbooks on the target site
5) if overwrite_policy == None, then throw an exception
6) if overwrite_policy == 'merge', then proceed with cloning the non-overlapping workbooks and leave the others as is
7) for each workbook on the source site, build a clone of that workbook on the target site
    7a) for each workbook, query its connections
    7b) for each connection, check if it is a published data source
    7c) build a dataframe that labels datasources as embedded or published
    7d) for each published data source on the source site, download and publish that data source to the target site
            must open each file and edit its server, username, and site name
    7e) make sure each data source is published to the correct respective workbook
    7f) download all workbooks (will be downloaded as .twbx)
        save each workbook to the local file system (in the future provide a way to save to cloud storage)
    7g) publish all 'published' datasources to the target site
    7h) publish all workbooks to their respective workbooks, with published data source credentials
            for published data sources: server name, connection username, connection password must be provided

"""


import pandas as pd
import requests

from tableau_api_lib.utils.querying import get_workbooks_dataframe, get_projects_dataframe, get_datasources_dataframe
from tableau_api_lib.exceptions import ContentOverwriteDisabled


def get_source_workbook_df(conn_source, workbook_names=None):
    """
    Query details for all workbooks on the source site, or only the workbooks listed in 'workbook_names'.
    :param class conn_source: the Tableau Server connection object
    :param list workbook_names: a list of the desired workbook names whose details will be queried
    :return: Pandas DataFrame
    """
    workbook_df = get_workbooks_dataframe(conn_source)
    workbook_df.fillna(value='', inplace=True)
    if workbook_names:
        workbook_df = workbook_df[workbook_df['name'].isin(workbook_names)]
    workbook_df = extract_project_details(workbook_df, keys=['id', 'name'])
    workbook_df = extract_owner_details(workbook_df, keys=['id', 'name', 'email'])
    workbook_df.columns = ["source_" + column for column in workbook_df.columns]
    return workbook_df


def get_target_workbook_df(conn_target, workbook_names=None):
    """
    Query details for all workbooks on the source site, or only the workbooks listed in 'workbook_names'.
    :param class conn_target: the Tableau Server connection object
    :param list workbook_names: a list of the desired workbook names whose details will be queried
    :return: Pandas DataFrame
    """
    try:
        workbook_df = get_workbooks_dataframe(conn_target)
        workbook_df.fillna(value='', inplace=True)
        if workbook_names:
            workbook_df = workbook_df[workbook_df['name'].isin(workbook_names)]
        workbook_df = extract_project_details(workbook_df, keys=['id', 'name'])
        workbook_df = extract_owner_details(workbook_df, keys=['id', 'name', 'email'])
        workbook_df.columns = ["target_" + column for column in workbook_df.columns]
    except ValueError:
        workbook_df = pd.DataFrame()
    return workbook_df


def get_source_to_target_wb_df(source_workbook_df, conn_target):
    """
    Get a DataFrame with source workbook details, source user details, and source target details combined.
    :param pd.DataFrame source_workbook_df: the source workbook details
    :param class conn_target: the target Tableau Server connection
    :return DataFrame: workbook_details_df
    """
    target_projects_df = get_projects_dataframe(conn_target)
    target_projects_df.columns = ["target_project_" + column for column in target_projects_df.columns]
    workbook_details_df = source_workbook_df.merge(target_projects_df,
                                                   how='left',
                                                   left_on='source_project_name',
                                                   right_on='target_project_name')
    return workbook_details_df


def get_source_to_target_ds_df(conn_target, source_datasource_df):
    """
    Get a DataFrame with source datasource details, source user details, and source target details combined.
    :param pd.DataFrame source_datasource_df: the source datasource details
    :param class conn_target: the target Tableau Server connection
    :return DataFrame: datasource_details_df
    """
    target_projects_df = get_projects_dataframe(conn_target)
    target_projects_df.columns = ["target_project_" + column for column in target_projects_df.columns]
    datasource_details_df = source_datasource_df.merge(target_projects_df,
                                                       how='left',
                                                       left_on='project_name',
                                                       right_on='target_project_name')
    return datasource_details_df


def extract_project_details(df, keys):
    for key in keys:
        df['project_' + key] = df['project'].apply(lambda project: project[key])
    df.drop(columns=['project'], inplace=True)
    return df


def extract_owner_details(df, keys):
    for key in keys:
        df['owner_' + key] = df['owner'].apply(lambda owner: owner[key])
    df.drop(columns=['owner'], inplace=True)
    return df


def extract_datasource_details(df, keys):
    for key in keys:
        df['datasource_' + key] = df['datasource'].apply(lambda datasource: datasource[key])
    df.drop(columns=['datasource'], inplace=True)
    return df


def add_connection_context(workbook, workbook_connections_df):
    workbook_connections_df['workbook_name'] = workbook['source_name']
    workbook_connections_df['workbook_id'] = workbook['source_id']
    workbook_connections_df['project_name'] = workbook['source_project_name']
    workbook_connections_df['project_id'] = workbook['source_project_id']
    return workbook_connections_df


def get_workbook_names(workbook_df, column_name) -> list:
    """
    Get the workbook names for the active site on the specified Tableau Server connection.
    :param DataFrame workbook_df: the workbook details DataFrame for the server
    :param str column_name: the name of the column containing workbook names [source_workbook_name, target_workbook_name]
    :return: workbook names
    """
    workbook_names = list(workbook_df[column_name])
    workbook_names = [workbook_name for workbook_name in workbook_names]
    return workbook_names


def get_overlapping_workbook_names(source_workbook_df, target_workbook_df) -> list:
    try:
        source_workbook_names = set(get_workbook_names(source_workbook_df, 'source_workbook_name'))
        target_workbook_names = set(get_workbook_names(target_workbook_df, 'target_workbook_name'))
        overlapping_workbook_names = source_workbook_names.intersection(target_workbook_names)
        return list(overlapping_workbook_names)
    except KeyError:
        return []


def delete_workbooks(conn, workbook_details_df, workbook_names):
    print("deleting overlapping target workbooks...")
    responses = []
    workbooks_to_delete = workbook_details_df[workbook_details_df['target_workbook_name'].isin(workbook_names)]
    for i, workbook in workbooks_to_delete.iterrows():
        print("deleting workbook: (name='{}', id='{}'".
              format(workbook['target_workbook_id'], workbook['target_workbook_name']))
        responses.append(conn.delete_workbook(workbook_id=workbook['target_workbook_id']))
    print("overlapping target workbooks deleted")
    return conn


def validate_inputs(overwrite_policy):
    valid_overwrite_policies = [
        None,
        'overwrite',
        'merge'  # add logic for merging (leave existing content as is and create any missing content)
    ]
    if overwrite_policy in valid_overwrite_policies:
        pass
    else:
        raise ValueError("Invalid overwrite policy provided: '{}'".format(overwrite_policy))


# def get_published_workbook_datasources(conn_source, source_workbook_df):
#     published_workbook_datasources_df = pd.DataFrame()
#     for index, workbook in source_workbook_df.iterrows():
#         workbook_connections_df = pd.DataFrame(
#             conn_source.query_workbook_connections(workbook['source_id']).json()['connections']['connection'])
#         workbook_connections_df = extract_datasource_details(workbook_connections_df, ['id', 'name'])
#         workbook_connections_df = add_connection_context(workbook, workbook_connections_df)
#         published_workbook_datasources_df = pd.concat([published_workbook_datasources_df, workbook_connections_df],
#                                                       sort=False)
#     published_workbook_datasources_df = published_workbook_datasources_df[
#         published_workbook_datasources_df['serverAddress'].isin([urlparse(conn_source.server).netloc])]
#     all_datasources_df = get_datasources_dataframe(conn_source)
#     print("published wb ds: \n: ", published_workbook_datasources_df['datasource_id'])
#     print("all ds: \n: ", all_datasources_df['id'])
#     published_workbook_datasources_df = published_workbook_datasources_df[
#         published_workbook_datasources_df['datasource_id'].isin(all_datasources_df['id'])]
#     return published_workbook_datasources_df


def get_published_datasources(conn_source):
    published_datasources_df = get_datasources_dataframe(conn_source)
    published_datasources_df = extract_project_details(published_datasources_df, ['id', 'name'])
    published_datasources_df = extract_owner_details(published_datasources_df, ['id', 'name'])
    return published_datasources_df


def download_published_datasources(conn_source, published_datasources_df, file_path='temp/'):
    projects = published_datasources_df['project_name'].unique()
    published_datasources_df['datasource_file_path'] = published_datasources_df['name']\
        .apply(lambda x: file_path + str(x) + '.tdsx')
    print("published_datasources_df_cols: \n", published_datasources_df.columns)
    for project in projects:
        print("downloading datasources from project '{}'...".format(project))
        datasources_df = published_datasources_df[published_datasources_df['project_name'].isin([project])]
        for index, datasource in datasources_df.iterrows():
            print("downloading datasource '{}'...".format(datasource['name']))
            file_contents = conn_source.download_data_source(datasource_id=datasource['id'])
            with open(datasource['datasource_file_path'], 'wb') as file:
                file.write(file_contents.content)
            print("completed download of datasource '{}'".format(datasource['name']))
        print("completed downloading datasources from project '{}'".format(project))
    return published_datasources_df


def publish_datasources(conn_target, datasources_df):
    publish_responses = []
    datasources_df['target_datasource_id'] = ''
    for index, datasource in datasources_df.iterrows():
        print("publishing datasource '{}'...".format(datasource['name']))
        response = conn_target.publish_data_source(datasource_file_path=datasource['datasource_file_path'],
                                                   datasource_name=datasource['name'],
                                                   project_id=datasource['target_project_id'])
        if response.status_code == 201:
            datasources_df.loc[index, 'target_datasource_id'] = response.json()['datasource']['id']
            print("successfully published datasource '{}'".format(datasource['name']))
        publish_responses.append(response)
    return datasources_df, publish_responses


def clone_datasource_tags(conn_target, datasources_df):
    responses = []
    for index, datasource in datasources_df.iterrows():
        print("cloning tags for datasource '{}'...".format(datasource['name']))
        try:
            datasource_tags = [tag['label'] for tag in datasource['tags']['tag']]
            response = conn_target.add_tags_to_data_source(datasource['target_datasource_id'], datasource_tags)
            responses.append(response)
            if response.status_code == 200:
                print("successfully cloned tags for datasource '{}'".format(datasource['name']))
            else:
                print("failed to clone tags for datasource '{}'".format(datasource['name']))
        except KeyError:
            print("no tags exist for datasource '{}'".format(datasource['name']))
    return responses


def process_overlapping_workbooks(conn_target,
                                  target_workbook_df,
                                  overlapping_workbook_names,
                                  overwrite_policy) -> None:
    """
    Processes overlapping workbooks depending on the overwrite policy specified.
    :param class conn_target: the target Tableau Server connection
    :param pd.DataFrame target_workbook_df: a Pandas DataFrame with target workbook details
    :param list overlapping_workbook_names: a list of workbooks present on both the source and target connections
    :param str overwrite_policy: must be set to 'overwrite' to enable overwriting existing content
    :return:
    """
    if any(overlapping_workbook_names) and not overwrite_policy:
        raise ContentOverwriteDisabled('workbooks')
    if any(overlapping_workbook_names) and overwrite_policy == 'overwrite':
        delete_workbooks(conn_target, target_workbook_df, overlapping_workbook_names)
    pass


def clone_workbooks(conn_source, conn_target, workbook_names=None, overwrite_policy=None):
    """
    Clones workbooks from the source server to the target server.
    :param class conn_source: the source server connection
    :param class conn_target: the target server connection
    :param list workbook_names: (optional) a list of workbook names to clone; specifying no names clones all workbooks
    :param str overwrite_policy: (optional) set to 'overwrite' to overwrite content; defaults to not overwriting
    :return: None
    """
    validate_inputs(overwrite_policy)
    source_workbook_df = get_source_workbook_df(conn_source=conn_source, workbook_names=workbook_names)
    target_workbook_df = get_target_workbook_df(conn_target=conn_target, workbook_names=workbook_names)
    # refactor the overlapping checks to a new function: process_overwrite_policy()
    overlapping_workbook_names = get_overlapping_workbook_names(source_workbook_df, target_workbook_df)
    process_overlapping_workbooks(conn_target, target_workbook_df, overlapping_workbook_names, overwrite_policy)
    workbook_details_df = get_source_to_target_wb_df(source_workbook_df, conn_target)
    workbook_details_df.to_csv('delete_workbook_details_df.csv')
    # published_datasources_df = get_published_datasources(conn_source)
    # published_datasources_df = download_published_datasources(conn_source, published_datasources_df)
    # -> modify the downloaded data source .tds files to reference the target server instead of the source server
    # -> save the modified .tds files and zip them back into .tdsx files
    # published_datasources_df = get_source_to_target_ds_df(conn_target, published_datasources_df)
    # published_datasources_df, publish_responses = publish_datasources(conn_target, published_datasources_df)
    # clone_tag_responses = clone_datasource_tags(conn_target, published_datasources_df)
    # -> download source site's workbooks
    # -> modify downloaded workbook .twb files to reflect the target server instead of the source server
    # -> repackage files if they were .twbx, otherwise leave as .twb
    # -> publish source site's workbooks to target site
    # -> update workbooks to have the correct owner (and other metadata)
    #   -> optional mapping stage: map_workbook_owners (searches for original owner on target site, otherwise default)
