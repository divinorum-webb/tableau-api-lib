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
import os

from tableau_api_lib.utils.querying import get_workbooks_dataframe, get_projects_dataframe, \
    get_embedded_datasources_dataframe
from tableau_api_lib.utils.filemod import modify_tableau_zipfile, set_temp_dirs, delete_temp_files, \
    replace_unzipped_xml_file
from tableau_api_lib.utils import flatten_dict_column, get_server_netloc
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
    workbook_df = flatten_dict_column(workbook_df, keys=['id', 'name'], col_name='project')
    workbook_df = flatten_dict_column(workbook_df, keys=['id', 'name', 'email'], col_name='owner')
    workbook_df.columns = ["source_" + column for column in workbook_df.columns]
    workbook_df['source_project_name_lower'] = workbook_df['source_project_name'].apply(lambda x: str(x).lower())
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
        workbook_df = flatten_dict_column(workbook_df, keys=['id', 'name'], col_name='project')
        workbook_df = flatten_dict_column(workbook_df, keys=['id', 'name', 'email'], col_name='owner')
        workbook_df.columns = ["target_" + column for column in workbook_df.columns]
        workbook_df['target_project_name_lower'] = workbook_df['target_project_name'].apply(lambda x: str(x).lower())
    except KeyError:
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
    target_projects_df['target_project_name_lower'] = target_projects_df['target_project_name'].apply(lambda x: str(x).lower())
    workbook_details_df = source_workbook_df.merge(target_projects_df,
                                                   how='left',
                                                   left_on='source_project_name_lower',
                                                   right_on='target_project_name_lower')
    return workbook_details_df


def get_workbook_connections_df(conn_source, conn_target, workbooks_df):
    workbook_connections_df = get_embedded_datasources_dataframe(conn_source,
                                                                 workbooks_df=workbooks_df,
                                                                 id_col='source_id',
                                                                 name_col='source_name',
                                                                 new_col_prefix='source_')
    workbook_connections_df['serverAddress'] = workbook_connections_df['serverAddress'].\
        apply(lambda x: x.replace(get_server_netloc(conn_source.server), get_server_netloc(conn_target.server)))
    workbook_connections_df['serverPort'] = workbook_connections_df['serverPort'].fillna(0).astype('int').astype('str')
    workbook_connections_df['userName'] = workbook_connections_df['userName'].fillna('')
    return workbook_connections_df


def get_workbook_credentials_df(workbook_connections_df, credentials_file_path=None):
    if not credentials_file_path:
        workbook_connections_df['password'] = ''
    else:
        credentials_df = pd.read_csv(credentials_file_path)
        credentials_df['password'].fillna('', inplace=True)
        credentials_df['serverPort'].fillna(0, inplace=True)
        # credentials_df['password'].fillna('', inplace=True)
        credentials_df['serverPort'] = credentials_df['serverPort'].astype('int').astype('str')
        workbook_connections_df = workbook_connections_df.merge(credentials_df,
                                                                how='left',
                                                                on=['serverAddress', 'userName'],
                                                                suffixes=(None, '_delete'))
    workbook_connections_df['password'] = workbook_connections_df['password'].fillna('')
    credential_cols = [col for col in list(workbook_connections_df.columns) if not str(col).endswith('_delete')]
    return workbook_connections_df[credential_cols]


def get_project_workbook_credentials_df(workbook_credentials_df, source_to_target_wb_df):
    source_to_target_abbreviated_df = source_to_target_wb_df[['source_id', 'source_project_name', 'target_project_name',
                                                              'source_project_name_lower', 'target_project_name_lower']]
    project_workbook_credentials_df = workbook_credentials_df.merge(source_to_target_abbreviated_df,
                                                                    how='left',
                                                                    left_on='source_workbook_id',
                                                                    right_on='source_id')
    return project_workbook_credentials_df


def get_workbook_names(workbook_df, column_name) -> list:
    """
    Get the workbook names for the active site on the specified Tableau Server connection.
    :param DataFrame workbook_df: the workbook details DataFrame for the server
    :param str column_name: the name of the column containing workbook names [source_workbook_name, target_workbook_name]
    :return: list
    """
    workbook_names = list(workbook_df[column_name])
    return workbook_names


def get_overlapping_workbook_names(source_workbook_df, target_workbook_df) -> list:
    try:
        source_workbook_names = set(get_workbook_names(source_workbook_df, 'source_workbook_name'))
        target_workbook_names = set(get_workbook_names(target_workbook_df, 'target_workbook_name'))
        overlapping_workbook_names = source_workbook_names.intersection(target_workbook_names)
        return list(overlapping_workbook_names)
    except KeyError:
        return []


def get_workbook_file_type(response):
    try:
        file_type = ''
        content_type = dict(response.headers)['Content-Type']
        if str(content_type.lower()) == 'application/xml':
            file_type = 'twb'
        elif str(content_type.lower()) == 'application/octet-stream':
            file_type = 'twbx'
        return file_type
    except KeyError:
        raise Exception(f"An error occurred while downloading the workbook: status code {response.status_code}")


def download_workbooks(conn_source, workbooks_df, download_dir=None, target_project_dir=None):
    download_dir = download_dir or os.getcwd() + '/temp/workbooks/source'
    target_project_dir = target_project_dir or os.getcwd() + '/temp/workbooks/target'
    workbook_file_names_df = pd.DataFrame()
    for index, workbook in workbooks_df.iterrows():
        response = conn_source.download_workbook(workbook_id=workbook['source_id'])
        file_type = get_workbook_file_type(response)
        print("downloading workbook '{0}' to '{1}/{0}.{2}'...".format(workbook['source_name'],
                                                                      download_dir,
                                                                      file_type))
        with open(f"{download_dir}/{workbook['source_name']}.{file_type}", 'wb') as file:
            file.write(response.content)
        workbook_file_name_df = workbook[['source_name', 'source_project_name']].copy()
        workbook_file_name_df['file_path'] = f"{target_project_dir}/{workbook['source_name']}.{file_type}"
        workbook_file_names_df = pd.concat([workbook_file_names_df, workbook_file_name_df], ignore_index=True, sort=False)
    return workbook_file_names_df


def add_downloaded_file_names(workbooks_df, downloaded_file_names_df):
    workbooks_df = workbooks_df.merge(downloaded_file_names_df,
                                      how='left',
                                      on=['source_name', 'source_project_name'])
    return workbooks_df


def modify_workbooks_by_project(conn_source, conn_target, source_project_dir, target_project_dir, extraction_dir):
    for file in os.listdir(source_project_dir):
        file_type = os.path.splitext(file)[-1]
        if file_type == '.twbx':
            modify_tableau_zipfile(zipfile_path=source_project_dir + '/' + file,
                                   conn_source=conn_source, conn_target=conn_target,
                                   extraction_dir_path=extraction_dir,
                                   destination_dir_path=target_project_dir)
        elif file_type == '.twb':
            replace_unzipped_xml_file(file_path=source_project_dir + '/' + file,
                                      conn_source=conn_source, conn_target=conn_target,
                                      extraction_dir_path=target_project_dir)
        delete_temp_files(extraction_dir)


def delete_workbooks(conn, workbook_details_df, workbook_names):
    print("deleting overlapping target workbooks...")
    responses = []
    workbooks_to_delete = workbook_details_df[workbook_details_df['target_workbook_name'].isin(workbook_names)]
    for index, workbook in workbooks_to_delete.iterrows():
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
    if overwrite_policy not in valid_overwrite_policies:
        raise ValueError("Invalid overwrite policy provided: '{}'".format(overwrite_policy))


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


def publish_workbooks_by_project(conn_target, project_workbooks_df, project_workbook_credentials_df):
    for index, workbook in project_workbooks_df.iterrows():
        workbook_credentials_df = project_workbook_credentials_df.loc[
            project_workbook_credentials_df['source_workbook_name'] == workbook['source_name']]
        response = conn_target.publish_workbook(workbook_file_path=workbook['file_path'],
                                                workbook_name=workbook['source_name'],
                                                project_id=workbook['target_project_id'],
                                                server_address=workbook_credentials_df['serverAddress'].to_list(),
                                                port_number=workbook_credentials_df['serverPort'].to_list(),
                                                connection_username=workbook_credentials_df['userName'].to_list(),
                                                connection_password=workbook_credentials_df['password'].to_list(),
                                                embed_credentials_flag=workbook_credentials_df['embedPassword'].to_list())
        print("publish response for {}:\n{}".format(workbook['source_name'], response.json()))
        if response.status_code in [200, 201]:
            project_workbooks_df.at[index, 'target_id'] = response.json()['workbook']['id']
    return project_workbooks_df


def clone_workbooks_by_project(conn_source,
                               conn_target,
                               source_to_target_wb_df,
                               project_workbook_credentials_df,
                               temp_dir,
                               project,
                               extraction_dir):
    source_project_dir = f"{temp_dir}/workbooks/source/{project}"
    target_project_dir = f"{temp_dir}/workbooks/target/{project}"
    os.makedirs(source_project_dir, exist_ok=False)
    os.makedirs(target_project_dir, exist_ok=False)
    project_workbooks_df = source_to_target_wb_df[
        source_to_target_wb_df['target_project_name_lower'] == str(project).lower()].copy()
    project_workbook_credentials_df = project_workbook_credentials_df[
        project_workbook_credentials_df['target_project_name_lower'] == str(project).lower()].copy()
    workbook_file_names_df = download_workbooks(conn_source, project_workbooks_df, download_dir=source_project_dir, target_project_dir=target_project_dir)
    project_workbooks_df = add_downloaded_file_names(project_workbooks_df, workbook_file_names_df)
    modify_workbooks_by_project(conn_source, conn_target, source_project_dir, target_project_dir, extraction_dir)
    project_workbooks_df.to_csv(project + '_project_workbooks_df.csv')
    project_workbook_credentials_df.to_csv(project + '_project_workbook_credentials_df.csv')
    print("project_workbooks_df:\n", project_workbooks_df['file_path'])
    print("project_workbook_credentials_df:\n", project_workbook_credentials_df)
    published_workbooks = publish_workbooks_by_project(conn_target, project_workbooks_df, project_workbook_credentials_df)
    print("published_workbooks_df:\n", published_workbooks)
    # project_datasources = publish_datasources_by_project(conn_target, project_datasources_df)
    # update_datasources_by_project(conn_target, project_datasources)


# TODO hide sheets not visible on the source server when publishing to the target; copy tags and owner (make optional)
def clone_workbooks(conn_source,
                    conn_target,
                    workbook_names=None,
                    credentials_file_path=None,
                    temp_dir=None,
                    overwrite_policy=None):
    """
    Clones workbooks from the source server to the target server.
    :param class conn_source: the source server connection
    :param class conn_target: the target server connection
    :param list workbook_names: (optional) a list of workbook names to clone; specifying no names clones all workbooks
    :param str temp_dir: (optional) designate the location where temp files will be stored
    :param str overwrite_policy: (optional) set to 'overwrite' to overwrite content; defaults to not overwriting
    :return: None
    """
    temp_dir, extraction_dir = set_temp_dirs(temp_dir)
    validate_inputs(overwrite_policy)
    source_workbook_df = get_source_workbook_df(conn_source=conn_source, workbook_names=workbook_names)
    target_workbook_df = get_target_workbook_df(conn_target=conn_target, workbook_names=workbook_names)
    # refactor the overlapping checks to a new function: process_overwrite_policy()
    if any(target_workbook_df.columns):
        overlapping_workbook_names = get_overlapping_workbook_names(source_workbook_df, target_workbook_df)
        process_overlapping_workbooks(conn_target, target_workbook_df, overlapping_workbook_names, overwrite_policy)
    source_to_target_wb_df = get_source_to_target_wb_df(source_workbook_df=source_workbook_df, conn_target=conn_target)
    workbook_conn_df = get_workbook_connections_df(conn_source, conn_target, source_to_target_wb_df)
    workbook_credentials_df = get_workbook_credentials_df(workbook_conn_df, credentials_file_path)
    project_workbook_credentials_df = get_project_workbook_credentials_df(workbook_credentials_df, source_to_target_wb_df)
    try:
        for project in source_to_target_wb_df['source_project_name'].unique():
            clone_workbooks_by_project(conn_source=conn_source,
                                       conn_target=conn_target,
                                       source_to_target_wb_df=source_to_target_wb_df,
                                       project_workbook_credentials_df=project_workbook_credentials_df,
                                       temp_dir=temp_dir,
                                       project=project,
                                       extraction_dir=extraction_dir)
    finally:
        delete_temp_files(temp_dir)
    # -> update workbooks to have the correct owner (and other metadata)
    #   -> optional mapping stage: map_workbook_owners (searches for original owner on target site, otherwise default)
