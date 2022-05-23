import pandas as pd
import os
import warnings


from tableau_api_lib.utils.querying import get_workbooks_dataframe, get_projects_dataframe, get_datasources_dataframe, \
    get_workbook_connections_dataframe, get_datasource_connections_dataframe
from tableau_api_lib.utils.filemod import modify_tableau_zipfile, set_temp_dirs, delete_temp_files
from tableau_api_lib.utils import flatten_dict_column, get_server_netloc
from tableau_api_lib.exceptions import ContentOverwriteDisabled


def get_source_datasource_df(conn_source, datasource_ids):
    datasource_df = get_datasources_dataframe(conn_source)
    if datasource_ids:
        datasource_df = datasource_df.loc[datasource_df['id'].isin(datasource_ids)]
    datasource_df = flatten_dict_column(datasource_df, keys=['id', 'name'], col_name='project')
    datasource_df = flatten_dict_column(datasource_df, keys=['id', 'name'], col_name='owner')
    datasource_df.columns = ["source_" + column for column in datasource_df.columns]
    datasource_df['source_project_name_lower'] = datasource_df['source_project_name'].apply(lambda x: x.lower())
    return datasource_df


def get_target_project_df(conn_target):
    project_df = get_projects_dataframe(conn_target)[['name', 'id']]
    project_df.columns = ['target_project_' + col for col in list(project_df.columns)]
    project_df['target_project_name_lower'] = project_df['target_project_name'].apply(lambda x: x.lower())
    return project_df


def get_mapped_datasource_df(source_datasource_df, target_project_df):
    mapped_datasource_df = source_datasource_df.merge(target_project_df,
                                                      how='left',
                                                      left_on='source_project_name_lower',
                                                      right_on='target_project_name_lower')
    relevant_cols = ['source_tags', 'source_description', 'source_encryptExtracts', 'source_id', 'source_isCertified',
                     'source_isPublished', 'source_name', 'source_type', 'source_useRemoteQueryAgent',
                     'source_serverName', 'target_project_name', 'target_project_id', 'source_owner_name']
    mapped_datasource_df = mapped_datasource_df[relevant_cols]
    return mapped_datasource_df


def get_all_workbook_connections_df(conn):
    all_wb_connections_df = pd.DataFrame()
    all_wbs = get_workbooks_dataframe(conn)
    for workbook_id in all_wbs['id']:
        wb_connections = get_workbook_connections_dataframe(conn, workbook_id).drop(columns=['id'])
        wb_connections = wb_connections[~wb_connections['userName'].isin([None, ''])]
        all_wb_connections_df = pd.concat([all_wb_connections_df, wb_connections], sort=False)
    all_wb_connections_df = all_wb_connections_df[['serverAddress', 'userName', 'serverPort']].drop_duplicates()
    return all_wb_connections_df


def get_all_datasource_connections_df(conn):
    all_ds_connections_df = pd.DataFrame()
    all_dss = get_datasources_dataframe(conn)
    for index, datasource in all_dss.iterrows():
        datasource_name, datasource_id = datasource['name'], datasource['id']
        ds_connections = get_datasource_connections_dataframe(conn, datasource_id).drop(columns=['id'])
        # ds_connections['datasource_name'] = datasource_name
        ds_connections = ds_connections[~ds_connections['userName'].isin([None, ''])]
        all_ds_connections_df = pd.concat([all_ds_connections_df, ds_connections], sort=False)
    all_ds_connections_df = all_ds_connections_df[['serverAddress', 'userName']].drop_duplicates()
    return all_ds_connections_df


def get_mapped_connections_df(conn_source, conn_target, source_connections_df):
    mapped_connections_df = source_connections_df.copy()
    for index, connection in mapped_connections_df.iterrows():
        if connection['serverAddress'] == get_server_netloc(conn_source.server):
            mapped_connections_df.at[index, 'target_serverAddress'] = get_server_netloc(conn_target.server)
        else:
            mapped_connections_df.at[index, 'target_serverAddress'] = connection['serverAddress']
    return mapped_connections_df


def get_datasource_connection_ports(all_datasource_connections_df, all_workbook_connections_df):
    all_datasource_connections_df = all_datasource_connections_df.merge(all_workbook_connections_df,
                                                                        how='left',
                                                                        left_on=['serverAddress', 'userName'],
                                                                        right_on=['serverAddress', 'userName'])
    return all_datasource_connections_df


def get_credentials_df(file_path):
    if file_path:
        if os.path.exists(file_path):
            datasource_credentials_df = pd.read_csv(file_path)
            return datasource_credentials_df
        else:
            raise FileNotFoundError(f"No credentials CSV mapping file was found at '{file_path}'.")
    else:
        warnings.warn("""
        No credentials mapping file path was provided.
        Attempts to publish datasources that require credentials may fail.
        If any datasources have live connections, or are published with embedded credentials, provide the 
        clone_datasources() function a file path using the 'credentials_file_path' parameter.
        The columns defined in the CSV must include: 'serverAddress', 'userName', 'password', and 'serverPort'.
        """)
        return pd.DataFrame(columns=['serverAddress', 'userName', 'password', 'serverPort'])


def download_datasources(conn_source, datasource_df, download_dir=None):
    download_dir = download_dir or os.getcwd() + '/temp/datasources/source'
    for index, datasource in datasource_df.iterrows():
        with open(f"{download_dir}/{datasource['source_name']}.tdsx", 'wb') as file:
            print("downloading datasource '{0}' to '{1}/{0}.tdsx'...".format(datasource['source_name'], download_dir))
            response = conn_source.download_data_source(datasource_id=datasource['source_id'])
            file.write(response.content)


def get_credential_mappings(datasource_df, credentials_df):
    mapped_df = datasource_df.merge(credentials_df,
                                    how='left',
                                    left_on=['target_serverAddress', 'userName', 'serverPort'],
                                    right_on=['serverAddress', 'userName', 'serverPort'],
                                    suffixes=[None, '_delete'])
    cols_to_keep = list(datasource_df.columns) + ['password']
    return mapped_df[cols_to_keep]


# def set_temp_dirs(temp_dir):
#     temp_dir = temp_dir or os.getcwd() + '/temp'
#     extraction_dir = temp_dir + '/extracted'
#     create_temp_dirs(temp_dir)
#     return temp_dir, extraction_dir


def get_cloning_df(conn_source,
                   conn_target,
                   datasource_ids,
                   credentials_file_path) -> pd.DataFrame:
    """
    Get a Pandas DataFrame populated with details used to clone datasources from conn_source to conn_target.
    :param TableauServerConnection conn_source: the source Tableau Server connection
    :param TableauServerConnection conn_target: the target (destination) Tableau Server connection
    :param list datasource_ids: if the list exists, only datasources whose ID values are present will be cloned
    :param str credentials_file_path: defines the path to the CSV file containing database credentials
    :return: pd.DataFrame
    """
    source_datasource_df = get_source_datasource_df(conn_source, datasource_ids)
    target_project_df = get_target_project_df(conn_target)
    mapped_datasource_df = get_mapped_datasource_df(source_datasource_df, target_project_df)
    source_connections_df = get_datasource_connection_ports(get_all_workbook_connections_df(conn_source),
                                                            get_all_datasource_connections_df(conn_source))
    mapped_connections_df = get_mapped_connections_df(conn_source, conn_target, source_connections_df)
    mapped_datasource_df = mapped_datasource_df.merge(mapped_connections_df, how='left',
                                                      left_on='source_serverName',
                                                      right_on='serverAddress')
    mapped_credentials_df = get_credential_mappings(mapped_datasource_df,
                                                    get_credentials_df(credentials_file_path))
    return mapped_credentials_df


def modify_datasources_by_project(conn_source, conn_target, source_project_dir, target_project_dir, extraction_dir):
    for file in os.listdir(source_project_dir):
        modify_tableau_zipfile(zipfile_path=source_project_dir + '/' + file,
                               conn_source=conn_source, conn_target=conn_target,
                               extraction_dir_path=extraction_dir,
                               destination_dir_path=target_project_dir)
        delete_temp_files(extraction_dir)


def publish_datasources_by_project(conn_target, project_datasources_df):
    for index, datasource in project_datasources_df.iterrows():
        response = conn_target.publish_data_source(datasource_file_path=datasource['file_path'],
                                                   datasource_name=datasource['source_name'],
                                                   project_id=datasource['target_project_id'],
                                                   connection_username=datasource['userName'],
                                                   connection_password=datasource['password'],
                                                   embed_credentials_flag=True if datasource['userName'] else False)
        if response.status_code in [200, 201]:
            project_datasources_df.at[index, 'target_id'] = response.json()['datasource']['id']
        print(f"publish response for {datasource['source_name']}: ", response.json())
    return project_datasources_df


def update_datasources_by_project(conn_target, project_datasources_df):
    for index, datasource in project_datasources_df.iterrows():
        conn_target.update_data_source(datasource_id=datasource['target_id'],
                                       # new_owner_id=,
                                       is_certified_flag=datasource['source_isCertified'])
        if 'tag' in datasource['source_tags'].keys():
            tags = datasource['source_tags']['tag']
            tag_list = [tag_dict['label'] for tag_dict in tags]
            conn_target.add_tags_to_data_source(datasource_id=datasource['target_id'], tags=tag_list)


def clone_datasources_by_project(conn_source,
                                 conn_target,
                                 mapped_credentials_df,
                                 temp_dir,
                                 project,
                                 extraction_dir):
    source_project_dir = f"{temp_dir}/datasources/source/{project}"
    target_project_dir = f"{temp_dir}/datasources/target/{project}"
    os.makedirs(source_project_dir, exist_ok=False)
    os.makedirs(target_project_dir, exist_ok=False)
    project_datasources_df = mapped_credentials_df[mapped_credentials_df['target_project_name'] == project].copy()
    project_datasources_df['file_path'] = project_datasources_df['source_name'].apply(
        lambda file: f"{target_project_dir}/{file}.tdsx")
    project_datasources_df.fillna('', inplace=True)
    download_datasources(conn_source, project_datasources_df, download_dir=source_project_dir)
    modify_datasources_by_project(conn_source, conn_target, source_project_dir, target_project_dir, extraction_dir)
    published_datasources = publish_datasources_by_project(conn_target, project_datasources_df)
    update_datasources_by_project(conn_target, published_datasources)


def clone_datasources(conn_source,
                      conn_target,
                      datasource_ids=None,
                      credentials_file_path=None,
                      temp_dir=None,
                      overwrite_policy=None):
    temp_dir, extraction_dir = set_temp_dirs(temp_dir)
    try:
        mapped_credentials_df = get_cloning_df(conn_source,
                                               conn_target,
                                               datasource_ids,
                                               credentials_file_path)
        for project in mapped_credentials_df['target_project_name'].unique():
            clone_datasources_by_project(conn_source,
                                         conn_target,
                                         mapped_credentials_df,
                                         temp_dir,
                                         project,
                                         extraction_dir)
    finally:
        delete_temp_files(temp_dir)
