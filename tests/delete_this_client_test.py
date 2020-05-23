import datetime
import logging
import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine


from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import flatten_dict_column
from tableau_api_lib.utils.querying import get_projects_dataframe, get_workbooks_dataframe, get_datasources_dataframe, \
    get_flows_dataframe

#  if preferred, you can move this to a JSON file and import it rather than keep it here
tableau_server_config = {
    'default': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.7',
        'personal_access_token_name': 'api-demo-apr15',
        'personal_access_token_secret': '<SECRET>',
        # 'username': '<USERNAME>',  #  use username or personal_access_token_name, but not both
        # 'password': '<PASSWORD>',  #  use password or personal_access_token_secret, but not both
        'site_name': 'estam2',  # this is your 'pretty' site name; set to '' for the Default site
        'site_url': 'estam2',  # this is your site's content url; set to '' for the Default site
        'postgresql_db': 'postgresql://readonly:password@stw-poc-04:8060/workgroup'
    }
}


#  if preferred, you can move this to a JSON file and import it rather than keep it here
process_config = {
    'default': {
        'archive_project_name': 'archive',  # the name of the archive project on Tableau Server
        'archive_project_id': '',  # the ID of the archive project
        'archive_dir': r'C:\Users\estam\Documents\Development\sandbox\client_dev\archive',  # local archive dir
        'date_col': 'last_interaction_date',
        'days_inactive_limit': 90,  # the number of days of inactivity before content is archived (Tableau Server)
        'days_archived_limit': 90,  # the number of days before content is deleted from Tableau Server (stored locally)
        'log_file': r'C:\Users\estam\Documents\Development\sandbox\client_dev\event_log.log'
    }
}

last_interactions_pg_query = """
WITH events AS (
    SELECT DISTINCT MAX(DATE_TRUNC('day', created_at)) AS last_interaction_date
                  , hist_target_site_id
                  , hist_workbook_id
                  , hist_datasource_id
                  , hist_flow_id
                  , historical_event_type_id
    FROM historical_events
    GROUP BY 2, 3, 4, 5, 6
),
     event_types AS (
         SELECT DISTINCT type_id
                       , name
                       , action_type
         FROM historical_event_types
     ),
     temp_sites AS (
         SELECT DISTINCT name
                       , id
                       , site_id AS hist_site_id
         FROM hist_sites
     ),
     temp_workbooks AS (
         SELECT DISTINCT name
                       , id
                       , workbook_id AS hist_workbook_id
         FROM hist_workbooks
     ),
     temp_datasources AS (
         SELECT DISTINCT name
                       , id
                       , datasource_id AS hist_datasource_id
         FROM hist_datasources
     ),
     temp_flows AS (
         SELECT DISTINCT name
                       , id
                       , flow_id AS hist_flow_id
         FROM hist_flows
     ),
     sites AS (
         SELECT DISTINCT name   AS site_name
                       , id
                       , luid   AS site_id
         FROM sites
     ),
     workbooks AS (
         SELECT DISTINCT name       AS content_name
                       , 'workbook' AS content_type
                       , id
                       , luid       AS content_id
         FROM workbooks
     ),
     datasources AS (
         SELECT DISTINCT name         AS content_name
                       , 'datasource' AS content_type
                       , id
                       , luid         AS content_id
         FROM datasources
     ),
     flows AS (
         SELECT DISTINCT name   AS content_name
                       , 'flow' AS content_type
                       , id
                       , luid   AS content_id
         FROM flows
     )

SELECT MAX(last_interaction_date) AS last_interaction_date
     , content_type
     , site_name
     , site_id
     , content_name
     , content_id
FROM (
         SELECT DISTINCT last_interaction_date
                       , action_type
                       , site_name
                       , site_id
                       , workbooks.content_type
                       , workbooks.content_name
                       , workbooks.content_id
         FROM events
                  LEFT JOIN event_types ON events.historical_event_type_id = event_types.type_id
                  LEFT JOIN temp_sites ON events.hist_target_site_id = temp_sites.id
                  LEFT JOIN sites ON temp_sites.hist_site_id = sites.id
                  LEFT JOIN temp_workbooks ON events.hist_workbook_id = temp_workbooks.id
                  LEFT JOIN workbooks ON temp_workbooks.hist_workbook_id = workbooks.id

         UNION

         SELECT DISTINCT last_interaction_date
                       , action_type
                       , site_name
                       , site_id
                       , datasources.content_type
                       , datasources.content_name
                       , datasources.content_id
         FROM events
                  LEFT JOIN event_types ON events.historical_event_type_id = event_types.type_id
                  LEFT JOIN temp_sites ON events.hist_target_site_id = temp_sites.id
                  LEFT JOIN sites ON temp_sites.hist_site_id = sites.id
                  LEFT JOIN temp_datasources ON events.hist_datasource_id = temp_datasources.id
                  LEFT JOIN datasources ON temp_datasources.hist_datasource_id = datasources.id

         UNION

         SELECT DISTINCT last_interaction_date
                       , action_type
                       , site_name
                       , site_id
                       , flows.content_type
                       , flows.content_name
                       , flows.content_id
         FROM events
                  LEFT JOIN event_types ON events.historical_event_type_id = event_types.type_id
                  LEFT JOIN temp_sites ON events.hist_target_site_id = temp_sites.id
                  LEFT JOIN sites ON temp_sites.hist_site_id = sites.id
                  LEFT JOIN temp_flows ON events.hist_flow_id = temp_flows.id
                  LEFT JOIN flows ON temp_flows.hist_flow_id = flows.id
     ) event_data
WHERE event_data.content_id IS NOT NULL
  AND (action_type = 'Send E-Mail' OR action_type = 'Access' OR action_type = 'Create' OR action_type = 'Publish')
GROUP BY 2, 3, 4, 5, 6
ORDER BY content_id DESC;
"""


def initialize_logging(config, env='default') -> None:
    """
    Initializes log entries to the designated log file. The logging level is set to INFO.
    :param dict config: a dict or json object containing environment variables for Tableau Server
    :param str env: the key associated with the environment variables you will use to authenticate to Tableau Server
    :return: None
    """
    logging.basicConfig(filename=config[env]['log_file'], level=logging.INFO)
    logging.info(f'Process started at {datetime.datetime.now()}')


def sign_in(config, env='default') -> TableauServerConnection:
    """
    Returns an active connection to Tableau Server containing a valid X-Auth token.
    :param dict config: a dict or json object containing environment variables for Tableau Server
    :param str env: the key associated with the environment variables you will use to authenticate to Tableau Server
    :return: TableauServerConnection
    """
    conn = TableauServerConnection(config, env)
    response = conn.sign_in()
    if response.status_code == 200:
        logging.info(f"Successfully signed in: server='{conn.server}' site='{conn.site_name}'")
        return conn
    else:
        logging.critical(f"Authentication to Tableau Server failed: '{response.content}'")
        raise Exception(f"Authentication to Tableau Server failed: '{response.content}'")


def sign_out(conn) -> None:
    """
    Signs out from the active Tableau Server connection, invalidating the connection's X-Auth token.
    :param TableauServerConnection conn:
    :return: None
    """
    response = conn.sign_out()
    if response.status_code == 204:
        logging.info("Successfully signed out")
    else:
        logging.critical(f"There was an error signing out: '{response.status_code}'")
        raise Exception(f"There was an error signing out: '{response.status_code}'")
    logging.info(f'Process completed at {datetime.datetime.now()}')


def set_process_project_id(conn,
                           config,
                           project_name_key='archive_project_name',
                           project_id_key='archive_project_id',
                           env='default') -> dict:
    """
    Sets project ID values for the 'archive' and 'resurrect' projects used by the process.
    :param TableauServerConnection conn: the active Tableau Server connection
    :param dict config: the dict or JSON object containing process configuration details
    :param str project_name_key: the dict key that points to the project's name
    :param str project_id_key: the dict key that points to the project's ID
    :param str env: the config environment to reference; defaults to 'default'
    :return: config with project IDs
    """
    logging.info(f"Configuring the project ID for project '{config[env][project_name_key]}'")
    projects_df = get_projects_dataframe(conn)
    archive_project_df = projects_df.loc[projects_df['name'] == process_config[env][project_name_key]]
    try:
        process_config[env][project_id_key] = archive_project_df['id'].to_list()[0]
        logging.info(f"The project ID is: '{process_config[env][project_id_key]}'")
        return process_config
    except IndexError:
        logging.warning(f"The project '{process_config[env][project_name_key]} does not exist yet... creating project'")
        response = conn.create_project(project_name=process_config[env][project_name_key])
        try:
            process_config[env][project_id_key] = response.json()['project']['id']
            logging.info(f"The project ID is: '{process_config[env][project_id_key]}'")
            return process_config
        except KeyError:
            logging.critical(f"Failed to configure the {config[env][project_name_key]} project: '{response.content}'")
            raise Exception(f"Failed to configure the {config[env][project_name_key]} project: '{response.content}'")


def get_last_content_interactions(query=last_interactions_pg_query,
                                  config=tableau_server_config,
                                  env='default') -> pd.DataFrame:
    """
    Queries the Tableau Server Postgresql 'repository' database to obtain the last interaction date for content.
    :param str query: the SQL query to be executed against the Postgresql database
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: pd.DataFrame
    """
    logging.info("Querying the Tableau Server PostgreSQL database for latest content interactions... ")
    str_cols = ['content_type', 'site_name', 'site_id', 'content_name', 'content_id']
    try:
        sql_engine = create_engine(config[env]['postgresql_db'])
        last_content_interactions_df = pd.read_sql_query(query, con=sql_engine)
        last_content_interactions_df[str_cols] = last_content_interactions_df[str_cols].astype(str)
        return last_content_interactions_df
    except sqlalchemy.exc.OperationalError:
        logging.critical(f"Failed to establish a connection to PostgreSQL:\n{config[env]['postgresql_db']}")
        raise Exception(f"Failed to establish a connection to PostgreSQL:\n{config[env]['postgresql_db']}")


def create_local_dirs(config=process_config, env='default') -> None:
    """
    Creates local directories to store archived files before deleting them from Tableau Server.
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: None
    """
    logging.info("Creating local dirs...")
    if not os.path.exists(config[env]['archive_dir']):
        logging.warning("Dir '{config[env]['archive_dir']}' does not exist; creating directory...")
        os.makedirs(config[env]['archive_dir'])
    for content_type in ['workbooks', 'datasources', 'flows']:
        try:
            os.makedirs(f"{config[env]['archive_dir']}/{content_type}")
            logging.info(f"Created dir '{config[env]['archive_dir']}/{content_type}'")
        except FileExistsError:
            logging.info(f"Dir '{config[env]['archive_dir']}/{content_type}' already exists")


def enforce_date_formats(df, date_col='updatedAt'):
    """
    Converts the specified date column of a Pandas DataFrame to a datetime value.
    :param pd.DataFrame df: the Pandas DataFrame containing a date column to convert to datetime
    :param str date_col: the name of the date columns to convert
    :return: pd.DataFrame
    """
    logging.info(f"Converting the column '{date_col}' to datetime...")
    modified_df = df.copy()
    modified_df[date_col] = modified_df[date_col].apply(lambda x: datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%SZ'))
    return modified_df


def calculate_days_inactive(df, date_col='updatedAt') -> pd.DataFrame:
    """
    Calculates the number of days content has been inactive, comparing the current date to the specified 'date_col'.
    :param pd.DataFrame df: the Pandas DataFrame containing a date column
    :param str date_col: the name of the date column containing the date content was last updated
    :return: pd.DataFrame
    """
    logging.info(f"Calculating the number of days content has been inactive...")
    try:
        df['days_inactive'] = datetime.datetime.now() - df[date_col]
        df['days_inactive'] = df['days_inactive'].apply(lambda date: date.days)
    except KeyError:
        raise Exception("No date column named '{date_col}' was available in the {type(df)} provided")
    return df


def get_content_to_archive(df, content_type, config=process_config, env='default'):
    """
    Returns a Pandas DataFrame populated with content that should be archived.
    :param pd.DataFrame df: the Pandas DataFrame whose content will be assessed for archiving eligibility
    :param str content_type: the type of content being archived [workbooks, datasources, flows]
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: pd.DataFrame
    """
    logging.info(f"Getting {content_type} to archive...")
    try:
        content_to_archive_df = df.loc[~df['project_name'].isin([[process_config[env]['archive_project_name']]])]
        content_to_archive_df = content_to_archive_df.loc[df['days_inactive'] >= config[env]['days_inactive_limit']]
        logging.info(f"{content_to_archive_df.shape[0]} {content_type} will be archived")
        return content_to_archive_df
    except KeyError:
        logging.critical(f"No column named 'days_inactive' was available in the {type(df)} provided.")
        raise Exception(f"No column named 'days_inactive' was available in the {type(df)} provided.")


def archive_content(conn, df, content_type, config=process_config, env='default') -> None:
    """
    Moves content from the current project to the archive project specified in the given config dict.
    :param TableauServerConnection conn: the active Tableau Server connection
    :param pd.DataFrame df: the Pandas DataFrame whose content will be assessed for removal
    :param str content_type: the type of content being archived [workbooks, datasources, flows]
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: None
    """
    logging.info(f"Archiving {content_type}...")
    for index, content in df.iterrows():
        logging.info(f"Archiving {content_type} '{content['name']}' (id='{content['id']}')")
        if content_type == 'workbooks':
            response = conn.update_workbook(workbook_id=content['id'],
                                            new_project_id=config[env]['archive_project_id'])
        if content_type == 'datasources':
            response = conn.update_data_source(datasource_id=content['id'],
                                               new_project_id=config[env]['archive_project_id'])
        if content_type == 'flows':
            response = conn.update_flow(flow_id=content['id'],
                                        new_project_id=config[env]['archive_project_id'])
        if response.status_code == 200:
            logging.info(f"{content_type} successfully archived")
        else:
            logging.critical(f"Failed to archive {content_type} '{content['name']}':\n{response.content}")


def get_content_to_remove(df, content_type, config=process_config, env='default'):
    """
    Returns a Pandas DataFrame populated with content that should be downloaded and removed from Tableau Server.
    :param pd.DataFrame df: the Pandas DataFrame whose content will be assessed for removal
    :param str content_type: the type of content being downloaded and removed [workbooks, datasources, flows]
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: pd.DataFrame
    """
    logging.info(f"Getting {content_type} to download and remove...")
    try:
        content_to_remove_df = df.loc[df['project_name'].isin([process_config[env]['archive_project_name']])]
        content_to_remove_df = content_to_remove_df.loc[df['days_inactive'] >= config[env]['days_archived_limit']]
        content_to_remove_df['content_type'] = content_type
        logging.info(f"{content_to_remove_df.shape[0]} {content_type} will be downloaded and removed")
        return content_to_remove_df
    except KeyError:
        logging.critical(f"No column named 'days_inactive' was available in the {type(df)} provided.")
        raise Exception(f"No column named 'days_inactive' was available in the {type(df)} provided.")


def download_content(conn, df, content_type, config=process_config, env='default') -> None:
    """
    Downloads workbooks from the archived project to a local directory.
    :param TableauServerConnection conn: the active Tableau Server connection
    :param pd.DataFrame df: the Pandas DataFrame whose content will be downloaded
    :param str content_type: the type of content being downloaded and removed [workbooks, datasources, flows]
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: None
    """
    logging.info(f"Downloading {content_type}...")
    standard_file_types = {'datasources': 'tdsx', 'flows': 'tlsx'}
    for index, content in df.iterrows():
        logging.info(f"Downloading {content_type} '{content['name']}' (id='{content['id']}')")
        if content_type == 'workbooks':
            response = conn.download_workbook(workbook_id=content['id'])
        elif content_type == 'datasources':
            response = conn.download_data_source(datasource_id=content['id'])
        else:
            response = conn.download_flow(flow_id=content['id'])
        if response.status_code == 200:
            if content_type == 'workbooks':
                file_type = 'twb' if response.headers['Content-Type'] == 'application/xml' else 'twbx'
            else:
                file_type = standard_file_types[content_type]
            file_path = f"{config[env]['archive_dir']}/{content_type}/{content['name']}.{file_type}"
            logging.info(f"Writing {content_type[:-1]} file to path '{file_path}'...")
            with open(file_path, 'wb') as file:
                file.write(response.content)
            logging.info("File write successful")
        else:
            logging.critical(f"Failed to download {content_type[:-1]} '{content['name']}:\n{response.content}")
            raise Exception(f"Terminating process to avoid deleting a possibly irrecoverable {content_type[:-1]}")


def remove_content(conn, df, content_type, config=process_config, env='default') -> None:
    """
    Removes content from Tableau Server.
    :param TableauServerConnection conn: the active Tableau Server connection
    :param pd.DataFrame df: the Pandas DataFrame whose content will be removed from Tableau Server.
    :param str content_type: the type of content being downloaded and removed [workbooks, datasources, flows]
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: None
    """
    logging.info(f"Removing {content_type} from Tableau Server...")
    for index, content in df.iterrows():
        logging.info(f"Removing {content_type} '{content['name']}' (id='{content['id']}')")
        if content_type == 'workbooks':
            response = conn.delete_workbook(workbook_id=content['id'])
        elif content_type == 'datasources':
            response = conn.delete_data_source(datasource_id=content['id'])
        else:
            response = conn.delete_flow(flow_id=content['id'])
        if response.status_code == 204:
            logging.info(f"{content_type} removal successful")
        else:
            logging.warning(f"Failed to delete {content_type} '{content['name']}:\n{response.content}")


def process_content(conn,
                    content_type,
                    last_content_interactions_df,
                    config=process_config,
                    env='default') -> None:
    """
    Get content details from Tableau Server and archive / remove files accordingly.
    :param TableauServerConnection conn: the active Tableau Server connection
    :param str content_type: the type of content being downloaded and removed [workbooks, datasources, flows]
    :param pd.DataFrame last_content_interactions_df: a Pandas DataFrame with details of the last content interactions
    :param dict config: the dict or JSON object containing process configuration details
    :param str env: the config environment to reference; defaults to 'default'
    :return: None
    """
    logging.info(f"Querying {content_type} for content to archive...")
    if content_type == 'workbooks':
        df = get_workbooks_dataframe(conn)
    elif content_type == 'datasources':
        df = get_datasources_dataframe(conn)
    else:
        df = get_flows_dataframe(conn)
    if df.shape[0] > 0:
        df = df.merge(last_content_interactions_df, how='left', left_on='id', right_on='content_id')
        df = flatten_dict_column(df, keys=['name', 'id'], col_name='project')
        df = enforce_date_formats(df)
        df = calculate_days_inactive(df, date_col=config[env]['date_col'])
        content_to_archive = get_content_to_archive(df, content_type)
        if content_to_archive.shape[0] > 0:
            archive_content(conn, content_to_archive, content_type)
        content_to_remove_df = get_content_to_remove(df, content_type)
        if content_to_remove_df.shape[0] > 0:
            download_content(conn, content_to_remove_df, content_type)
            remove_content(conn, content_to_remove_df, content_type)
    else:
        logging.info(f"No {content_type} were found")


def main():
    global tableau_server_config, process_config, last_interactions_pg_query
    initialize_logging(process_config, env='default')
    conn = sign_in(tableau_server_config, env='default')
    process_config = set_process_project_id(conn, process_config)
    last_content_interactions_df = get_last_content_interactions()
    create_local_dirs()
    process_content(conn, 'workbooks', last_content_interactions_df)
    process_content(conn, 'datasources', last_content_interactions_df)
    process_content(conn, 'flows', last_content_interactions_df)
    sign_out(conn)


if __name__ == '__main__':
    main()
