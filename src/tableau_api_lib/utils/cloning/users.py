"""
Logical Flow:
1) get users from source connection
2) get users from target connection
3) if a user mapping file is provided, join it to source users
4) get overlapping users
    4a) if user mapping file is provided, do an inner join on target users
5) if there are any overlapping users and no overwrite policy is set, raise an exception
6) if there are any overlapping users and the overwrite policy is 'overwrite', delete the overlapping target users
7) for each user on the source connection, create a new user on the target connection as defined by the mapping file
    7a) if no mapping file is provided, create a new user on the target connection matching the source connection

*Note the limitation of Tableau Online: cannot update user full names... so they are stuck with a busted full name

"""


import pandas as pd

from tableau_api_lib.exceptions import ContentOverwriteDisabled
from tableau_api_lib.utils.querying.users import get_users_dataframe


def verify_mapping_file_columns(mapping_file_columns, required_columns) -> None:
    """
    Throws an error if required columns in the mapping file are not present.
    :param list mapping_file_columns: list of columns present in the mapping file
    :param list required_columns: list of required columns for mapping users
    :return: None
    """
    missing_columns = [col for col in required_columns if col not in mapping_file_columns]
    if any(missing_columns):
        raise ValueError(""""
        The mapping file provided is missing the following required columns: {}
        """.format(missing_columns))
    pass


def get_mapping_file_df(mapping_file_path) -> pd.DataFrame:
    """
    Creates a Pandas DataFrame from the CSV file located in the mapping_file_path.
    :param str mapping_file_path: the file path to the mapping file CSV
    :return: Pandas DataFrame
    """
    required_columns = ['source_site_name', 'source_username', 'target_site_name', 'target_username']
    try:
        mapping_file_df = pd.read_csv(mapping_file_path)
        verify_mapping_file_columns(list(mapping_file_df.columns), required_columns)
        return mapping_file_df
    except FileNotFoundError:
        raise FileNotFoundError("The mapping file '{}' could not be found. Please verify the file path provided.")


def get_source_user_df(conn_source, usernames=None) -> pd.DataFrame:
    """
    Creates a Pandas DataFrame populated with data for users on the source Tableau Server connection.
    :param class conn_source: the source Tableau Server connection
    :param list usernames: (optional) a subset of users; if specified, only these users will appear in the Dataframe
    :return: Pandas DataFrame
    """
    user_df = get_users_dataframe(conn_source)
    if usernames:
        user_df = user_df[user_df['name'].isin(usernames)]
    user_df.rename(columns={
        'name': 'source_username',
        'fullName': 'source_full_name',
        'email': 'source_email',
        'siteRole': 'source_site_role',
        'authSetting': 'source_auth_setting'
    }, inplace=True, errors='ignore')
    if 'source_auth_setting' not in user_df.columns:
        user_df['source_auth_setting'] = 'ServerDefault'
    return user_df


def get_target_user_df(conn_target, usernames=None) -> pd.DataFrame:
    """
    Creates a Pandas DataFrame populated with data for users on the target Tableau Server connection.
    :param class conn_target: the source Tableau Server connection
    :param list usernames: (optional) a subset of users; if specified, only these users will appear in the Dataframe
    :return: Pandas DataFrame
    """
    user_df = get_users_dataframe(conn_target)
    if usernames:
        user_df = user_df[user_df['name'].isin(usernames)]
    user_df.rename(columns={
        'name': 'target_username',
        'fullName': 'target_full_name',
        'email': 'target_email',
        'siteRole': 'target_site_role',
        'authSetting': 'target_auth_setting'
    }, inplace=True, errors='ignore')
    return user_df


def get_overlapping_usernames(source_user_df,
                              target_user_df,
                              mapping_file_path=None) -> list:
    """
    Creates a list of usernames that appear in the source connection and the target connection.
    :param pd.DataFrame source_user_df: a Pandas DataFrame populated with source connection user details
    :param pd.DataFrame target_user_df: a Pandas DataFrame populated with target connection user details
    :param str mapping_file_path: (optional) the file path for the mapping file CSV
    :return: overlapping_usernames
    """
    if mapping_file_path:
        source_usernames = set(source_user_df['target_username'])
    else:
        source_usernames = set(source_user_df['source_username'])
    target_usernames = set(target_user_df['target_username'])
    overlapping_usernames = source_usernames.intersection(target_usernames)
    return list(overlapping_usernames)


def get_mapped_user_df(user_df, mapping_file_path) -> pd.DataFrame:
    """
    Creates a Pandas DataFrame that combines source user details with user mapping details.
    :param pd.DataFrame user_df: a Pandas Dataframe with source connection user details
    :param str mapping_file_path: the file path for the mapping file CSV
    :return: user_df
    """
    if mapping_file_path:
        mapping_file_df = get_mapping_file_df(mapping_file_path)
        user_df = user_df.merge(mapping_file_df,
                                left_on='source_username',
                                right_on='source_username',
                                suffixes=('_delete', None))
    return user_df


def delete_users(conn, target_user_df) -> list:
    """
    Removes users from the specified connection.
    :param class conn:
    :param pd.DataFrame target_user_df:
    :return: list of HTTP responses
    """
    print("removing overlapping target users...")
    if conn.username in list(target_user_df['target_username']):
        target_user_df = target_user_df[~target_user_df['target_username'].isin([conn.username])]
        print("preserving user '{}', which was used to authenticate into the target Tableau Server."
              .format(conn.username))
    user_ids = target_user_df['id']
    responses = [conn.remove_user_from_site(user_id) for user_id in user_ids]
    [print(response) for response in responses]
    print("overlapping target users removed")
    return responses


def create_users(conn_target, source_user_df, mapping_file_path, server_type) -> list:
    """
    Create users on the target server, based on user details from the source server.
    :param class conn_target: the target server connection
    :param DataFrame source_user_df: the source user DataFrame containing details for the users being created
    :param mapping_file_path: the file path to the user mapping file CSV
    :param server_type: (optional) the product variety ['tableau_server', 'tableau_online']
    :return: list of HTTP responses
    """
    responses = []
    field_prefix = get_field_prefix(mapping_file_path)
    source_user_df = source_user_df[~source_user_df[field_prefix + 'site_role'].isin(['ServerAdministrator'])]
    if server_type == 'tableau_server':
        for index, row in source_user_df.iterrows():
            response = conn_target.add_user_to_site(
                user_name=row[field_prefix + 'username'],
                site_role=row[field_prefix + 'site_role'],
                auth_setting=row[field_prefix + 'auth_setting'])
            responses.append(response)
    if server_type == 'tableau_online':
        for index, row in source_user_df.iterrows():
            response = conn_target.add_user_to_site(
                user_name=row[field_prefix + 'email'],
                site_role=row[field_prefix + 'site_role'],
                auth_setting=row[field_prefix + 'auth_setting'])
            responses.append(response)
    for response in responses:
        print(response.content)
    return responses


def reset_connection(conn) -> object:
    """
    Resets the connection to Tableau Server by signing out and signing in again.
    :param class conn: the Tableau Server connection to reset
    :return: TableauServerConnection
    """
    conn.sign_out()
    conn.sign_in()
    return conn


def get_field_prefix(mapping_file_path) -> str:
    """
    Get the field prefix, depending on whether or not a mapping_file_path was provided.
    :param str mapping_file_path: the file path to the user mapping file
    :return: field_prefix
    """
    if mapping_file_path:
        field_prefix = 'target_'
    else:
        field_prefix = 'source_'
    return field_prefix


def update_users(conn_target, source_user_df, mapping_file_path) -> list:
    """
    Updates users on the target connection to mirror values on the source connection.
    :param class conn_target: the target Tableau Server connection
    :param pd.DataFrame source_user_df: a Pandas DataFrame with source user details
    :param str mapping_file_path: the file path to the user mapping file
    :return: list of HTTP responses
    """
    print("updating users...")
    reset_connection(conn_target)
    target_user_df = get_target_user_df(conn_target)
    target_user_df = target_user_df[~target_user_df['target_username'].isin([conn_target.username])]
    field_prefix = get_field_prefix(mapping_file_path)
    combined_user_df = target_user_df.merge(source_user_df,
                                            left_on='target_username',
                                            right_on=field_prefix + 'username',
                                            suffixes=('_update', None))
    responses = []
    for index, row in combined_user_df.iterrows():
        response = conn_target.update_user(
            user_id=row['id_update'],
            new_full_name=row[field_prefix + 'full_name'],
            new_email=row[field_prefix + 'email'],
            new_site_role=row[field_prefix + 'site_role'],
            new_auth_setting=row[field_prefix + 'auth_setting'])
        responses.append(response)
    for response in responses:
        print(response.content)
    print("users updated")
    return responses


def fill_expected_columns(source_user_df):
    """
    Populates any missing 'expected_columns' with None value if they do not yet exist.
    :param pd.DataFrame source_user_df: a Pandas DataFrame with source user details
    :return: source_user_df
    """
    expected_columns = ['source_email', 'source_full_name', 'source_site_role', 'source_auth_setting']
    source_columns = source_user_df.columns
    for expected_column in expected_columns:
        if expected_column not in source_columns:
            source_user_df[expected_column] = None
    return source_user_df


def process_overlapping_usernames(conn_target,
                                  target_user_df,
                                  overlapping_usernames,
                                  overwrite_policy) -> None:
    """
    Processes overlapping usernames depending on the overwrite policy specified.
    :param class conn_target: the target Tableau Server connection
    :param pd.DataFrame target_user_df: a Pandas DataFrame with target user details
    :param list overlapping_usernames: a list of usernames present on both the source and target connections
    :param str overwrite_policy: must be set to 'overwrite' to enable overwriting existing content
    :return:
    """
    if any(overlapping_usernames) and not overwrite_policy:
        raise ContentOverwriteDisabled('users')
    if any(overlapping_usernames) and overwrite_policy == 'overwrite':
        delete_users(conn_target, target_user_df[target_user_df['target_username'].isin(overlapping_usernames)])
    pass


def clone_users(conn_source,
                conn_target,
                server_type='tableau_server',
                usernames=None,
                mapping_file_path=None,
                overwrite_policy=None) -> list:
    """
    Clones users from the source Tableau Server connection to the target Tableau Server connection.
    :param class conn_source: the source Tableau Server connection
    :param class conn_target: the target Tableau Server connection
    :param str server_type: (optional) the product variety ['tableau_server', 'tableau_online']
    :param list usernames: (optional) a list of users to clone; if provided, only these users are cloned
    :param str mapping_file_path: (optional) the path to a file mapping users from source to target
    :param  overwrite_policy: (optional) must be set to 'overwrite' to enable overwriting existing content
    :return:
    """
    source_user_df = get_source_user_df(conn_source, usernames)
    source_user_df = get_mapped_user_df(source_user_df, mapping_file_path)
    source_user_df = fill_expected_columns(source_user_df)
    target_user_df = get_target_user_df(conn_target, usernames)
    overlapping_usernames = get_overlapping_usernames(source_user_df, target_user_df, mapping_file_path)
    process_overlapping_usernames(conn_target, target_user_df, overlapping_usernames, overwrite_policy)
    cloned_users = create_users(conn_target, source_user_df, mapping_file_path, server_type)
    cloned_users = update_users(conn_target, source_user_df, mapping_file_path)
    # if server_type == 'tableau_server':
    #     cloned_users = update_users(conn_target, source_user_df, mapping_file_path)
    return cloned_users
