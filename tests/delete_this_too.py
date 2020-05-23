import pandas as pd
import zipfile
import os
import urllib
import shutil

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import extract_pages
from tableau_api_lib.utils.cloning import clone_projects, clone_sites, clone_users, clone_groups
from tableau_api_lib.utils.querying import get_users_dataframe, get_datasources_dataframe, get_groups_dataframe, \
    get_group_users_dataframe, get_workbooks_dataframe, get_projects_dataframe, get_sites_dataframe, \
    get_user_favorites_dataframe, get_schedules_dataframe, get_flows_dataframe, get_workbook_connections_dataframe, \
    get_active_site_id, get_views_dataframe, get_embedded_datasources_dataframe, get_subscriptions_dataframe
from tableau_api_lib.utils.cloning.workbooks import clone_workbooks


TEST_WORKBOOK_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\workbook_with_extract2.twbx'
TEST_PROJECT_NAME = 'Test'

pd.set_option('display.width', 600)
pd.set_option('display.max_columns', 10)


def get_test_project_id(connection):
    projects = connection.query_projects().json()['projects']['project']
    for project in projects:
        if project['name'] == TEST_PROJECT_NAME:
            return project['id']
    return projects.pop()['id']


tableau_server_config = {
    'tableau_prod': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.6',
        'username': 'estam',
        'password': 'Act1Andariel!',
        'site_name': 'estam',
        'site_url': 'estam'
    },
    'tableau_test': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.6',
        'username': 'estam',
        'password': 'Act1Andariel!',
        'site_name': 'estam2',
        'site_url': 'estam2'
    },
    'tableau_test3': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.6',
        'username': 'estam',
        'password': 'Act1Andariel!',
        'site_name': 'estam3',
        'site_url': 'estam3'
    },
    'tableau_online': {
        'server': 'https://10ax.online.tableau.com',
        'api_version': '3.8',
        'username': 'elliott.stam@interworks.com',
        'password': 'Jarganuke1122!',
        'site_name': 'estamiwdev422309',
        'site_url': 'estamiwdev422309',
        'cache_buster': '',
        'temp_dir': ''
    }
}


conn_a = TableauServerConnection(tableau_server_config, env='tableau_prod')
conn_b = TableauServerConnection(tableau_server_config, env='tableau_test')
conn_c = TableauServerConnection(tableau_server_config, env='tableau_online')

response_a = conn_a.sign_in()
response_b = conn_b.sign_in()
response_c = conn_c.sign_in()

estam_default_project_id = '24eef575-e136-42e1-b3ec-16375d760937'
estam2_default_project_id = '060d734a-d320-4636-a80c-0ff29a5175ef'
tonline_default_project_id = '1d624d82-f659-4ef8-9825-de4f0ee0e7f5'
sample_tableau_twbx_id = 'ebe595d3-c7b8-4b12-b6ea-82db057776f7'
estam_sample_tableau_twb_id = 'aa0d80f4-fc1b-44e9-9c5f-148e6a858e91'
sample_workbook_id = '33176399-9a66-4df1-8873-fa974a6985cb'


def get_server_netloc(server_address):
    return urllib.parse.urlparse(server_address).netloc


def remap_xml_references(file, conn_source, conn_target):
    file = file.replace("xml:base='{}'".format(conn_source.server),
                        "xml:base='{}'".format(conn_target.server))
    file = file.replace("xml:base=&apos;{}".format(conn_source.server),
                        "xml:base=&apos;{}".format(conn_target.server))
    file = file.replace("path='/t/{}/".format(conn_source.site_url),
                        "path='/t/{}/".format(conn_target.site_url))
    file = file.replace("path=&apos;/t/{}/".format(conn_source.site_url),
                        "path=&apos;/t/{}/".format(conn_target.site_url))
    file = file.replace("site='{}'".format(conn_source.site_url),
                        "site='{}'".format(conn_target.site_url))
    file = file.replace("site=&apos;{}".format(conn_source.site_url),
                        "site=&apos;{}".format(conn_target.site_url))
    file = file.replace("server='{}'".format(get_server_netloc(conn_source.server)),
                        "server='{}'".format(get_server_netloc(conn_target.server)))
    file = file.replace("server=&apos;{}".format(get_server_netloc(conn_source.server)),
                        "server=&apos;{}".format(get_server_netloc(conn_target.server)))
    return file


def copy_dir_to_zip(path, zip_file_obj):
    for root, dirs, files in os.walk(path):
        for file in files:
            filename, file_extension = os.path.splitext(file)
            if file_extension not in ['.twb', '.tds']:
                zip_file_obj.write(os.path.join(root, file))


# def replace_unzipped_xml_file(conn_source, conn_target, tableau_file_name, tableau_file_dir, extraction_dir_path):
#     with open(tableau_file_dir + '/' + tableau_file_name, 'r', encoding='utf-8') as original_file:
#         file_contents = original_file.read()
#         file_contents = remap_xml_references(file_contents, conn_source, conn_target)
#     with open(extraction_dir_path + '/' + tableau_file_name, 'w', encoding='utf-8') as new_file:
#         new_file.write(file_contents)


def replace_unzipped_xml_file(file_path, conn_source, conn_target, extraction_dir_path):
    with open(file_path, 'r', encoding='utf-8') as original_file:
        file_contents = original_file.read()
        file_contents = remap_xml_references(file_contents, conn_source, conn_target)
    with open(extraction_dir_path + '/' + os.path.basename(file_path), 'w', encoding='utf-8') as new_file:
        new_file.write(file_contents)


def replace_zipped_xml_file(conn_source, conn_target, zip_file, tableau_file_base, extraction_dir_path):
    with zip_file.open(tableau_file_base, 'r') as file:
        file_contents = str(file.read(), 'utf-8')
        file_contents = remap_xml_references(file_contents, conn_source, conn_target)
    with open(extraction_dir_path + '/' + tableau_file_base, 'w', encoding='utf-8') as new_file:
        new_file.write(file_contents)


def generate_tableau_zipfile(extraction_dir_path, zip_file_name, xml_file_name):
    with zipfile.ZipFile(extraction_dir_path + '/' + zip_file_name, 'w', zipfile.ZIP_DEFLATED) as modified_zipfile:
        modified_zipfile.write(extraction_dir_path + '/' + xml_file_name, xml_file_name)
        if 'Data' in os.listdir(extraction_dir_path):
            original_dir = os.getcwd()
            os.chdir(extraction_dir_path)
            copy_dir_to_zip(path='Data', zip_file_obj=modified_zipfile)
            os.chdir(original_dir)


def get_tableau_filenames(origin_path, extract_dir_path):
    origin_base = os.path.basename(origin_path)
    extracted_files = os.listdir(extract_dir_path)
    try:
        xml_file_base = [file for file in extracted_files if os.path.splitext(file)[1] in ['.tds', '.twb']].pop()
    except IndexError:
        raise Exception(f"No .tds or .twb files were extracted in the directory '{extract_dir_path}'.")
    return origin_base, xml_file_base


def modify_tableau_zipfile(zipfile_path, conn_source, conn_target, extraction_dir_path, destination_dir):
    with zipfile.ZipFile(file=zipfile_path) as zip_file:
        zip_file.extractall(path=extraction_dir_path)
        zip_file_base, xml_file_base = get_tableau_filenames(zipfile_path, extraction_dir_path)
        replace_zipped_xml_file(conn_source, conn_target, zip_file, xml_file_base, extraction_dir_path)
    generate_tableau_zipfile(extraction_dir_path, zip_file_base, xml_file_base)


def delete_temp_files(temp_dir):
    for root, dirs, files in os.walk(temp_dir):
        for file in files:
            os.remove(os.path.join(root, file))
        for folder in dirs:
            shutil.rmtree(os.path.join(root, folder))


twbx_path = 'twbx_files/twbx3/twbx3.twbx'
twbx_extraction_path = 'twbx_files/zip_test_again'
twb_path = 'twbx_files/twb1_again.twb'
twb_extraction_path = 'twbx_files/twb1_test'


# replace_unzipped_xml_file(file_path=twb_path,
#                           conn_source=conn_a,
#                           conn_target=conn_c,
#                           extraction_dir_path='twbx_files/twb1_test')
#
#
# response = conn_c.publish_workbook('twbx_files/twb1_test/twb1_again.twb',
#                                    'twb1_again',
#                                    tonline_default_project_id,
#                                    server_address=urllib.parse.urlparse(conn_c.server).netloc,
#                                    connection_username='',
#                                    connection_password='')
# print(response.content)
#
# print(os.listdir(twb_extraction_path))
# delete_temp_files(twb_extraction_path)
# print(os.listdir(twb_extraction_path))


# modify_tableau_zipfile(twbx_path,
#                        extraction_dir_path=twbx_extraction_path,
#                        conn_source=conn_a,
#                        conn_target=conn_c)
#
# modify_tableau_zipfile('twbx_files/published ds1.tdsx',
#                        extraction_dir_path='twbx_files/datasource_test',
#                        conn_source=conn_a,
#                        conn_target=conn_c)
#
# modify_tableau_zipfile('twbx_files/snowflake_live.tdsx',
#                        extraction_dir_path='twbx_files/datasource_test2',
#                        conn_source=conn_a,
#                        conn_target=conn_c)
#
# print("publishing workbook...")
# response = conn_c.publish_workbook('twbx_files/zip_test_again/twbx3.twbx',
#                                    'twbx3_modified',
#                                    tonline_default_project_id,
#                                    server_address=[urllib.parse.urlparse(conn_c.server).netloc, 'interworks.snowflakecomputing.com'],
#                                    connection_username=['estam', 'estam'],
#                                    connection_password=['Act1Andariel!', 'Act1Andariel!'],
#                                    embed_credentials_flag=[True, True])
# print(response.content)
#
# delete_temp_files(twbx_extraction_path)


# response = conn_c.publish_workbook('twbx_files/zip_test_again/twbx3.twbx',
#                                    'twbx3_modified',
#                                    tonline_default_project_id,
#                                    server_address=['interworks.snowflakecomputing.com', urllib.parse.urlparse(conn_c.server).netloc, 'interworks.snowflakecomputing.com'],
#                                    connection_username=['estam', '', 'estam'],
#                                    connection_password=['Act1Andariel!', '', 'Act1Andariel!'],
#                                    embed_credentials_flag=[True, True, True])
# print(response.content)


# response = conn_c.publish_workbook('twbx_files/snowflake_live_twb.twb',
#                                    'snowflake_live_twb',
#                                    tonline_default_project_id,
#                                    server_address=urllib.parse.urlparse(conn_c.server).netloc,
#                                    connection_username='',
#                                    connection_password='')
#                                    # tonline_default_project_id,
#                                    # server_address='interworks.snowflakecomputing.com',
#                                    # connection_username='estam',
#                                    # connection_password='',
#                                    # embed_credentials_flag=False)
# print(response.content)

twbx_snowflake_test_live_id = "665a5e2d-4462-440b-9ad4-971117960fd0"
snowflake_live_twbx_id = "39fea8d3-24fa-45c5-8fd4-1a41a053f993"
snowflake_live_twb_id = "98207f78-b384-48e0-bef9-9c8c8b446f48"
twbx3_modified_id = "20e79fe5-8b04-450c-ab28-be286fefd387"
snowflake_live_id = "168fae09-d87a-4a2a-9810-f352ea2888ab"
published_ds1_id = "3a3c302f-eb4c-4490-90b8-dd722e0874ae"


# response = conn_c.query_workbook_connections(twbx3_modified_id).json()
# print(response)
# print(response['connections']['connection'])

# print(get_workbook_connections_dataframe(conn_c, twbx3_modified_id))
# print(get_datasources_dataframe(conn_c))
# print(get_datasources_dataframe(conn_c).columns)
# print(get_datasources_dataframe(conn_c)['id'])
#
# print(get_projects_dataframe(conn_a)[['name', 'id']])
# print(get_projects_dataframe(conn_c)[['name', 'id']])

# print(get_workbook_connections_dataframe(conn_c, twbx3_modified_id))

# all_workbook_connections = pd.DataFrame()
# all_workbooks = get_workbooks_dataframe(conn_c)
# for workbook_id in all_workbooks['id']:
#     wb_connections = get_workbook_connections_dataframe(conn_c, workbook_id).drop(columns=['id'])
#     wb_connections = wb_connections[~wb_connections['userName'].isin([None, ''])]
#     all_workbook_connections = all_workbook_connections.append(wb_connections, sort=False)
#
#
# all_workbook_connections = all_workbook_connections[['serverAddress', 'userName', 'serverPort']].drop_duplicates()
#
# wb_conn_df = get_workbook_connections_dataframe(conn_c, twbx3_modified_id)
# joined_dfs = wb_conn_df.merge(all_workbook_connections,
#                               left_on=['serverAddress', 'userName'],
#                               right_on=['serverAddress', 'userName'],
#                               suffixes=(None, '_duplicate'))
#
# print(joined_dfs)

# print(conn_a.query_workbooks_for_site().json())
# print(conn_a.query_workbook_connections('33176399-9a66-4df1-8873-fa974a6985cb').json())

# response = get_workbooks_dataframe(conn_a)
# print(get_workbooks_dataframe(conn_a)[['name', 'id']])

# workbooks_df = get_workbooks_dataframe(conn_a)
# print(workbooks_df[['name', 'id']])

# views_df = get_views_dataframe(conn_a)
# print(views_df.columns)
# print(views_df[['workbook', 'name', 'id']])

TEST_VIEW_ID = '892dc253-d9c3-477e-a5a4-2add36102000'
TEST_WORKBOOK_ID = '82021bf4-f1d8-4e9b-9701-3374a026cc78'
TEST_GROUP_ID = 'd36d4a8a-9dbe-435b-b0a5-c0cf7fa153b0'
TEST_SCHEDULE_ID = 'f1846ec7-b1e2-45f0-b946-d3503900d431'
TEST_CONTENT_ID = '89fbbd98-3c80-41b0-b8b1-2454f19b9c3e'
TEST_CONTENT_TYPE = 'view'


print("groups_df: \n", get_groups_dataframe(conn_a))

print("existing subscriptions: \n", get_subscriptions_dataframe(conn_a))

print("group_users: \n", get_group_users_dataframe(conn_a, group_id=TEST_GROUP_ID))


def get_duplicate_subscriptions(subscriptions_df, subject, content_id, user_id):
    duplicates_query = f"subject == '{subject}' & content_id == '{content_id}' & user_id == '{user_id}'"
    duplicate_subscriptions = subscriptions_df.query(duplicates_query)
    return duplicate_subscriptions


subscriptions_df = get_subscriptions_dataframe(conn_a)

duplicate_subscriptions = get_duplicate_subscriptions(subscriptions_df, 'sample subscription', '89fbbd98-3c80-41b0-b8b1-2454f19b9c3e', '60e82c40-7dc1-4251-90b4-6f789c2624c6')
print(duplicate_subscriptions)


def create_unique_subscriptions(conn, group_users_df, subscriptions_df, subject, content_id, content_type, schedule_id):
    responses = []
    for index, user in group_users_df.iterrows():
        response = None
        duplicate_subscriptions = get_duplicate_subscriptions(subscriptions_df=subscriptions_df,
                                                              subject=subject,
                                                              content_id=content_id,
                                                              user_id=user['id'])
        if not any(duplicate_subscriptions.index):
            response = conn.create_subscription(subscription_subject=subject,
                                                content_type=content_type,
                                                content_id=content_id,
                                                schedule_id=schedule_id,
                                                user_id=user['id'])
        responses.append(response)
    return responses


responses = create_unique_subscriptions(conn_a,
                                        group_users_df=get_group_users_dataframe(conn_a, TEST_GROUP_ID),
                                        subscriptions_df=subscriptions_df,
                                        subject='Group Subscription Tutorial',
                                        content_id=TEST_CONTENT_ID,
                                        content_type=TEST_CONTENT_TYPE,
                                        schedule_id=TEST_SCHEDULE_ID)
print(responses)


# def get_unique_subscriptions(subscriptions_df, subject, content_id, user_id):

print(get_schedules_dataframe(conn_a))

df = get_schedules_dataframe(conn_a).\
    applY(lambda x: print(x))


# response = conn_a.create_extracts_for_workbook(workbook_id='c8c58e90-8a56-4183-a3f6-417706638eae',
#                                                extract_all_datasources_flag=False,
#                                                datasource_ids=['1fb4b25f-5215-4d11-94ec-27e041f2ca97'])
# print(response.json())

# response = conn_a.create_extracts_for_workbook(workbook_id='c8c58e90-8a56-4183-a3f6-417706638eae',
#                                                extract_all_datasources_flag=True)
# print(response.json())

# response = conn_a.delete_extracts_from_workbook(workbook_id='c8c58e90-8a56-4183-a3f6-417706638eae')
# print(response.status_code)
# print(response.content)
#
# response = conn_a.delete_extracts_from_workbook(workbook_id='0c3b81a2-9679-4949-bd6d-28bd509fe1c0')
# print(response.status_code)
# print(response.content)

# # workbooks_df = get_workbooks_dataframe(conn_c)
# # print(workbooks_df)
#
# print(conn_c.query_data_source_connections(snowflake_live_id).json())
# print(conn_c.query_data_source_connections(published_ds1_id).json())
# print(conn_c.query_data_source_connections("907511aa-3ae3-4c56-93fc-e8b67f9d3de0", parameter_dict={'fields': 'fields=_all_'}).json())

# for index, workbook in workbooks_df.iterrows():
#     workbook['connections'] = conn_c.query_workbook_connections(workbook['id']).json()['connections']['connection']

# workbooks_df['connections'] = workbooks_df['id'].apply(lambda x: conn_c.query_workbook_connections(x).json()['connections']['connection'])
#
# print(workbooks_df['connections'])
#
# # TODO: build upon the code above to get connections for each workbook AND store unique connections into a df
# # TODO: isolate the downloading of datasources away to datasources.py for cloning
# # TODO: download all datasources, get the connection details, and publish to target server
#
# for index, workbook in workbooks_df.iterrows():
#     for i, connection in enumerate(workbook['connections']):
#         print('workbook connection {}: {}'.format(i, connection['datasource']['name']))

conn_a.sign_out()
conn_b.sign_out()
conn_c.sign_out()
