import pandas as pd
import zipfile
import os
import urllib
import shutil

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import extract_pages, get_server_netloc
from tableau_api_lib.utils.cloning import clone_projects, clone_sites, clone_users, clone_groups, clone_datasources
from tableau_api_lib.utils.querying import get_users_dataframe, get_datasources_dataframe, get_groups_dataframe, \
    get_group_users_dataframe, get_workbooks_dataframe, get_projects_dataframe, get_sites_dataframe, \
    get_user_favorites_dataframe, get_schedules_dataframe, get_flows_dataframe, get_workbook_connections_dataframe, \
    get_datasource_connections_dataframe
from tableau_api_lib.utils.cloning.workbooks import clone_workbooks
from tableau_api_lib.utils.cloning.schedules import override_schedule_state
from tableau_api_lib.utils.filemod import create_temp_dirs, delete_temp_files


pd.set_option('display.width', 600)
pd.set_option('display.max_columns', 10)


tableau_server_config = {
        'tableau_prod': {
                'server': 'https://tableaupoc.interworks.com',
                'api_version': '3.7',
                'username': 'estam',
                'password': 'Act1Andariel!',
                'site_name': 'estam',
                'site_url': 'estam',
                'cache_buster': 'Donut',
                'temp_dir': '/dags/tableau/temp/'
        },
        'env_a': {
                'server': 'https://tableaupoc.interworks.com',
                'api_version': '3.7',
                'username': 'estam',
                'password': 'Act1Andariel!',
                'site_name': 'estam',
                'site_url': 'estam'
        },
        'env_b': {
                'server': 'https://tableaupoc.interworks.com',
                'api_version': '3.7',
                'username': 'estam',
                'password': 'Act1Andariel!',
                'site_name': 'estam2',
                'site_url': 'estam2'
        },
        'tableau_online': {
                'server': 'https://10ax.online.tableau.com',
                'api_version': '3.10',
                'personal_access_token_name': 'dev-oct30',
                'personal_access_token_secret': 'RARMkcBbSWqPwzyhjZnmLA==:pqflHd72xoMsbQfel1DXkNGxzEUAxlIL',
                'site_name': 'estamdevdev348344',
                'site_url': 'estamdevdev348344'
        }
}


conn = TableauServerConnection(tableau_server_config, 'tableau_online')


server_info = conn.server_info().json()
print("server api version: ", server_info['serverInfo']['restApiVersion'])

conn.sign_in()
print(conn.query_site().json())

#
# conn_a = TableauServerConnection(tableau_server_config, env='env_a')
# conn_b = TableauServerConnection(tableau_server_config, env='env_b')
# conn_c = TableauServerConnection(tableau_server_config, env='tableau_online')


# response_a = conn_a.sign_in()
# response_b = conn_b.sign_in()
# response_c = conn_c.sign_in()

# clone_projects(conn_a, conn_c, overwrite_policy='overwrite')

# print(help(override_schedule_state))

# clone_datasources(conn_a, conn_c, credentials_file_path='test_connection_credentials_mapping_file.csv')
#
# clone_datasources(conn_a, conn_c, credentials_file_path='test_connection_credentials_mapping_file.csv')
#
# clone_workbooks(conn_a, conn_c, credentials_file_path='test_connection_credentials_mapping_file.csv', overwrite_policy='overwrite')
#
# conn_a.publish
#
# conn_a.sign_out()
# conn_b.sign_out()
# conn_c.sign_out()
