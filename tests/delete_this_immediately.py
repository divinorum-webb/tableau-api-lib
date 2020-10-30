from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import querying


TEST_PROJECT_ID = '19116c5b-9bb1-4777-ae65-195ba29841da'


tableau_server_config = {
        'tableau_online': {
            'server': 'https://10ax.online.tableau.com',
            'api_version': '3.9',
            'username': 'elliott.stam@interworks.com',
            'password': 'Jarganuke1122!',
            'site_name': 'estamiwdev422309',
            'site_url': 'estamiwdev422309'
        },
        'tableau_online2': {
            'server': 'https://10ax.online.tableau.com',
            'api_version': '3.9',
            'personal_access_token_name': 'dev-sep03',
            'personal_access_token_secret': 'HeiRJwikQ4e1EvN8CdDoYA==:gAnVDsrhDbuhMjIr2mjHPQHWrsu1cAMg',
            'site_name': 'estamdevdev348344',
            'site_url': 'estamdevdev348344'
        }
}

conn = TableauServerConnection(tableau_server_config, 'tableau_online2')

conn.sign_in()

import os
print(os.listdir('./twbx_files/'))

conn.publish_data_source(r'./twbx_files/Superstore Datasource.tdsx',
                         'test datasource',
                         TEST_PROJECT_ID,
                         datasource_description='this is my description!')

conn.sign_out()


