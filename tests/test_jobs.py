from tableau_api_lib import TableauServerConnection
from .config import tableau_server_config

TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']


def sign_in():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    return conn
