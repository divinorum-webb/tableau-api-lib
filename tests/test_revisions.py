import time

from tableau.client.tableau_server_connection import TableauServerConnection
from tableau.client.config.config import tableau_server_config

TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_WORKBOOK_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_workbook_with_extract.twbx'
TEST_WORKBOOK_PREFIX = 'test_wb_'
TEST_DATASOURCE_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_datasource_with_extract.tdsx'
TEST_DATASOURCE_PREFIX = 'test_ds_'
TEST_PROJECT_NAME = 'Test'
TEST_ALT_PROJECT_NAME = 'Test Alt'


def sign_in():
    connection = TableauServerConnection(tableau_server_config)
    connection.sign_in()
    return connection


def get_active_user_id(connection):
    users = connection.get_users_on_site().json()['users']['user']
    for user in users:
        if user['name'] == TEST_USERNAME:
            return user['id']
    return users.pop()['id']


def get_alt_user_id(connection):
    users = connection.get_users_on_site().json()['users']['user']
    for user in users:
        if 'test' in user['name'].lower():
            return user['id']
    return users.pop()['id']


def get_test_project_id(connection):
    projects = connection.query_projects().json()['projects']['project']
    for project in projects:
        if project['name'] == TEST_PROJECT_NAME:
            return project['id']
    return projects.pop()['id']


def get_alt_project_id(connection):
    projects = connection.query_projects().json()['projects']['project']
    for project in projects:
        if project['name'] == TEST_ALT_PROJECT_NAME:
            return project['id']
    return projects.pop()['id']


def get_test_workbook_id(connection):
    workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
    for workbook in workbooks:
        if workbook['name'] == TEST_WORKBOOK_PREFIX + 'a':
            return workbook['id']
    return workbooks.pop()['id']


def get_test_datasource_id(connection):
    datasources = connection.query_data_sources().json()['datasources']['datasource']
    for datasource in datasources:
        if datasource['name'] == TEST_DATASOURCE_PREFIX + 'a':
            return datasource['id']
    return datasources.pop()['id']


def publish_workbook():
    test_project_id = get_test_project_id(conn)
    response = conn.publish_workbook(workbook_file_path=TEST_WORKBOOK_FILE_PATH,
                                     workbook_name=TEST_WORKBOOK_PREFIX+'a',
                                     project_id=test_project_id,
                                     show_tabs_flag=False,
                                     parameter_dict={
                                         'overwrite': 'overwrite=true',
                                         'asJob': 'asJob=true'
                                     })
    assert response.status_code in [201, 202]
    # publish the same workbook again to have 2 versions of the workbook for later
    response = conn.publish_workbook(workbook_file_path=TEST_WORKBOOK_FILE_PATH,
                                     workbook_name=TEST_WORKBOOK_PREFIX+'a',
                                     project_id=test_project_id,
                                     show_tabs_flag=False,
                                     parameter_dict={
                                         'overwrite': 'overwrite=true',
                                         'asJob': 'asJob=true'
                                     })
    assert response.status_code in [201, 202]


def publish_data_source():
    test_project_id = get_test_project_id(conn)
    response = conn.publish_data_source(datasource_file_path=TEST_DATASOURCE_FILE_PATH,
                                        datasource_name=TEST_DATASOURCE_PREFIX + 'a',
                                        project_id=test_project_id,
                                        parameter_dict={
                                            'overwrite': 'overwrite=true',
                                            'asJob': 'asJob=true'
                                        })
    assert response.status_code in [201, 202]
    # publish the same workbook again to have 2 versions of the workbook for later
    response = conn.publish_data_source(datasource_file_path=TEST_DATASOURCE_FILE_PATH,
                                        datasource_name=TEST_DATASOURCE_PREFIX + 'a',
                                        project_id=test_project_id,
                                        parameter_dict={
                                            'overwrite': 'overwrite=true',
                                            'asJob': 'asJob=true'
                                        })
    assert response.status_code in [201, 202]


conn = sign_in()
publish_workbook()
publish_data_source()


def test_get_data_source_revisions():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.get_data_source_revisions(test_datasource_id)
    assert response.status_code == 200


def test_get_workbook_revisions():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.get_workbook_revisions(test_workbook_id)
    assert response.status_code == 200


def test_download_data_source_revision():
    time.sleep(1)
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.download_data_source_revision(test_datasource_id, revision_number=1)
    assert response.status_code == 200


def test_download_workbook_revision():
    time.sleep(1)
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.download_workbook_revision(test_workbook_id, revision_number=1)
    assert response.status_code == 200


def test_remove_data_source_revision():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.remove_data_source_revision(test_datasource_id, revision_number=1)
    assert response.status_code == 204


def test_remove_workbook_revision():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.remove_workbook_revision(test_workbook_id, revision_number=1)
    assert response.status_code == 204


def test_delete_workbook():
    test_workbook_a_id = get_test_workbook_id(conn)
    response = conn.delete_workbook(test_workbook_a_id)
    assert response.status_code == 204


def test_delete_data_source():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.delete_data_source(test_datasource_id)
    assert response.status_code == 204
