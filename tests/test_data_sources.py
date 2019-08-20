from tableau.client import TableauServerConnection
from tableau.client.config import tableau_server_config

TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
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


def get_test_datasource_id(connection):
    datasources = connection.query_data_sources().json()['datasources']['datasource']
    for datasource in datasources:
        if datasource['name'] == TEST_DATASOURCE_PREFIX + 'a':
            return datasource['id']
    return datasources.pop()['id']


def get_test_datasource_b_id(connection):
    datasources = connection.query_data_sources().json()['datasources']['datasource']
    for datasource in datasources:
        if datasource['name'] == TEST_DATASOURCE_PREFIX + 'b':
            return datasource['id']
    return datasources.pop()['id']


def get_datasource_connection_id(connection, datasource_id):
    datasource_connections = connection.query_data_source_connections(datasource_id).json()['connections']['connection']
    return datasource_connections.pop()['id']


conn = sign_in()


def test_publish_data_source():
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
    response = conn.publish_data_source(datasource_file_path=TEST_DATASOURCE_FILE_PATH,
                                        datasource_name=TEST_DATASOURCE_PREFIX + 'b',
                                        project_id=test_project_id,
                                        parameter_dict={
                                            'overwrite': 'overwrite=true',
                                            'asJob': 'asJob=true'
                                        })
    assert response.status_code in [201, 202]


def test_add_tags_to_data_source():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.add_tags_to_data_source(test_datasource_id, ['test_datasource', 'temp_datasource'])
    assert response.status_code == 200


def test_delete_tag_from_data_source():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.delete_tag_from_data_source(test_datasource_id, 'test_datasource')
    assert response.status_code == 204
    response = conn.delete_tag_from_data_source(test_datasource_id, 'temp_datasource')
    assert response.status_code == 204


def test_query_data_source():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.query_data_source(test_datasource_id)
    assert response.status_code == 200


def test_query_data_sources():
    response = conn.query_data_sources()
    assert response.status_code == 200


def test_query_data_source_connections():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.query_data_source_connections(test_datasource_id)
    assert response.status_code == 200


def test_get_data_source_revisions():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.get_data_source_revisions(test_datasource_id)
    assert response.status_code == 200


def test_download_data_source():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.download_data_source(test_datasource_id)
    assert response.status_code == 200


def test_download_data_source_revision():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.download_data_source_revision(test_datasource_id, revision_number=1)
    assert response.status_code == 200


def test_update_data_source_project_id():
    original_project_id = get_test_project_id(conn)
    alt_project_id = get_alt_project_id(conn)
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.update_data_source(test_datasource_id, new_project_id=alt_project_id)
    assert response.status_code == 200
    response = conn.update_data_source(test_datasource_id, new_project_id=original_project_id)
    assert response.status_code == 200


def test_update_data_source_owner_id():
    original_user_id = get_active_user_id(conn)
    alt_user_id = get_alt_user_id(conn)
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.update_data_source(test_datasource_id, new_owner_id=alt_user_id)
    assert response.status_code == 200
    response = conn.update_data_source(test_datasource_id, new_owner_id=original_user_id)
    assert response.status_code == 200


def test_update_data_source_now():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.update_data_source_now(test_datasource_id)
    print(response.json())
    assert response.status_code == 202


def test_update_data_source_connection():
    test_datasource_id = get_test_datasource_id(conn)
    test_datasource_connection_id = get_datasource_connection_id(conn, test_datasource_id)
    response = conn.update_data_source_connection(test_datasource_id,
                                                  test_datasource_connection_id,
                                                  embed_password_flag=False)
    print(response.json())
    assert response.status_code == 200


def test_remove_data_source_revision():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.remove_data_source_revision(test_datasource_id, revision_number=1)
    assert response.status_code == 204


def test_delete_data_source():
    test_datasource_a_id = get_test_datasource_id(conn)
    test_datasource_b_id = get_test_datasource_b_id(conn)
    response = conn.delete_data_source(test_datasource_a_id)
    assert response.status_code == 204
    response = conn.delete_data_source(test_datasource_b_id)
    assert response.status_code == 204
