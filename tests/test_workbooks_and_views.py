from tableau.client import TableauServerConnection
from tableau.client.config import tableau_server_config

TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_WORKBOOK_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_workbook_with_extract.twbx'
TEST_WORKBOOK_PREFIX = 'test_wb_'
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


def get_test_workbook_b_id(connection):
    workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
    for workbook in workbooks:
        if workbook['name'] == TEST_WORKBOOK_PREFIX + 'b':
            return workbook['id']
    return workbooks.pop()['id']


def get_workbook_connection_id(connection, workbook_id):
    workbook_connections = connection.query_workbook_connections(workbook_id).json()['connections']['connection']
    return workbook_connections.pop()['id']


def get_test_view_id(connection):
    test_workbook_id = get_test_workbook_id(connection)
    test_views = connection.query_views_for_workbook(test_workbook_id).json()['views']['view']
    return test_views.pop()['id']


conn = sign_in()


def test_publish_workbook():
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
    response = conn.publish_workbook(workbook_file_path=TEST_WORKBOOK_FILE_PATH,
                                     workbook_name=TEST_WORKBOOK_PREFIX + 'b',
                                     project_id=test_project_id,
                                     show_tabs_flag=True,
                                     parameter_dict={
                                         'overwrite': 'overwrite=true',
                                         'asJob': 'asJob=true'
                                     })
    assert response.status_code in [201, 202]


def test_add_tags_to_view():
    test_view_id = get_test_view_id(conn)
    response = conn.add_tags_to_view(test_view_id, ['test_view', 'temp_view'])
    assert response.status_code == 200


def test_add_tags_to_workbook():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.add_tags_to_workbook(test_workbook_id, ['test', 'temp'])
    assert response.status_code == 200


def test_query_views_for_site():
    response = conn.query_views_for_site()
    assert response.status_code == 200


def test_query_views_for_workbook():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.query_views_for_workbook(test_workbook_id)
    assert response.status_code == 200


def test_query_view_data():
    test_view_id = get_test_view_id(conn)
    response = conn.query_view_data(test_view_id)
    assert response.status_code == 200


def test_query_view_image():
    test_view_id = get_test_view_id(conn)
    response = conn.query_view_image(test_view_id)
    assert response.status_code == 200


def test_query_view_pdf():
    test_view_id = get_test_view_id(conn)
    response = conn.query_view_image(test_view_id)
    assert response.status_code == 200


def test_query_view_preview_image():
    test_view_id = get_test_view_id(conn)
    response = conn.query_view_pdf(test_view_id)
    assert response.status_code == 200


def test_query_workbook():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.query_workbook(test_workbook_id)
    assert response.status_code == 200


def test_query_workbook_connections():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.query_workbook_connections(test_workbook_id)
    assert response.status_code == 200


def test_get_view():
    test_view_id = get_test_view_id(conn)
    response = conn.get_view(test_view_id)
    assert response.status_code == 200
    response = conn.query_view(test_view_id)
    assert response.status_code == 200


def test_get_workbook_revisions():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.get_workbook_revisions(test_workbook_id)
    print(response.content)
    assert response.status_code == 200


def test_query_workbook_preview_image():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.query_workbook_preview_image(test_workbook_id)
    assert response.status_code == 200


def test_query_workbooks_for_site():
    response = conn.query_workbooks_for_site()
    assert response.status_code == 200


def test_query_workbooks_for_user():
    test_user_id = get_active_user_id(conn)
    response = conn.query_workbooks_for_user(test_user_id)
    assert response.status_code == 200


def test_download_workbook():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.download_workbook(test_workbook_id)
    assert response.status_code == 200


def test_download_workbook_pdf():
    test_workbook_id = get_test_workbook_id(conn)
    print(test_workbook_id)
    response = conn.download_workbook_pdf(test_workbook_id,
                                          parameter_dict={
                                              'orientation': 'orientation=portrait',
                                              'type': 'type=A5'
                                          })
    print(response.content)
    assert response.status_code == 200


def test_download_workbook_revision():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.download_workbook_revision(test_workbook_id, revision_number=1)
    print(response.content)
    assert response.status_code == 200


def test_update_workbook_show_tabs_flag():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.update_workbook(test_workbook_id, show_tabs_flag=False)
    assert response.status_code == 200
    response = conn.update_workbook(test_workbook_id, show_tabs_flag=True)
    assert response.status_code == 200


def test_update_workbook_project_id():
    test_workbook_id = get_test_workbook_id(conn)
    original_project_id = get_test_project_id(conn)
    alt_project_id = get_alt_project_id(conn)
    response = conn.update_workbook(test_workbook_id, project_id=alt_project_id)
    assert response.status_code == 200
    response = conn.update_workbook(test_workbook_id, project_id=original_project_id)
    assert response.status_code == 200


def test_update_workbook_owner_id():
    test_workbook_id = get_test_workbook_id(conn)
    original_owner_id = get_active_user_id(conn)
    alt_owner_id = get_alt_user_id(conn)
    response = conn.update_workbook(test_workbook_id, owner_id=alt_owner_id)
    assert response.status_code == 200
    response = conn.update_workbook(test_workbook_id, owner_id=original_owner_id)
    assert response.status_code == 200


def test_update_workbook_now():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.update_workbook_now(test_workbook_id)
    print('update_workbook_now: ', response.content)
    assert response.status_code == 202


def test_update_workbook_connection():
    test_workbook_id = get_test_workbook_id(conn)
    test_connection_id = get_workbook_connection_id(conn, test_workbook_id)
    response = conn.update_workbook_connection(test_workbook_id, test_connection_id, embed_password_flag=False)
    assert response.status_code == 200


def test_delete_tag_from_view():
    test_view_id = get_test_view_id(conn)
    response = conn.delete_tag_from_view(test_view_id, 'temp_view')
    assert response.status_code == 204
    response = conn.delete_tag_from_view(test_view_id, 'test_view')
    assert response.status_code == 204


def test_delete_tag_from_workbook():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.delete_tag_from_workbook(test_workbook_id, 'temp')
    assert response.status_code == 204
    response = conn.delete_tag_from_workbook(test_workbook_id, 'test')
    assert response.status_code == 204


def test_delete_workbook():
    test_workbook_a_id = get_test_workbook_id(conn)
    test_workbook_b_id = get_test_workbook_b_id(conn)
    response = conn.delete_workbook(test_workbook_a_id)
    assert response.status_code == 204
    response = conn.delete_workbook(test_workbook_b_id)
    assert response.status_code == 204
