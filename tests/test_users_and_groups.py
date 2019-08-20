import time

from tableau.client import TableauServerConnection
from tableau.client.config import tableau_server_config

TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_GROUP_NAME = 'Test Group'
TEST_USER_DISPLAY_NAME = 'ESTAM TEST'
TEST_USER = 'estam_test'
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


def get_test_group_id(connection):
    groups = connection.query_groups().json()['groups']['group']
    for group in groups:
        if group['name'] == TEST_GROUP_NAME:
            return group['id']
    return groups.pop()['id']


conn = sign_in()


def test_get_users_on_site():
    response = conn.get_users_on_site()
    assert response.status_code == 200


def test_query_user_on_site():
    test_user_id = get_active_user_id(conn)
    response = conn.query_user_on_site(test_user_id)
    assert response.status_code == 200


def test_query_groups():
    response = conn.query_groups()
    assert response.status_code == 200


def test_get_users_in_group():
    test_group_id = get_test_group_id(conn)
    response = conn.get_users_in_group(test_group_id)
    assert response.status_code == 200


def test_create_group():
    response = conn.create_group(TEST_GROUP_NAME)
    print('test_create_group: ', response.content)
    assert response.status_code in [201, 202]


def test_add_user_to_site():
    response = conn.add_user_to_site(TEST_USER, site_role='Creator')
    assert response.status_code == 201


def test_add_user_to_group():
    time.sleep(1)
    test_group_id = get_test_group_id(conn)
    test_user_id = get_alt_user_id(conn)
    response = conn.add_user_to_group(test_group_id, test_user_id)
    assert response.status_code == 200


def test_update_group():
    test_group_id = get_test_group_id(conn)
    response = conn.update_group(test_group_id, new_group_name='Test Group Renamed')
    print('test_update_group: ', response.content)
    assert response.status_code in [200, 201, 202]
    response = conn.update_group(test_group_id, new_group_name=TEST_GROUP_NAME)
    print('test_update_group: ', response.content)
    assert response.status_code in [200, 201, 202]


def test_update_user_full_name():
    test_user_id = get_alt_user_id(conn)
    response = conn.update_user(test_user_id, new_full_name='Elliott API Test User')
    assert response.status_code == 200


def test_update_user_site_role():
    test_user_id = get_alt_user_id(conn)
    response = conn.update_user(test_user_id, new_site_role='Explorer')
    assert response.status_code == 200


def test_remove_user_from_group():
    test_user_id = get_alt_user_id(conn)
    test_group_id = get_test_group_id(conn)
    response = conn.remove_user_from_group(test_group_id, test_user_id)
    print('test_remove_user_from_group: ', response.content)
    assert response.status_code == 204


def test_remove_user_from_site():
    test_user_id = get_alt_user_id(conn)
    response = conn.remove_user_from_site(test_user_id)
    assert response.status_code == 204


def test_delete_group():
    test_group_id = get_test_group_id(conn)
    response = conn.delete_group(test_group_id)
    assert response.status_code == 204
