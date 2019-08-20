from tableau.client import TableauServerConnection
from tableau.client.config import tableau_server_config


TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_PROJECT_NAME_A = 'Test Temp A'
TEST_PROJECT_NAME_B = 'Test Temp B'

test_project_a_id = ''
test_project_b_id = ''


def sign_in():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    return conn


def test_create_project():
    global test_project_a_id
    global test_project_b_id
    conn = sign_in()
    response = conn.create_project(project_name=TEST_PROJECT_NAME_A,
                                   project_description='API test A',
                                   content_permissions='LockedToProject')
    if response.status_code == 409:
        raise Exception("Oops, you already have a project named '{}'. Reconfigure your test accordingly.".
                        format(TEST_PROJECT_NAME_A))
    test_project_a_id = response.json()['project']['id']
    assert response.status_code == 201
    response = conn.create_project(project_name=TEST_PROJECT_NAME_B,
                                   parent_project_id=test_project_a_id,
                                   project_description='API test B: nested within test A')
    test_project_b_id = response.json()['project']['id']
    assert response.status_code == 201
    conn.sign_out()


def test_query_projects():
    conn = sign_in()
    response = conn.query_projects()
    assert response.status_code == 200
    conn.sign_out()


def test_update_project_name():
    conn = sign_in()
    response = conn.update_project(project_id=test_project_b_id, project_name='Test Temp C')
    assert response.status_code == 200
    conn.sign_out()


def test_update_project_description():
    conn = sign_in()
    response = conn.update_project(project_id=test_project_b_id, project_description='Updated project description')
    assert response.status_code == 200
    conn.sign_out()


def test_update_project_parent_id():
    conn = sign_in()
    response = conn.update_project(project_id=test_project_b_id, parent_project_id='')
    assert response.status_code == 200
    response = conn.update_project(project_id=test_project_b_id, parent_project_id=test_project_a_id)
    assert response.status_code == 200
    conn.sign_out()


def test_update_project_content_permissions():
    conn = sign_in()
    response = conn.update_project(project_id=test_project_a_id, content_permissions='ManagedByOwner')
    assert response.status_code == 200
    conn.sign_out()


def test_delete_project():
    conn = sign_in()
    test_projects_parent_id = test_project_a_id
    response = conn.delete_project(test_projects_parent_id)
    assert response.status_code == 204
    conn.sign_out()
