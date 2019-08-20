from tableau.client.tableau_server_connection import TableauServerConnection
from tableau.client.config.config import tableau_server_config


TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_PROJECT_NAME = 'Test'
TEST_ALT_PROJECT_NAME = 'Test Alt'
TEST_FLOW_FILE_PATH = r'C:\Users\estam\Documents\InterWorks\Demo\Tableau Prep\sample_snowflake_flow.tfl'


def sign_in():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    return conn


def get_flow_project_id(conn, flow_id):
    return conn.query_flow(flow_id).json()['flow']['project']['id']


def get_alt_project_id(conn):
    projects = conn.query_projects().json()['projects']['project']
    for project in projects:
        if project['name'] == TEST_ALT_PROJECT_NAME:
            return project['id']
    return projects.pop()['id']


def get_active_user_id(conn):
    users = conn.get_users_on_site().json()['users']['user']
    for user in users:
        if user['name'] == TEST_USERNAME:
            return user['id']
    return users.pop()['id']


def get_alt_user_id(conn):
    users = conn.get_users_on_site().json()['users']['user']
    for user in users:
        if user['name'] != TEST_USERNAME:
            return user['id']
    return users.pop()['id']


def get_flow_id(conn):
    # This will take the first flow_id found, only considering flows whose names contain the string 'test'
    try:
        flows = conn.query_flows_for_site().json()['flows']['flow']
        test_flows = [flow for flow in flows if 'test' in flow['name'].lower()]
        return test_flows.pop()['id']
    except KeyError:
        print('No flows exist for the site {}.'.
              format(conn.query_site().json()['site']['name']))


def get_flow_schedule_id(conn):
    schedules = conn.query_schedules().json()['schedules']['schedule']
    flow_schedules = [schedule for schedule in schedules if schedule['type'] == 'Flow']
    sample_flow_schedule_id = flow_schedules.pop()['id']
    return sample_flow_schedule_id


def get_flow_connection_id(conn, flow_id):
    flow_connections = conn.query_flow_connections(flow_id).json()['connections']['connection']
    return flow_connections.pop()['id']


def test_publish_flow():
    conn = sign_in()
    sample_project_id = get_alt_project_id(conn)
    response = conn.publish_flow(TEST_FLOW_FILE_PATH, 'test_publish_flow', project_id=sample_project_id)
    assert response.status_code == 201
    conn.sign_out()


def test_query_flows_for_site():
    conn = sign_in()
    response = conn.query_flows_for_site()
    assert response.status_code == 200
    conn.sign_out()


def test_query_flow():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    response = conn.query_flow(sample_flow_id)
    assert response.status_code == 200
    conn.sign_out()


def test_add_flow_task_to_schedule():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    sample_flow_schedule_id = get_flow_schedule_id(conn)
    response = conn.add_flow_task_to_schedule(sample_flow_id, sample_flow_schedule_id)
    print('add_flow_to_schedule: ', response.json())
    assert response.status_code == 200
    conn.sign_out()


def test_get_flow_run_tasks():
    conn = sign_in()
    response = conn.get_flow_run_tasks()
    assert response.status_code == 200
    conn.sign_out()


def test_get_flow_run_task():
    conn = sign_in()
    sample_flow_run_task_id = conn.get_flow_run_tasks().json()['tasks']['task'].pop()['flowRun']['id']
    response = conn.get_flow_run_task(sample_flow_run_task_id)
    print(response.json())
    assert response.status_code == 200
    conn.sign_out()


def test_run_flow_task():
    conn = sign_in()
    sample_flow_run_task_id = conn.get_flow_run_tasks().json()['tasks']['task'].pop()['flowRun']['id']
    response = conn.run_flow_task(sample_flow_run_task_id)
    print(response.json())
    assert response.status_code == 200
    conn.sign_out()


def test_query_flows_for_user():
    conn = sign_in()
    user_id = get_active_user_id(conn)
    response = conn.query_flows_for_user(user_id)
    assert response.status_code == 200
    conn.sign_out()


def test_query_flow_connections():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    response = conn.query_flow_connections(sample_flow_id)
    assert response.status_code == 200
    conn.sign_out()


def test_query_flow_permissions():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    response = conn.query_flow_permissions(sample_flow_id)
    assert response.status_code == 200
    conn.sign_out()


def test_add_and_delete_flow_permissions():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    user_id = get_alt_user_id(conn)
    conn.delete_flow_permission(flow_id=sample_flow_id,
                                delete_permissions_object='user',
                                delete_permissions_object_id=user_id,
                                capability_name='Read',
                                capability_mode='Allow')
    conn.delete_flow_permission(flow_id=sample_flow_id,
                                delete_permissions_object='user',
                                delete_permissions_object_id=user_id,
                                capability_name='Write',
                                capability_mode='Deny')
    response = conn.add_flow_permissions(flow_id=sample_flow_id,
                                         user_capability_dict={
                                             'Read': 'Allow',
                                             'Write': 'Deny'
                                         },
                                         user_id=user_id)
    assert response.status_code == 200
    conn.sign_out()


def test_download_flow():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    response = conn.download_flow(sample_flow_id)
    assert response.status_code == 200
    conn.sign_out()


def test_update_flow_project():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    flow_project_id = get_flow_project_id(conn, sample_flow_id)
    alt_project_id = get_alt_project_id(conn)
    response = conn.update_flow(sample_flow_id, new_project_id=alt_project_id)
    assert response.status_code == 200
    conn.update_flow(sample_flow_id, new_project_id=flow_project_id)
    conn.sign_out()


def test_update_flow_owner():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    flow_owner = get_active_user_id(conn)
    alt_owner = get_alt_user_id(conn)
    response = conn.update_flow(sample_flow_id, new_owner_id=alt_owner)
    assert response.status_code == 200
    conn.update_flow(sample_flow_id, new_owner_id=flow_owner)
    conn.sign_out()


def test_update_flow_connection():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    sample_flow_connection_id = get_flow_connection_id(conn, flow_id=sample_flow_id)
    response = conn.update_flow_connection(sample_flow_id, sample_flow_connection_id, embed_password_flag=False)
    print(response.content)
    assert response.status_code == 200
    conn.sign_out()


def test_delete_flow():
    conn = sign_in()
    sample_flow_id = get_flow_id(conn)
    response = conn.delete_flow(sample_flow_id)
    assert response.status_code == 204
    conn.sign_out()
