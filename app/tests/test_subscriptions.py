import time

from tableau_api_lib import TableauServerConnection
from .config import tableau_server_config


TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_WORKBOOK_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_workbook_with_extract.twbx'
TEST_WORKBOOK_PREFIX = 'test_wb_'
TEST_SCHEDULE_NAME = 'temp_test_schedule'
TEST_SUBSCRIPTION_SUBJECT = 'temp_test_subscription_subject'
TEST_UPDATED_SUBSCRIPTION_SUBJECT = 'temp_test_updated_subscription_subject'
TEST_PROJECT_NAME = 'Test'


def sign_in():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    return conn


def get_test_user_id(connection):
    users = connection.get_users_on_site().json()['users']['user']
    for user in users:
        if user['name'] == TEST_USERNAME:
            return user['id']
    return users.pop()['id']


def get_test_project_id(connection):
    projects = connection.query_projects().json()['projects']['project']
    for project in projects:
        if project['name'] == TEST_PROJECT_NAME:
            return project['id']
    return projects.pop()['id']


def get_test_workbook_id(connection):
    print("test_workbook_id: ", connection.query_workbooks_for_site().content)
    workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
    for workbook in workbooks:
        if workbook['name'] == TEST_WORKBOOK_PREFIX + 'a':
            return workbook['id']
    return workbooks.pop()['id']


def get_test_schedule_id(connection):
    schedules = connection.query_schedules().json()['schedules']['schedule']
    for schedule in schedules:
        if schedule['name'] == TEST_SCHEDULE_NAME:
            return schedule['id']
    raise Exception('The test schedule {} was not found on Tableau Server.'.format(TEST_SCHEDULE_NAME))


def get_test_subscription_id(connection):
    subscriptions = connection.query_subscriptions().json()['subscriptions']['subscription']
    for subscription in subscriptions:
        if subscription['subject'] == TEST_SUBSCRIPTION_SUBJECT:
            return subscription['id']
    raise Exception('The test subscription {} was not found on Tableau Server.'.format(TEST_SUBSCRIPTION_SUBJECT))


def get_test_updated_subscription_id(connection):
    subscriptions = connection.query_subscriptions().json()['subscriptions']['subscription']
    for subscription in subscriptions:
        if subscription['subject'] == TEST_UPDATED_SUBSCRIPTION_SUBJECT:
            return subscription['id']
    raise Exception('The test subscription {} was not found on Tableau Server.'.
                    format(TEST_UPDATED_SUBSCRIPTION_SUBJECT))


def publish_test_workbook():
    test_project_id = get_test_project_id(conn)
    response = conn.publish_workbook(workbook_file_path=TEST_WORKBOOK_FILE_PATH,
                                     workbook_name=TEST_WORKBOOK_PREFIX+'a',
                                     project_id=test_project_id,
                                     show_tabs_flag=False,
                                     parameter_dict={
                                         'overwrite': 'overwrite=true',
                                         'asJob': 'asJob=true'
                                     })
    print("Published workbook: {}".format(response.content))
    assert response.status_code in [201, 202]


conn = TableauServerConnection(tableau_server_config)
conn.sign_in()
publish_test_workbook()


def test_create_schedule():
    response = conn.create_schedule(schedule_name=TEST_SCHEDULE_NAME,
                                    schedule_type='subscription',
                                    start_time='09:00:00')
    print(response.content)
    assert response.status_code in [200, 201]


def test_create_subscription():
    time.sleep(3)
    print(conn.query_workbooks_for_site().json())
    response = conn.create_subscription(subscription_subject=TEST_SUBSCRIPTION_SUBJECT,
                                        content_type='workbook',
                                        content_id=get_test_workbook_id(conn),
                                        schedule_id=get_test_schedule_id(conn),
                                        user_id=get_test_user_id(conn))
    print(response.content)
    assert response.status_code in [200, 201]


def test_query_subscriptions():
    response = conn.query_subscriptions()
    assert response.status_code == 200


def test_query_subscription():
    response = conn.query_subscription(subscription_id=get_test_subscription_id(conn))
    assert response.status_code == 200


def test_update_subscription():
    response = conn.update_subscription(subscription_id=get_test_subscription_id(conn),
                                        new_subscription_subject='temp_test_updated_subscription_subject',
                                        new_schedule_id=get_test_schedule_id(conn))
    assert response.status_code == 200


def test_delete_subscription():
    response = conn.delete_subscription(subscription_id=get_test_updated_subscription_id(conn))
    assert response.status_code == 204


def test_delete_schedule():
    test_schedule_id = get_test_schedule_id(conn)
    response = conn.delete_schedule(test_schedule_id)
    assert response.status_code == 204


def test_delete_workbook():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.delete_workbook(test_workbook_id)
    print(response.content)
    assert response.status_code == 204
