from tableau_api_lib import TableauServerConnection
from .config import tableau_server_config


TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_SUBSCRIPTION_NAME = 'temp_test_subscription'


def sign_in():
    connection = TableauServerConnection(tableau_server_config)
    connection.sign_in()
    return connection


def get_test_schedule_id():
    schedules = conn.query_schedules().json()['subscriptions']['subscription']
    for schedule in schedules:
        if schedule['name'] == TEST_SUBSCRIPTION_NAME:
            return schedule['id']
    raise Exception('The test subscription {} was not found on Tableau Server.'.format(TEST_SUBSCRIPTION_NAME))


conn = sign_in()


def test_create_subscription():
    pass


def test_query_subscriptions():
    response = conn.query_subscriptions()
    assert response.status_code == 200


def test_query_subscription():
    pass


def test_update_subscription():
    pass


def test_delete_subscription():
    pass
