from tableau.client.tableau_server_connection import TableauServerConnection
from tableau.client.config.config import tableau_server_config

TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']


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
        if user['name'] != TEST_USERNAME:
            return user['id']
    return users.pop()['id']


def get_data_driven_alert_id(connection):
    # This will take the first alert_id found, only considering alerts whose subjects include the string 'test'
    try:
        alerts = connection.query_data_driven_alerts().json()['dataAlerts']['dataAlert']
        test_alerts = [alert for alert in alerts if 'test' in alert['subject'].lower()]
        return test_alerts.pop()['id']
    except KeyError:
        print('No data driven alerts exist for the site {}.'.
              format(connection.query_site().json()['site']['name']))


conn = sign_in()


def test_query_data_driven_alerts():
    response = conn.query_data_driven_alerts()
    assert response.status_code == 200


def test_query_data_driven_alert_details():
    sample_alert_id = get_data_driven_alert_id(conn)
    response = conn.query_data_driven_alert_details(data_alert_id=sample_alert_id)
    assert response.status_code == 200


def test_update_data_driven_alert_subject():
    sample_alert_id = get_data_driven_alert_id(conn)
    response = conn.update_data_driven_alert(sample_alert_id, data_alert_subject='Test Updated Subject')
    assert response.status_code == 200


def test_update_data_driven_alert_frequency():
    sample_alert_id = get_data_driven_alert_id(conn)
    response_a = conn.update_data_driven_alert(sample_alert_id, data_alert_frequency='once')
    response_b = conn.update_data_driven_alert(sample_alert_id, data_alert_frequency='frequently')
    response_c = conn.update_data_driven_alert(sample_alert_id, data_alert_frequency='hourly')
    response_d = conn.update_data_driven_alert(sample_alert_id, data_alert_frequency='daily')
    response_e = conn.update_data_driven_alert(sample_alert_id, data_alert_frequency='weekly')
    assert response_a.status_code == 200
    assert response_b.status_code == 200
    assert response_c.status_code == 200
    assert response_d.status_code == 200
    assert response_e.status_code == 200


def test_update_data_driven_alert_owner_id():
    sample_alert_id = get_data_driven_alert_id(conn)
    original_user_id = get_active_user_id(conn)
    if sample_alert_id == original_user_id:
        print('The site alert {} belongs to only has one user. Add another user to properly test this function'.
              format(sample_alert_id))
    alt_user_id = get_alt_user_id(conn)
    response = conn.update_data_driven_alert(sample_alert_id, data_alert_owner_id=alt_user_id)
    if response.status_code == 500:
        print('Error code 500 occurred. Users in your site need to be eligible to receive alerts to own alerts.')
    assert response.status_code == 200
    response = conn.update_data_driven_alert(sample_alert_id, data_alert_owner_id=original_user_id)
    assert response.status_code == 200


def test_update_data_driven_alert_is_public_flag():
    sample_alert_id = get_data_driven_alert_id(conn)
    response = conn.update_data_driven_alert(sample_alert_id, is_public_flag=False)
    assert response.status_code == 200
    response = conn.update_data_driven_alert(sample_alert_id, is_public_flag=True)
    assert response.status_code == 200


def test_add_user_to_data_driven_alert():
    sample_alert_id = get_data_driven_alert_id(conn)
    alt_user_id = get_alt_user_id(conn)
    response = conn.add_user_to_data_driven_alert(alt_user_id, sample_alert_id)
    assert response.status_code == 200


def test_delete_user_from_data_driven_alert():
    sample_alert_id = get_data_driven_alert_id(conn)
    alt_user_id = get_alt_user_id(conn)
    response = conn.delete_user_from_data_driven_alert(alt_user_id, sample_alert_id)
    assert response.status_code == 204


def test_delete_data_driven_alert():
    # Alert must have 'test' in its name to be deleted. This is to prevent deleting real alerts.
    sample_alert_id = get_data_driven_alert_id(conn)
    response = conn.delete_data_driven_alert(sample_alert_id)
    assert response.status_code == 204
