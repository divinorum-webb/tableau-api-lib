from tableau.client.tableau_server_connection import TableauServerConnection
from tableau.client.config.config import tableau_server_config

TEST_SITE_NAME = 'estam_api_test'
TEST_SITE_CONTENT_URL = 'estam_api_test'
UPDATED_CONTENT_URL = 'estam_api_test_renamed'
DEFAULT_ADMIN_MODE = 'ContentAndUsers'
DEFAULT_STATE = 'Active'


def sign_in():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    return conn


def sign_out(conn):
    conn.sign_out()


def create_site(conn, site_name, site_content_url):
    response = conn.create_site(site_name, site_content_url)
    return conn, response


def query_site(conn):
    response = conn.query_site()
    return conn, response


def query_sites(conn):
    response = conn.query_sites()
    return conn, response


def query_views_for_site(conn):
    response = conn.query_views_for_site()
    return conn, response


def update_site_name(conn, new_content_url):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response = conn.update_site(site_id=site_id, content_url=new_content_url)
    return conn, response


def update_site_admin_mode(conn, new_admin_mode):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response_a = conn.update_site(site_id=site_id, admin_mode=new_admin_mode)
    response_b = conn.update_site(site_id=site_id, admin_mode=DEFAULT_ADMIN_MODE)
    return conn, response_a, response_b


def update_site_user_quota(conn, new_user_quota):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response = conn.update_site(site_id=site_id, user_quota=new_user_quota)
    return conn, response


def update_site_state(conn, new_state):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response_a = conn.update_site(site_id=site_id, state=new_state)
    response_b = conn.update_site(site_id=site_id, state=DEFAULT_STATE)
    return conn, response_a, response_b


def update_site_storage_quota(conn, new_storage_quota):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response = conn.update_site(site_id=site_id, storage_quota=new_storage_quota)
    return conn, response


def update_site_disable_subscriptions_flag(conn):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response_a = conn.update_site(site_id=site_id, disable_subscriptions_flag=True)
    response_b = conn.update_site(site_id=site_id, disable_subscriptions_flag=False)
    return conn, response_a, response_b


def update_site_revision_history_enabled_flag(conn):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response_a = conn.update_site(site_id=site_id, revision_history_enabled_flag=True)
    response_b = conn.update_site(site_id=site_id, revision_history_enabled_flag=False)
    return conn, response_a, response_b


def update_site_revision_limit(conn, new_revision_limit):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response = conn.update_site(site_id=site_id, revision_limit=new_revision_limit)
    return conn, response


def update_site_misc_flag(conn):
    response_list = []
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response_list.append(conn.update_site(site_id=site_id, subscribe_others_enabled_flag=True))
    response_list.append(conn.update_site(site_id=site_id, subscribe_others_enabled_flag=False))
    # response_list.append(conn.update_site(site_id=site_id, guest_access_enabled_flag=True))
    response_list.append(conn.update_site(site_id=site_id, guest_access_enabled_flag=False))
    response_list.append(conn.update_site(site_id=site_id, cache_warmup_enabled_flag=True))
    response_list.append(conn.update_site(site_id=site_id, cache_warmup_enabled_flag=False))
    response_list.append(conn.update_site(site_id=site_id, commenting_enabled_flag=True))
    response_list.append(conn.update_site(site_id=site_id, commenting_enabled_flag=False))
    response_list.append(conn.update_site(site_id=site_id, flows_enabled_flag=True))
    response_list.append(conn.update_site(site_id=site_id, flows_enabled_flag=False))
    return conn, response_list


def delete_site(conn):
    site_json = conn.query_site().json()
    site_id = site_json['site']['id']
    response = conn.delete_site(site_id=site_id)
    return conn, response


def test_create_site():
    conn = sign_in()
    conn, create_site_response = create_site(conn, TEST_SITE_NAME, TEST_SITE_NAME)
    assert create_site_response.status_code == 201
    conn.sign_out()


def test_query_site():
    conn = sign_in()
    conn, query_site_response = query_site(conn)
    assert query_site_response.status_code == 200
    conn.sign_out()


def test_query_sites():
    conn = sign_in()
    conn, query_sites_response = query_sites(conn)
    assert query_sites_response.status_code == 200
    conn.sign_out()


def test_query_views_for_site():
    conn = sign_in()
    conn, query_views_for_site_response = query_views_for_site(conn)
    assert query_views_for_site_response.status_code == 200
    conn.sign_out()


def test_update_site_name():
    conn = sign_in()
    conn.switch_site(TEST_SITE_CONTENT_URL)
    conn, update_site_response = update_site_name(conn, UPDATED_CONTENT_URL)
    assert update_site_response.status_code == 200
    conn.sign_out()


def test_update_site_admin_mode():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, update_site_response_a, update_site_response_b = update_site_admin_mode(conn, 'ContentOnly')
    assert update_site_response_a.status_code == 200
    assert update_site_response_b.status_code == 200
    conn.sign_out()


def test_update_site_user_quota():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, update_site_response = update_site_user_quota(conn, 15)
    assert update_site_response.status_code == 200
    conn.sign_out()


def test_update_site_state():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, update_site_response_a, update_site_response_b = update_site_state(conn, 'Suspended')
    assert update_site_response_a.status_code == 200
    assert update_site_response_b.status_code == 200
    conn.sign_out()


def test_update_site_storage_quota():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, update_site_response = update_site_storage_quota(conn, 1024)
    assert update_site_response.status_code == 200
    conn.sign_out()


def test_update_site_disable_subscriptions_flag():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, update_site_response_a, update_site_response_b = update_site_disable_subscriptions_flag(conn)
    assert update_site_response_a.status_code == 200
    assert update_site_response_b.status_code == 200
    conn.sign_out()


def test_update_site_revision_history_enabled_flag():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, update_site_response_a, update_site_response_b = update_site_revision_history_enabled_flag(conn)
    assert update_site_response_a.status_code == 200
    assert update_site_response_b.status_code == 200
    conn.sign_out()


def test_update_site_revision_limit():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, update_site_response = update_site_revision_limit(conn, 15)
    assert update_site_response.status_code == 200
    conn.sign_out()


def test_update_site_misc_flag():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    conn, response_list = update_site_misc_flag(conn)
    for i, response in enumerate(response_list):
        print("iteration {}".format(i))
        assert response.status_code == 200
    conn.sign_out()


def test_delete_site():
    conn = sign_in()
    conn.switch_site(UPDATED_CONTENT_URL)
    if conn.query_site().json()['site']['name'] != 'estam':
        conn, delete_site_response = delete_site(conn)
        assert delete_site_response.status_code == 204
    conn.sign_out()
