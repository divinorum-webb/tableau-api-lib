from tableau.client.tableau_server_connection import TableauServerConnection
from tableau.client.config.config import tableau_server_config


def get_basic_auth_responses():
    conn = TableauServerConnection(tableau_server_config)
    sign_in_response = conn.sign_in()
    sign_out_response = conn.sign_out()
    return sign_in_response, sign_out_response


def get_original_content_url():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    original_site = conn.query_site().json()
    original_content_url = original_site['site']['contentUrl']
    return conn, original_content_url


def get_replacement_content_url(conn, original_content_url):
    sites = conn.query_sites().json()
    site_content_urls = [site['contentUrl'] for site in sites['sites']['site']]
    other_site_ids = [site_id for site_id in site_content_urls if site_id != original_content_url]
    if any(other_site_ids):
        new_content_url = other_site_ids.pop()
    else:
        raise Exception('Could not switch sites. Only one site was available to your user ID.')
    return conn, new_content_url


def switch_site(conn, new_content_url):
    switch_site_response = conn.switch_site(new_content_url)
    new_active_site = conn.query_site().json()
    return conn, new_active_site, switch_site_response


def test_basic_auth():
    sign_in_response, sign_out_response = get_basic_auth_responses()
    assert sign_in_response.status_code == 200
    assert sign_out_response.status_code == 204


def test_switch_site():
    conn, original_content_url = get_original_content_url()
    conn, new_content_url = get_replacement_content_url(conn, original_content_url)
    conn, new_active_site, switch_site_response = switch_site(conn, new_content_url)
    conn.sign_out()
    assert switch_site_response.status_code == 200
    assert original_content_url != new_active_site['site']['contentUrl']
