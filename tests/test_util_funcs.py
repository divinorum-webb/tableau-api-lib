from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import extract_pages


tableau_server_config = {
    'tableau_prod': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.4',
        'username': 'estam',
        'password': 'Act1Andariel!',
        'site_name': 'estam',
        'site_url': 'estam',
        'cache_buster': 'Donut',
        'temp_dir': '/dags/tableau/temp/'
    }
}


conn = TableauServerConnection(tableau_server_config)


def test_sign_in():
    conn.sign_in()


def test_extract_pages():
    test_query = conn.query_sites().json()
    total_sites_available = int(test_query['pagination']['totalAvailable'])
    extracted_sites = extract_pages(conn.query_sites)
    print("\nNumber of extracted sites: {}\nNumber of available sites: {}".format(len(extracted_sites),
                                                                                  total_sites_available))
    assert len(extracted_sites) == total_sites_available


def test_sign_out():
    conn.sign_out()
