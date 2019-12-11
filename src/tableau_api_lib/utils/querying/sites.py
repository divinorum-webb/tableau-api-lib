"""
Functions to add:
get_all_site_fields
get_sites_dataframe
"""


def get_active_site_name(conn):
    return conn.query_site().json()['site']['name']


def get_active_site_content_url(conn):
    return conn.query_site().json()['site']['contentUrl']
