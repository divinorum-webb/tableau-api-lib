"""
Functions to add:
get_all_site_fields
get_sites_dataframe
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_site_fields(conn):
    all_sites = extract_pages(conn.query_sites, parameter_dict={'fields': 'fields=_all_'})
    return all_sites


def get_sites_dataframe(conn, site_names=None, content_urls=None):
    sites_df = pd.DataFrame(get_all_site_fields(conn))
    if site_names:
        sites_df = sites_df[sites_df['name'].isin(site_names)]
    if content_urls:
        sites_df = sites_df[sites_df['contentUrl'].isin(content_urls)]
    return sites_df


def get_active_site_name(conn):
    try:
        return conn.query_site().json()['site']['name']
    except KeyError:
        raise Exception("Unable to query the site. Only site admins and server admins can query the site.")


def get_active_site_id(conn):
    try:
        return conn.query_site().json()['site']['id']
    except KeyError:
        raise Exception("Unable to query the site. Only site admins and server admins can query the site.")


def get_active_site_content_url(conn):
    try:
        return conn.query_site().json()['site']['contentUrl']
    except KeyError:
        raise Exception("Unable to query the site. Only site admins and server admins can query the site.")
