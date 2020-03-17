"""
Functions to add:
get_all_subscription_fields
get_sites_dataframe
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages, flatten_dict_column


def get_all_subscription_fields(conn):
    all_subscriptions = extract_pages(conn.query_subscriptions, parameter_dict={'fields': 'fields=_all_'})
    return all_subscriptions


def get_subscriptions_dataframe(conn):
    subscriptions_df = pd.DataFrame(get_all_subscription_fields(conn))
    subscriptions_df = flatten_dict_column(subscriptions_df, keys=['id', 'type'], col_name='content')
    subscriptions_df = flatten_dict_column(subscriptions_df, keys=['id', 'name'], col_name='schedule')
    subscriptions_df = flatten_dict_column(subscriptions_df, keys=['id', 'name'], col_name='user')
    return subscriptions_df
