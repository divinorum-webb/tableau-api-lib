"""
Functions to add:
get_all_subscription_fields
get_sites_dataframe
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages, flatten_dict_column


def get_all_subscription_fields(conn):
    all_subscriptions = extract_pages(conn.query_subscriptions, parameter_dict={'fields': 'fields=_default_'})
    return all_subscriptions


def get_subscriptions_dataframe(conn):
    target_cols = ['content', 'schedule', 'user']
    subscriptions_df = pd.DataFrame(get_all_subscription_fields(conn))
    target_cols_present = [True for col in target_cols if col in subscriptions_df.columns]
    if not subscriptions_df.empty and all(target_cols_present):
        subscriptions_df = flatten_dict_column(subscriptions_df, keys=['id', 'type'], col_name='content')
        subscriptions_df = flatten_dict_column(subscriptions_df, keys=['id', 'name'], col_name='schedule')
        subscriptions_df = flatten_dict_column(subscriptions_df, keys=['id', 'name'], col_name='user')
    return subscriptions_df
