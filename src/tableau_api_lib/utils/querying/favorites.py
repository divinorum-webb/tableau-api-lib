"""
Functions to add:
get_favorites_dataframe
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_favorite_fields(conn, user_id):
    all_favorites = conn.get_favorites_for_user(user_id).json()['favorites']['favorite']
    return all_favorites


def get_user_favorites_dataframe(conn, user_id):
    favorites_df = pd.DataFrame(get_all_favorite_fields(conn, user_id))
    return favorites_df

