"""
Functions to add:
get_favorites_dataframe
"""


import pandas as pd


def get_all_favorite_fields(conn, user_id) -> list:
    """
    Queries all available favorites for the specified user on Tableau Server.
    :param class conn: the Tableau Server connection
    :param str user_id: the Tableau Server user whose favorites will be queried
    :return: list
    """
    try:
        all_favorites = conn.get_favorites_for_user(user_id).json()['favorites']['favorite']
    except KeyError:
        all_favorites = {}
    return all_favorites


def get_user_favorites_dataframe(conn, user_id) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame containing details for the specified user's favorites on Tableau Server.
    :param class conn: the Tableau Server connection
    :param str user_id: the Tableau Server user whose favorites will be queried
    :return: pd.DataFrame
    """
    favorites_df = pd.DataFrame(get_all_favorite_fields(conn, user_id))
    return favorites_df
