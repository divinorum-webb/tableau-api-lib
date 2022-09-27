"""
Helper functions for querying REST API data for users
"""


from typing import Any, Dict, List, Optional

import pandas as pd

from tableau_api_lib.utils import extract_pages


def get_all_user_fields(conn, all_fields: Optional[bool] = True, page_size: int = 1000) -> List[Dict[str, Any]]:
    fields_param = "_all_" if all_fields is True else "_default_"
    all_users = extract_pages(conn.get_users_on_site, page_size=page_size, parameter_dict={"fields": f"fields={fields_param}"})
    return all_users


def get_all_user_names(conn):
    all_users = get_all_user_fields(conn)
    all_usernames = [user["name"] for user in all_users]
    return all_usernames


def get_all_user_emails(conn):
    all_users = get_all_user_fields(conn)
    all_user_emails = [user["email"] for user in all_users]
    return all_user_emails


def get_all_user_fullnames(conn):
    all_users = get_all_user_fields(conn)
    all_user_fullnames = [user["fullName"] for user in all_users]
    return all_user_fullnames


def get_all_user_roles(conn):
    all_users = get_all_user_fields(conn)
    all_user_roles = [user["siteRole"] for user in all_users]
    return all_user_roles


def get_users_dataframe(conn, all_fields: Optional[bool] = True, page_size: int = 1000) -> pd.DataFrame:
    users_df = pd.DataFrame(get_all_user_fields(conn, all_fields=all_fields, page_size=page_size))
    return users_df
