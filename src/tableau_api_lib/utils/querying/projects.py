"""
Functions to add:
get_all_project_fields
get_projects_dataframe
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_project_fields(conn):
    all_projects = extract_pages(conn.query_projects, parameter_dict={'fields': 'fields=_all_'})
    return all_projects


def get_projects_dataframe(conn):
    projects_df = pd.DataFrame(get_all_project_fields(conn))
    return projects_df
