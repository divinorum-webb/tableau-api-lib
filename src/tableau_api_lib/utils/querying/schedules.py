"""
Functions to add:
get_all_schedule_fields
get_schedules_dataframe
"""


import pandas as pd
from tableau_api_lib.utils import extract_pages


def get_all_schedule_fields(conn):
    all_schedules = extract_pages(conn.query_schedules, parameter_dict={'fields': 'fields=_all_'})
    return all_schedules


def get_schedules_dataframe(conn):
    schedules_df = pd.DataFrame(get_all_schedule_fields(conn))
    return schedules_df
