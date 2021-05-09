"""Helper functions for querying REST API data for extract refresh tasks."""


from typing import Any, Dict, List

import pandas as pd

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.exceptions.tableau_server_exceptions import ContentNotFound


def get_extract_refresh_tasks_for_site(conn: TableauServerConnection) -> List[Dict[str, Any]]:
    """Returns a list of Python dicts describing all extract refresh tasks for the active site."""
    try:
        return conn.get_extract_refresh_tasks_for_site().json()["tasks"]["task"]
    except KeyError:
        raise ContentNotFound(content_type="extract refresh tasks")


def get_extract_refresh_tasks_dataframe(conn: TableauServerConnection) -> pd.DataFrame:
    """Returns a DataFrame describing all extract refresh tasks for the site."""
    try:
        extract_refresh_tasks = [task["extractRefresh"] for task in get_extract_refresh_tasks_for_site(conn)]
        extract_refresh_tasks_df = pd.DataFrame(extract_refresh_tasks)
    except ContentNotFound:
        extract_refresh_tasks_df = pd.DataFrame()
    return extract_refresh_tasks_df
