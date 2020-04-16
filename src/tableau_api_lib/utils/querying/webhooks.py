import pandas as pd


def get_webhooks_dataframe(conn) -> pd.DataFrame:
    """
    Returns a Pandas DataFrame containing details for all webhooks on the active site.
    :param class conn: the Tableau Server connection
    :return: pd.DataFrame
    """
    try:
        webhooks_json = conn.query_webhooks().json()['webhooks']['webhook']
    except KeyError:
        webhooks_json = {}
    webhooks_df = pd.DataFrame(webhooks_json)
    return webhooks_df
