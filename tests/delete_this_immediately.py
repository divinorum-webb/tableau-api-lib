from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_groups_dataframe, get_group_users_dataframe, get_schedules_dataframe, \
    get_subscriptions_dataframe, get_webhooks_dataframe
from tableau_api_lib.utils.cloning import clone_schedules, clone_schedule_state
from tableau_api_lib.utils import querying


TEST_GROUP_ID = 'd36d4a8a-9dbe-435b-b0a5-c0cf7fa153b0'
TEST_SCHEDULE_ID = 'f1846ec7-b1e2-45f0-b946-d3503900d431'
TEST_CONTENT_ID = '89fbbd98-3c80-41b0-b8b1-2454f19b9c3e'
TEST_CONTENT_TYPE = 'view'
TEST_SUBSCRIPTION_SUBJECT = 'Group Subscription Tutorial'

tableau_server_config = {
    'my_env': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.7',
        'username': 'estam',
        'password': 'Act1Andariel!',
        'site_name': 'estam',
        'site_url': 'estam'
    },
    'dev': {
        'server': 'https://10ax.online.tableau.com',
        'api_version': '3.8',
        'personal_access_token_name': 'datadev-temp',
        'personal_access_token_secret': 'qyKnzy3AS3mbAjp4eVL2OQ==:7gayFzBtsSpyCsKTRxjE75lvNSc2keDI',
        'site_name': 'estam-iw-dev-422309',
        'site_url': 'estamiwdev422309'
    }
}


conn = TableauServerConnection(tableau_server_config, 'tableau_poc')
conn.sign_in()

import pandas as pd
from pandas.io.json import json_normalize

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import flatten_dict_column, flatten_dict_list_column


#  using personal access tokens is preferred; otherwise, comment those details out and use username / password
tableau_server_config = {
    'my_env': {
        'server': 'https://10ax.online.tableau.com',  # replace with your own server
        'api_version': '3.8',  # replace with your REST API version
        'personal_access_token_name': '<PAT NAME>',
        'personal_access_token_secret': '<PAT SECRET',
#         'username': '<USERNAME>',
#         'password': '<PASSWORD>',
        'site_name': 'your-pretty-site-name',  # if accessing your default site, set this to ''
        'site_url': 'YourSiteContentUrl'  # if accessing your default site, set this to ''
    }
}


conn = TableauServerConnection(tableau_server_config, 'my_env')
conn.sign_in()

#  define the GraphQL queries to run against the Metadata API
query_workbooks = """
{
  workbooks {
    workbook_name: name
    workbook_id: luid
    workbook_project: projectName
    views {
      view_type: __typename
      view_name: name
      view_id: luid
    }
    upstreamTables {
      upstr_table_name: name
      upstr_table_id: luid
      upstreamDatabases {
        upstr_db_name: name
        upstr_db_type: connectionType
        upstr_db_id: luid
        upstr_db_isEmbedded: isEmbedded
      }
    }
    upstreamDatasources {
      upstr_ds_name: name
      upstr_ds_id: luid
      upstr_ds_project: projectName
    }
    embeddedDatasources {
      emb_ds_name: name
    }
    upstreamFlows {
      flow_name: name
      flow_id: luid
      flow_project: projectName
    }
  }
}
"""

query_databases = """
{
  databaseServers {
    database_hostname: hostName
		database_port: port
    database_id: luid
  }
}
"""

def get_metadata_json(conn, query, content_type):
    results = conn.metadata_graphql_query(query)
    results_json = results.json()['data'][content_type]
    return results_json

def get_workbook_metadata_df(json_data):
    df = json_normalize(json_data)
    cols_to_drop = ['views', 'upstreamTables', 'upstreamDatasources', 'embeddedDatasources', 'upstreamFlows']
    df.drop(columns=cols_to_drop, inplace=True)
    return df

def get_view_metadata_df(json_data):
    df = json_normalize(data=json_data, record_path='views', meta='workbook_id')
    return df

def get_tables_metadata_df(json_data):
    df = json_normalize(data=json_data, record_path='upstreamTables', meta='workbook_id')
    df = flatten_dict_list_column(df, col_name='upstreamDatabases')
    return df

def get_database_metadata_df(json_data):
    return pd.DataFrame(json_data)

def get_datasource_metadata_df(json_data):
    wb_upstream_ds_df = json_normalize(data=json_data, record_path='upstreamDatasources', meta='workbook_id')
    wb_embedded_ds_df = json_normalize(data=json_data, record_path='embeddedDatasources', meta='workbook_id')
    df = wb_upstream_ds_df.merge(wb_embedded_ds_df, how='left', on='workbook_id')
    return df

def get_flow_metadata_df(json_data):
    df = json_normalize(data=json_data, record_path='upstreamFlows', meta='workbook_id')
    return df

def get_combined_metadata_df(wb_json, db_json):
    wb_df = get_workbook_metadata_df(wb_json)
    wb_views_df = get_view_metadata_df(wb_json)
    wb_tables_df = get_tables_metadata_df(wb_json)
    db_df = get_database_metadata_df(db_json)
    wb_ds_df = get_datasource_metadata_df(wb_json)
    wb_flows_df = get_flow_metadata_df(wb_json)
    combined_df = wb_df.merge(wb_views_df, how='left', on='workbook_id')
    combined_df = combined_df.merge(wb_tables_df, how='left', on='workbook_id')
    combined_df = combined_df.merge(db_df, how='left', left_on='upstr_db_id', right_on='database_id')
    combined_df = combined_df.merge(wb_ds_df, how='left', on='workbook_id')
    combined_df = combined_df.merge(wb_flows_df, how='left', on='workbook_id')
    return combined_df

def add_contextual_columns(df, site_name):
    df['summary_date'] = pd.datetime.now()
    df['site_name'] = site_name
    return df

#  query metadata from our Tableau environment
wb_query_results_json = get_metadata_json(conn, query_workbooks, 'workbooks')
db_query_results_json = get_metadata_json(conn, query_databases, 'databaseServers')

combined_df = get_combined_metadata_df(wb_query_results_json, db_query_results_json)

#  add in the site name and the date the process was run
combined_df = add_contextual_columns(combined_df, conn.site_name)

#  output the resulting data to a CSV file
combined_df.to_csv('impact_analysis_milestone1.csv', header=True, index=False)

conn.sign_out()
