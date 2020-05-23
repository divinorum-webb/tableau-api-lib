import pandas as pd
from pandas.io.json import json_normalize

from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils import extract_pages, get_server_netloc
from tableau_api_lib.utils.cloning import clone_projects, clone_sites, clone_users, clone_groups, clone_datasources
from tableau_api_lib.utils.querying import get_users_dataframe, get_datasources_dataframe, get_groups_dataframe, \
    get_group_users_dataframe, get_workbooks_dataframe, get_projects_dataframe, get_sites_dataframe, \
    get_user_favorites_dataframe, get_schedules_dataframe, get_flows_dataframe, get_workbook_connections_dataframe, \
    get_datasource_connections_dataframe
from tableau_api_lib.utils.cloning.workbooks import clone_workbooks
from tableau_api_lib.utils.filemod import create_temp_dirs, delete_temp_files
from tableau_api_lib.utils.common import flatten_dict_column


pd.set_option('display.width', 600)
pd.set_option('display.max_columns', 10)


tableau_server_config = {
    'tableau_prod': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.6',
        'personal_access_token_name': 'estam_test',
        'personal_access_token_secret': 'r2ycVfAUS2CZVdVgOced/g==:NXrz9WwofT8x9AYLdZZmuiDZkqnIQ93A',
        'site_name': 'estam',
        'site_url': 'estam'
    }
}


conn = TableauServerConnection(tableau_server_config, env='tableau_prod')

conn.sign_in()

query = """
{
  databases {
    connectionType
    id
    name
    __typename
    tables {
      fullName
      name
      downstreamWorkbooks {
        name
        id
        projectName
        owner {
          username
          email
        }
        createdAt
        updatedAt
        embeddedDatasources {
          name
        }
        views {
          name
          id
          createdAt
          updatedAt
          __typename
        }
      }
      downstreamDatasources {
        name
        id
        projectName
        owner {
          username
          email
        }
        hasExtracts
        extractLastRefreshTime
        extractLastUpdateTime
        extractLastIncrementalUpdateTime
      }
      downstreamFlows {
        name
        id
        projectName
        owner {
          username
          email
        }
      }
    }
  }
}
"""

my_query = conn.metadata_graphql_query(query).json()

import json

with open('metadata_json.json', 'w') as outfile:
    json.dump(my_query, outfile)


def unpack_col(df, col_name):
    df_cols = [col for col in list(df.columns) if col != col_name]
    new_df = pd.DataFrame()
    for index, row in df.iterrows():
        if isinstance(row[col_name], list):
            for item in row[col_name]:
                new_row = row[df_cols].copy()
                for key in list(item.keys()):
                    key_alias = col_name + '_' + key
                    new_row[key_alias] = item[key]
                new_df = new_df.append(new_row, ignore_index=True)
        else:
            new_row = row[df_cols].copy()
            new_df = new_df.append(new_row, ignore_index=True)
        new_df['content_type'] = col_name
    return new_df


unpacked_df = pd.DataFrame()

full_df = pd.DataFrame(my_query['data']['databases'])
print(full_df)

print("unpacking 'tables'...")

full_df = unpack_col(full_df, 'tables')
print(full_df)
print(full_df.shape)

temp_df = unpack_col(full_df.drop(['tables_downstreamWorkbooks', 'tables_downstreamDatasources'], axis=1), 'tables_downstreamFlows')
print(temp_df)
print(temp_df.shape)

unpacked_df = unpacked_df.append(temp_df, sort=False)

temp_df = unpack_col(full_df.drop(['tables_downstreamWorkbooks', 'tables_downstreamFlows'], axis=1), 'tables_downstreamDatasources')
print(temp_df)
print(temp_df.shape)

unpacked_df = unpacked_df.append(temp_df, sort=False)

temp_df = unpack_col(full_df.drop(['tables_downstreamDatasources', 'tables_downstreamFlows'], axis=1), 'tables_downstreamWorkbooks')
print(temp_df)
print(temp_df.shape)

unpacked_df = unpacked_df.append(temp_df, sort=False)

temp_df = unpack_col(temp_df, 'tables_downstreamWorkbooks_views')
print(temp_df)
print(temp_df.shape)

unpacked_df = unpacked_df.append(temp_df, sort=False)

print(unpacked_df)
print(unpacked_df.shape)

unpacked_df.to_csv('delete_metadata_output.csv', header=True, index=False)

conn.sign_out()

