import pandas as pd
import io
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_views_dataframe

WORKBOOK_NAME = '2023_OPV_BioNData_BatchExclusion_V2'
DASHBOARD_VIEW_NAME = 'Übersicht'
FILTER_VIEW_NAME = 'Kategorie'
FILTER_FIELD_NAME = 'Kategorie'
FILE_PREFIX = 'superstore_'
tableau_server_config = {
    'tableau_prod': {
        'server': 'http://tableau.demo.sgc.corp/',
        'api_version': '3.19',
        'username': 'kamipat',
        'password': 'P@ssw0rd',
        'site_name': 'bnt-extension-poc',
        'site_url': 'bnt-extension-poc'
    }
}


conn = TableauServerConnection(tableau_server_config)
conn.sign_in()
views = get_views_dataframe(conn)
dashboard_view_id = views[views['name'] == DASHBOARD_VIEW_NAME]['id'].values[0]
filter_view_id = views[views['name'] == FILTER_VIEW_NAME]['id'].values[0]
filter_data = conn.query_view_data(view_id=filter_view_id)
filter_df = pd.read_csv(io.StringIO(filter_data.content.decode('utf-8')))
filter_list = list(filter_df[FILTER_FIELD_NAME])
pdf_params = {
    'type': 'type=A4',
    'orientation': 'orientation=Landscape',
    'filter': None
}
for item in filter_list:
    pdf_params['filter'] = f'vf_{FILTER_FIELD_NAME}={item}'
    pdf = conn.query_view_pdf(view_id=dashboard_view_id, parameter_dict=pdf_params)
    with open(f'{FILE_PREFIX}{item}.pdf', 'wb') as pdf_file:
        pdf_file.write(pdf.content)
conn.sign_out()