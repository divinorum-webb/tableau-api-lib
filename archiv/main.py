import json
import tkinter
from tkinter import filedialog
import pandas as pd
import io
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_views_dataframe
from pypdf import PdfMerger
from tkinter import *
from tkinter.filedialog import asksaveasfile

WORKBOOK_NAME = '2023_OPV_BioNData_BatchExclusion_V2'
DASHBOARD_VIEW_NAME = 'Übersicht'
FILTER_VIEW_NAME = 'Kategorie'
FILTER_FIELD_NAME = 'Kategorie'
FILE_PREFIX = 'bnt_'
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

#print(conn.query_sites().json())

def get_workbook_id(connection):
    workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
    for workbook in workbooks:
        if workbook['name'] == WORKBOOK_NAME:
            return workbook['id']
    return workbooks.pop()['id']

print(get_workbook_id(conn))

def query_viewnames_for_workbook():
    view_list = []
    workbook_id = get_workbook_id(conn)
    response = conn.query_views_for_workbook(workbook_id)
    views_list_dict = response.json()['views']['view']
    for view in views_list_dict:
        view_list.append((view['id'],view['name']))
    df = pd.DataFrame(view_list,columns = ['view_id','view_name'])
    return df
#print(query_viewnames_for_workbook()) #!!!! benutzen

views = query_viewnames_for_workbook().head(2)

# workbook_list = []
# workbooks = conn.query_workbooks_for_site().json()['workbooks']['workbook']
# def get_test_workbook_list(connection):
#     workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
#     for workbook in workbooks:
#         if workbook['name'] == WORKBOOK_NAME:
#             workbook_list.append(workbook['name'])
#     return workbook_list
# print(get_test_workbook_list(conn))

#-------------------------------------------------------------- gegebener Dataframe - nicht so geil weil verschachtelt
# all_views = get_views_dataframe(conn)
# relevant_views = all_views.loc[all_views['workbook'] == get_workbook_id(conn)]
# relevant_views = {key: all_views[key] for key in all_views.keys()
#        & {'name'}}
# print(relevant_views)

# for col in all_views.columns:
#     print(col)
# print(relevant_views)
#--------------------------------------------------------------

#-------------------------------------------------------------- save
root = tkinter.Tk()
root.withdraw()
  
# function to call when user press
# the save button, a filedialog will
# open and ask to save file

# dashboard_view_id = views[views['name'] == DASHBOARD_VIEW_NAME]['id'].values[0]
# #print(views[['name', 'id']])
# filter_view_id = views[views['name'] == FILTER_VIEW_NAME]['id'].values[0]
# filter_data = conn.query_view_data(view_id=filter_view_id)
# filter_df = pd.read_csv(io.StringIO(filter_data.content.decode('utf-8')))
# filter_list = list(filter_df[FILTER_FIELD_NAME])
pdf_list = []
pdf_params = {
    'type': 'type=A4',
    'orientation': 'orientation=Landscape'
}
# for item in filter_list:
#     pdf_params['filter'] = f'vf_{FILTER_FIELD_NAME}={item}'
#     pdf = conn.query_view_pdf(view_id=dashboard_view_id, parameter_dict=pdf_params)
#     with open(f'{FILE_PREFIX}{item}.pdf', 'wb') as pdf_file:
#         pdf_file.write(pdf.content)
#         pdf_list.append(pdf_file.name)


#----------------------------------------------------------------------------- benötigt
for ind in views.index:
    #print(views['view_id'][ind])
    view_string = views['view_id'][ind]
    #print(type(view_string))
    pdf = conn.query_view_pdf(view_id=view_string, parameter_dict=pdf_params)
    with open(f'{FILE_PREFIX}{view_string}.pdf', 'wb') as pdf_file:
        pdf_file.write(pdf.content)
        pdf_list.append(pdf_file.name)


#----------------------------------------------------------------------------- pdf merger und def
def save_as_pdf (pdf_merger):
    pdfPath = filedialog.asksaveasfilename(defaultextension = "*.pdf", filetypes = (("PDF Files", "*.pdf"),))
    if pdfPath: #If the user didn't close the dialog window
        pdfOutputFile = open(pdfPath, 'wb')
        pdf_merger.write(pdfOutputFile)
        pdfOutputFile.close()

def merge_pdf ():
    merger = PdfMerger()
    for pdf in pdf_list:
        merger.append(pdf)
    save_as_pdf(merger)
    #merger.write('result.pdf')
    merger.close()

conn.sign_out()

