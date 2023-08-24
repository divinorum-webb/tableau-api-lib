import json
import os
import shutil
import tkinter
from tkinter import filedialog
import pandas as pd
import io
from tableau_api_lib import TableauServerConnection
from tableau_api_lib.utils.querying import get_views_dataframe
from pypdf import PdfMerger
from tkinter import *
from tkinter.filedialog import asksaveasfile


def tableau_login():
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
    return conn


def get_workbook_id(connection):
    WORKBOOK_NAME = '2023_OPV_BioNData_BatchExclusion_V2'
    workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
    for workbook in workbooks:
        if workbook['name'] == WORKBOOK_NAME:
            return workbook['id']
    return workbooks.pop()['id']

def query_viewnames_for_workbook():
    conn = tableau_login()
    view_list = []
    workbook_id = get_workbook_id(conn)
    response = conn.query_views_for_workbook(workbook_id)
    views_list_dict = response.json()['views']['view']
    for view in views_list_dict:
        view_list.append((view['id'],view['name']))
    df = pd.DataFrame(view_list,columns = ['view_id','view_name'])
    conn.sign_out()
    return df



def save_as_pdf (pdf_merger):
    root = tkinter.Tk()
    root.withdraw()
    pdfPath = filedialog.asksaveasfilename(defaultextension = "*.pdf", filetypes = (("PDF Files", "*.pdf"),))
    if pdfPath: #If the user didn't close the dialog window
        pdfOutputFile = open(pdfPath, 'wb')
        pdf_merger.write(pdfOutputFile)
        pdfOutputFile.close()

def create_pdf ():
    FILE_PREFIX = 'bnt_'
    views = query_viewnames_for_workbook().head(5)
    pdf_list = []
    pdf_params = {
        'type': 'type=A4',
        'orientation': 'orientation=Landscape'
    }
    os.mkdir('./temp')

    conn = tableau_login()
    for ind in views.index:
        #print(views['view_id'][ind])
        view_string = views['view_id'][ind]
        #print(type(view_string))
        pdf = conn.query_view_pdf(view_id=view_string, parameter_dict=pdf_params)
        with open('./temp/'+f'{FILE_PREFIX}{view_string}.pdf', 'wb') as pdf_file:
            pdf_file.write(pdf.content)
            pdf_list.append(pdf_file.name)
    merger = PdfMerger()
    for pdf in pdf_list:
        merger.append(pdf)
    save_as_pdf(merger)
    #merger.write('result.pdf')
    merger.close()
    conn.sign_out()
    shutil.rmtree('./temp/')
    return 'PDF created'

#conn.sign_out()
#print(get_workbook_id(tableau_login()))
#create_pdf()

