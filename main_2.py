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

class TableauExtension:

    count_views = 0
    counter = 0
    status_percent = 0

    def __init__(self):
        self.status = self.status_percent
        self.connection = self.tableau_login()

    def check_status(self):
        return self.status_percent
    
    def change_status(self):
        self.status_percent = 7


    @staticmethod
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


    def get_workbook_id(self):
        WORKBOOK_NAME = '2023_OPV_BioNData_BatchExclusion_V2'
        workbooks = self.connection.query_workbooks_for_site().json()['workbooks']['workbook']
        for workbook in workbooks:
            if workbook['name'] == WORKBOOK_NAME:
                return workbook['id']
        return workbooks.pop()['id']

    def query_viewnames_for_workbook(self):
        conn = self.connection
        view_list = []
        workbook_id = self.get_workbook_id()
        response = conn.query_views_for_workbook(workbook_id)
        views_list_dict = response.json()['views']['view']
        for view in views_list_dict:
            view_list.append((view['id'],view['name']))
        df = pd.DataFrame(view_list,columns = ['view_id','view_name'])
        return df



    def save_as_pdf (pdf_merger):
        root = tkinter.Tk()
        root.withdraw()
        pdfPath = filedialog.asksaveasfilename(defaultextension = "*.pdf", filetypes = (("PDF Files", "*.pdf"),))
        if pdfPath: #If the user didn't close the dialog window
            pdfOutputFile = open(pdfPath, 'wb')
            pdf_merger.write(pdfOutputFile)
            pdfOutputFile.close()

    def create_pdf (self):
        FILE_PREFIX = 'bnt_'
        views = self.query_viewnames_for_workbook().head(5)
        pdf_list = []
        pdf_params = {
            'type': 'type=A4',
            'orientation': 'orientation=Landscape'
        }
        os.mkdir('./temp')

        self.count_views = len(views.index)
        print(self.count_views)

        conn = self.connection
        for ind in views.index:
            #print(views['view_id'][ind])
            self.counter = self.counter + 1
        
            view_string = views['view_id'][ind]
            #print(type(view_string))
            pdf = conn.query_view_pdf(view_id=view_string, parameter_dict=pdf_params)
            with open('./temp/'+f'{FILE_PREFIX}{view_string}.pdf', 'wb') as pdf_file:
                pdf_file.write(pdf.content)
                pdf_list.append(pdf_file.name)
                
                self.status_percent = self.counter/self.count_views*100
                print(self.status_percent)
        merger = PdfMerger()
        for pdf in pdf_list:
            merger.append(pdf)
        self.save_as_pdf(merger)
        #merger.write('result.pdf')

        #clean up
        merger.close()
        conn.sign_out()
        shutil.rmtree('./temp/')
        self.count_views = 0
        self.counter = 0
        self.status_percent = 0

        return 'PDF created'

#conn.sign_out()
#print(get_workbook_id(tableau_login()))
#create_pdf()

jo = TableauExtension()
# print(jo.check_status())
# print(jo.change_status())
# print(jo.check_status())
# print(jo.status)
#print(jo.query_viewnames_for_workbook())
jo.create_pdf()
#test

