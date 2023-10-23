import os
import shutil
import pandas as pd
from tableau_api_lib import TableauServerConnection
from pypdf import PdfMerger
from urllib import parse

#Erstellung der Klasse für den pdf Gen
class TableauExtension:

    #VAriablen um später den Status ans html zu schicken
    count_views = 0
    counter = 0
    status_percent = 0

    #Init - bei jeder Erstellung der Klasse wird eine Connection zum Tableau Server automatisch aufgebaut
    def __init__(self):
        self.status = self.status_percent
        self.connection = self.tableau_login()

    #Einfach eine Funktion um den Status rauszugeben
    def check_status(self):
        return self.status_percent
    
    #Testfunktion
    def change_status(self):
        self.status_percent = 7

    #Konfig zum anmelden. Kann auch in eine Datei gepackt werden und später als Secret behandelt werden
    @staticmethod
    def tableau_login():
        tableau_server_config = {
            'tableau_prod': {
                'server': 'http://tableau.demo.sgc.corp/',
                'api_version': '3.19',
                'personal_access_token_name': 'opv-python-token',
                'personal_access_token_secret': '+7NLXnzxR9S+UxhmvXfn8g==:e6jrJQVUDjOf8h2638VjJeuNgQ0bZS3V',
                'site_name': 'bnt-extension-poc',
                'site_url': 'bnt-extension-poc'
            }
        }
        conn = TableauServerConnection(tableau_server_config)
        conn.sign_in()
        return conn

    #Gibt die Workbook ID unseres gewünschten Workbooks zurück
    def get_workbook_id(self):
        WORKBOOK_NAME = '2023_OPV_TechStack_V1_Base'
        workbooks = self.connection.query_workbooks_for_site().json()['workbooks']['workbook']
        for workbook in workbooks:
            if workbook['name'] == WORKBOOK_NAME:
                return workbook['id']
        return workbooks.pop()['id']

    #Nutzt die Workbook ID um eine Liste aller worksheets in diesem Workbook rauszugeben - ist gefiltert auf OPV
    def query_viewnames_for_workbook(self):
        conn = self.connection
        view_list = []
        workbook_id = self.get_workbook_id()
        response = conn.query_views_for_workbook(workbook_id)
        views_list_dict = response.json()['views']['view']
        for view in views_list_dict:
            view_list.append((view['id'],view['name']))
        # take only the necessary dashboards
        df_complete = pd.DataFrame(view_list,columns = ['view_id','view_name'])
        #Filterschritt
        df = df_complete[df_complete['view_name'].str.contains("OPV")]
        return df


    #Funktion um die pdf abzuspeichern
    def save_as_pdf (self,pdf_merger):
        pdfPath = "APQR.pdf"
        
        pdfOutputFile = open(pdfPath, 'wb')
        pdf_merger.write(pdfOutputFile)
        pdfOutputFile.close()

    #Funktion um die einzelnen PDFs zu generieren und das abspeichern aufzurufen
    def create_pdf (self):
        #Filter Variablen, welche später noch manipuliert werden müssen
        tableau_filter_name = parse.quote('Result Name')
        tableau_filter_value = parse.quote('pH')
        FILE_PREFIX = 'bnt_'
        views = self.query_viewnames_for_workbook()#.head(5)
        pdf_list = []

        # Müssen immer wieder aktualisiert werden und durchiteriert werden
        pdf_params = {
            'type': 'type=A4',
            'orientation': 'orientation=Landscape',
            'filter_tableau': f'vf_{tableau_filter_name}={tableau_filter_value}'
        }

        try:
            shutil.rmtree('./temp/')
        except:
            print('Path not available')
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
                
                self.status_percent = round(self.counter/self.count_views*100,2)
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
    
    #Nutzt die Workbook ID um Hilfssheet zu bekommen.
    def create_csv (self):
        conn = self.connection
        view_list = []
        workbook_id = self.get_workbook_id()
        response = conn.query_views_for_workbook(workbook_id)
        views_list_dict = response.json()['views']['view']
        for view in views_list_dict:
            view_list.append((view['id'],view['name']))
        # take only the necessary dashboards
        df_complete = pd.DataFrame(view_list,columns = ['view_id','view_name'])
        #Filterschritt
        df = df_complete[df_complete['view_name'].str.contains("Helper")]
        view_id = str(df.head(1)['view_id'][10])
        print(view_id)
        response = conn.query_view_data(view_id)
        #self.save_as_csv(response)
        return response
    
    #Funktion um die csv abzuspeichern
    def save_as_csv (self,response):
        csvPath = "APQR.csv"
        if csvPath: #If the user didn't close the dialog window
            with open(csvPath, 'wb') as csv_file:
                csv_file.write(response.content)
                csv_file.close()

#conn.sign_out()
#print(get_workbook_id(tableau_login()))
#create_pdf()
#jo = TableauExtension()
#jo.create_csv()
# print(jo.check_status())
#jo.create_pdf()


