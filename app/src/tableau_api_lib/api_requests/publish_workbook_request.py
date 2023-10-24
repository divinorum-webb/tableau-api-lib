import os
import json
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata

from tableau_api_lib.tableau_server_connection import TableauServerConnection
from tableau_api_lib.api_requests import BaseRequest
from tableau_api_lib.exceptions import InvalidFileTypeException


CHUNK_SIZE = 1024 * 1024 * 5  # 5MB
FILE_SIZE_LIMIT = 1024 * 1024 * 60  # 60MB


class PublishWorkbookRequest(BaseRequest):
    """Builds the body for Tableau Server REST API requests publishing workbooks."""
    def __init__(self,
                 ts_connection: TableauServerConnection,
                 workbook_name: str,
                 workbook_file_path: str,
                 project_id,
                 show_tabs_flag=False,
                 user_id=None,
                 server_address=None,
                 port_number=None,
                 connection_username=None,
                 connection_password=None,
                 embed_credentials_flag=False,
                 oauth_flag=False,
                 workbook_views_to_hide=None,
                 hide_view_flag=False):

        super().__init__(ts_connection)
        self._workbook_name = workbook_name
        self._workbook_file_path = workbook_file_path
        self._project_id = project_id
        self._show_tabs_flag = show_tabs_flag
        self._user_id = user_id
        self._server_address = server_address
        self._port_number = port_number
        self._connection_username = connection_username
        self._connection_password = connection_password
        self._embed_credentials_flag = embed_credentials_flag
        self._oauth_flag = oauth_flag
        self._workbook_views_to_hide = workbook_views_to_hide
        self._hide_view_flag = hide_view_flag
        self.payload = None
        self.content_type = None
        self._verify_embed_requirements()
        self._listify_inputs()
        self._file_is_chunked = self._file_requires_chunking()
        self.base_publish_workbook_request()

    def _listify_inputs(self):
        if any(self.optional_connection_param_values):
            if isinstance(self._server_address, str):
                self._server_address = [self._server_address]
            if isinstance(self._port_number, str):
                self._port_number = [self._port_number]
        if self._connection_username:
            if isinstance(self._connection_username, str):
                self._connection_username = [self._connection_username]
        if self._connection_password:
            if isinstance(self._connection_password, str):
                self._connection_password = [self._connection_password]
        if self._embed_credentials_flag:
            if isinstance(self._embed_credentials_flag, bool) or isinstance(self._embed_credentials_flag, str):
                self._embed_credentials_flag = [self._embed_credentials_flag]
        if isinstance(self._oauth_flag, bool) or isinstance(self._oauth_flag, str):
            self._oauth_flag = [self._oauth_flag]

    def _verify_embed_requirements(self):
        if self._embed_credentials_flag:
            if (self._server_address or self._oauth_flag) and self._connection_username:
                pass
            else:
                self._invalid_parameter_exception()

    @property
    def valid_file_extensions(self):
        return [
            'twb',
            'twbx'
        ]

    @property
    def optional_workbook_param_keys(self):
        return [
            'showTabs',
            'generateThumbnailsAsUser'
        ]

    @property
    def optional_connection_param_keys(self):
        return [
            'serverAddress',
            'serverPort'
        ]

    @property
    def optional_credentials_param_keys(self):
        return [
            'name',
            'password',
            'embed',
            'oAuth'
        ]

    @property
    def optional_view_param_keys(self):
        return [
            'name',
            'hidden'
        ]

    @property
    def optional_workbook_param_values(self):
        return [
            self._show_tabs_flag,
            self._user_id
        ]

    @property
    def optional_connection_param_values(self):
        return [
            self._server_address,
            self._port_number
        ]

    @property
    def optional_credentials_param_values(self):
        return [
            self._connection_username,
            self._connection_password,
            self._embed_credentials_flag,
            self._oauth_flag
        ]

    @property
    def optional_view_param_values(self):
        values_list = []
        if self._hide_view_flag:
            [values_list.append(view_name) for view_name in self._workbook_views_to_hide]
        else:
            [values_list.append(None) for _ in self.optional_view_param_keys]
        return values_list

    def base_publish_workbook_request(self):
        self._request_body.update({
            'workbook': {
                'name': self._workbook_name,
                'project': {'id': self._project_id}
            }
        })
        return self._request_body

    def modified_publish_workbook_request(self):
        self._request_body['workbook'].update(self._get_parameters_dict(self.optional_workbook_param_keys,
                                                                        self.optional_workbook_param_values))
        if any(self.optional_connection_param_values) or any(self.optional_credentials_param_values):
            self._request_body['workbook'].update({'connections': {'connection': []}})
            for i, _ in enumerate(self._connection_username or [None]):
                self._request_body['workbook']['connections']['connection'].append({
                    'serverAddress': self._server_address[i] if self._server_address else None,
                    'serverPort': self._port_number[i] if self._port_number else None,
                    'connectionCredentials': {
                        'name': self._connection_username[i] if self._connection_username else None,
                        'password': self._connection_password[i] if self._connection_password else None,
                        'embed': self._embed_credentials_flag[i] if self._embed_credentials_flag else False
                    }
                })
            if any(self._oauth_flag):
                for i, _ in enumerate(self._oauth_flag):
                    self._request_body['workbook']['connections']['connection'][i]['connectionCredentials'].update({
                        'oAuth': self._oauth_flag[i]
                    })

        if any(self.optional_view_param_values):
            self._request_body['workbook'].update({'views': {'view': []}})
            for i, view in enumerate(self.optional_view_param_values):
                self._request_body['workbook']['views']['view'].append({
                    'name': view,
                    'hidden': 'true'
                })
        return self._request_body

    def _file_requires_chunking(self):
        file_size = os.path.getsize(self._workbook_file_path)
        if file_size > FILE_SIZE_LIMIT:
            return True

    def get_workbook(self):
        workbook_file = os.path.basename(self._workbook_file_path)
        with open(self._workbook_file_path, 'rb') as f:
            workbook_bytes = f.read()
        file_extension = workbook_file.split('.')[-1]
        if file_extension in self.valid_file_extensions:
            pass
        else:
            raise InvalidFileTypeException(self.__class__.__name__,
                                           file_variety='workbook',
                                           file_extension=file_extension)
        return workbook_file, workbook_bytes

    # testing for chunk upload
    def publish_prep(self, publish_content_type, parameter_dict):
        parameter_dict = parameter_dict if parameter_dict else {'overwrite': 'overwrite=true'}
        filename = os.path.basename(self._workbook_file_path)
        file_extension = filename.split('.')[-1]

        if self._file_is_chunked:
            upload_session_id = self._connection.initiate_file_upload().json()['fileUpload']['uploadSessionId']
            parameter_dict.update({'param': 'uploadSessionId={}'.format(upload_session_id)})
            chunks = self.read_chunks(self._workbook_file_path)
            for chunk in chunks:
                request, append_content_type = self.chunk_req(chunk)
                file_upload = self._connection.append_to_file_upload(upload_session_id=upload_session_id,
                                                                     payload=request,
                                                                     content_type=append_content_type)

        publishing_headers = self._connection.default_headers.copy()
        publishing_headers.update({'content-type': publish_content_type})
        parameter_dict.update({'workbookType': 'workbookType={}'.format(file_extension)})
        return publishing_headers, parameter_dict

    @staticmethod
    def read_chunks(file_path):
        with open(file_path, 'rb') as f:
            while True:
                chunked_content = f.read(CHUNK_SIZE)
                if not chunked_content:
                    break
                yield chunked_content

    def chunk_req(self, chunk):
        parts = {'request_payload': (None, '', 'application/json'),
                 'tableau_file': ('file', chunk, 'application/octet-stream')}
        return self._add_multipart(parts)

    @staticmethod
    def _add_multipart(parts):
        mime_multipart_parts = list()
        for name, (filename, data, content_type) in parts.items():
            multipart_part = RequestField(name=name, data=data, filename=filename)
            multipart_part.make_multipart(content_type=content_type)
            mime_multipart_parts.append(multipart_part)
        request, content_type = encode_multipart_formdata(mime_multipart_parts)
        content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
        return request, content_type

    def _publish_chunked_file_request(self):
        request = self.modified_publish_workbook_request()
        parts = {'request_payload': (None, json.dumps(request), 'application/json')}
        return self._add_multipart(parts)

    def _publish_single_file_request(self):
        request = self.modified_publish_workbook_request()
        workbook_file, workbook_bytes = self.get_workbook()
        parts = {'request_payload': (None, json.dumps(request), 'application/json'),
                 'tableau_workbook': (workbook_file, workbook_bytes, 'application/octet-stream')}
        return self._add_multipart(parts)

    def get_request(self):
        if self._file_is_chunked:
            return self._publish_chunked_file_request()
        else:
            return self._publish_single_file_request()
