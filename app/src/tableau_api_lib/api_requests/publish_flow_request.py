import os
import json
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata

from tableau_api_lib.api_requests import BaseRequest


CHUNK_SIZE = 1024 * 1024 * 5  # 5MB
FILE_SIZE_LIMIT = 1024 * 1024 * 60  # 60MB


class PublishFlowRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests publishing flows.
    :param class ts_connection: the Tableau Server connection object
    :param string flow_name: the name for the flow being published
    :param string flow_file_path: the file path for the flow being published
    :param string project_id: the project ID for the project the flow belongs to
    :param string flow_description: (Optional) s description for the flow being published
    :param list server_address: (Optional) list of string values for each flow connection server address
    :param list port_number: (Optional) list of string values for each flow connection server port number
    :param list connection_username: (Optional) list of string values for each flow connection username
    :param list connection_password: (Optional) list of string values for each flow connection password
    :param list embed_credentials_flag: (Optional) list of boolean values; True if embedding credentials, False if not
    :param list oauth_flag: (Optional) list of boolean values; True if using OAuth, False if not
    """
    def __init__(self,
                 ts_connection,
                 flow_name,
                 flow_file_path,
                 project_id,
                 flow_description=None,
                 server_address=None,
                 port_number=None,
                 connection_username=None,
                 connection_password=None,
                 embed_credentials_flag=False,
                 oauth_flag=False
                 ):
        super().__init__(ts_connection)
        self._flow_name = flow_name
        self._flow_file_path = flow_file_path
        self._project_id = project_id
        self._flow_description = flow_description
        self._server_address = server_address
        self._port_number = port_number
        self._connection_username = connection_username
        self._connection_password = connection_password
        self._embed_credentials_flag = embed_credentials_flag
        self._oauth_flag = oauth_flag
        self._listify_inputs()
        self._file_is_chunked = self._file_requires_chunking()
        self.base_publish_flow_request()

    def _listify_inputs(self):
        if any(self.optional_credentials_param_values) or any(self.optional_connection_param_values):
            if isinstance(self._server_address, str):
                self._server_address = [self._server_address]
            if isinstance(self._port_number, str):
                self._port_number = [self._port_number]
            if isinstance(self._connection_username, str):
                self._connection_username = [self._connection_username]
            if isinstance(self._connection_password, str):
                self._connection_password = [self._connection_password]
            if isinstance(self._embed_credentials_flag, bool) or isinstance(self._embed_credentials_flag, str):
                self._embed_credentials_flag = [self._embed_credentials_flag]
            if isinstance(self._oauth_flag, bool) or isinstance(self._oauth_flag, str):
                self._oauth_flag = [self._oauth_flag]

    @property
    def valid_file_extensions(self):
        return [
            'tfl',
            'tflx'
        ]

    @property
    def optional_flow_param_keys(self):
        return [
            'description'
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
    def optional_flow_param_values(self):
        return [
            self._flow_description
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

    def base_publish_flow_request(self):
        self._request_body.update({
            'flow': {
                'name': self._flow_name,
                'project': {'id': self._project_id}
            }
        })
        return self._request_body

    def modified_publish_flow_request(self):
        self._request_body['flow'].update(self._get_parameters_dict(self.optional_flow_param_keys,
                                                                    self.optional_flow_param_values))

        if any(self.optional_connection_param_values) or any(self.optional_credentials_param_values):
            self._request_body['flow'].update({'connections': {'connection': []}})
            for i, _ in enumerate(self._connection_username or [None]):
                self._request_body['flow']['connections']['connection'].append({
                    'serverAddress': self._server_address[i] if self._server_address else None,
                    'portNumber': self._port_number[i] if self._port_number else None,
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

        return self._request_body

    def _file_requires_chunking(self):
        file_size = os.path.getsize(self._flow_file_path)
        if file_size > FILE_SIZE_LIMIT:
            return True

    def get_flow(self):
        flow_file = os.path.basename(self._flow_file_path)
        with open(self._flow_file_path, 'rb') as f:
            flow_bytes = f.read()
        file_extension = flow_file.split('.')[-1]
        if file_extension in self.valid_file_extensions:
            pass
        else:
            raise Exception('Invalid flow type provided. flow must be a .tfl or .tflx file.')
        return flow_file, flow_bytes

    # testing for chunk upload
    def publish_prep(self, publish_content_type, parameter_dict):
        parameter_dict = parameter_dict if parameter_dict else {'overwrite': 'overwrite=true'}
        filename = os.path.basename(self._flow_file_path)
        file_extension = filename.split('.')[-1]

        if self._file_is_chunked:
            upload_session_id = self._connection.initiate_file_upload().json()['fileUpload']['uploadSessionId']
            parameter_dict.update({'param': 'uploadSessionId={}'.format(upload_session_id)})
            chunks = self.read_chunks(self._flow_file_path)
            for chunk in chunks:
                request, append_content_type = self.chunk_req(chunk)
                file_upload = self._connection.append_to_file_upload(upload_session_id=upload_session_id,
                                                                     payload=request,
                                                                     content_type=append_content_type)

        publishing_headers = self._connection.default_headers.copy()
        publishing_headers.update({'content-type': publish_content_type})
        parameter_dict.update({'flowType': 'flowType={}'.format(file_extension)})
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
        request = self.modified_publish_flow_request()
        parts = {'request_payload': (None, json.dumps(request), 'application/json')}
        return self._add_multipart(parts)

    def _publish_single_file_request(self):
        request = self.modified_publish_flow_request()
        flow_file, flow_bytes = self.get_flow()
        parts = {'request_payload': (None, json.dumps(request), 'application/json'),
                 'tableau_flow': (flow_file, flow_bytes, 'application/octet-stream')}
        return self._add_multipart(parts)

    def get_request(self):
        if self._file_is_chunked:
            return self._publish_chunked_file_request()
        else:
            return self._publish_single_file_request()
