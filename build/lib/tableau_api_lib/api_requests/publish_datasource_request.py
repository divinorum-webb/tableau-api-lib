import os
import json
from urllib3.fields import RequestField
from urllib3.filepost import encode_multipart_formdata

from tableau_api_lib.api_requests import BaseRequest
from tableau_api_lib.exceptions import InvalidFileTypeException


CHUNK_SIZE = 1024 * 1024 * 5  # 5MB
FILE_SIZE_LIMIT = 1024 * 1024 * 60  # 60MB


class PublishDatasourceRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests publishing datasources.
    :param class ts_connection: the Tableau Server connection object
    :param str datasource_name: the name for the datasource being published
    :param str datasource_file_path: the file path for the datasource being published
    :param str project_id: the project ID for the project the datasource belongs to
    :param str connection_username: if the datasource requires credentials, this value specifies the connection username
    :param str connection_password: if the datasource requires credentials, this value specifies the connection password
    :param bool embed_credentials_flag: True if credentials are to be embedded, False otherwise
    :param bool oauth_flag: True if the data connection username is an OAuth username, False otherwise
    """
    def __init__(self,
                 ts_connection,
                 datasource_name,
                 datasource_file_path,
                 project_id,
                 connection_username=None,
                 connection_password=None,
                 embed_credentials_flag=False,
                 oauth_flag=False
                 ):
        super().__init__(ts_connection)
        self._datasource_name = datasource_name
        self._datasource_file_path = datasource_file_path
        self._project_id = project_id
        self._connection_username = connection_username
        self._connection_password = connection_password
        self._embed_credentials_flag = embed_credentials_flag
        self._oauth_flag = oauth_flag
        self._file_is_chunked = self._file_requires_chunking()
        self.base_publish_datasource_request()

    @property
    def valid_file_extensions(self):
        return [
            'hyper',
            'tds',
            'tdsx',
            'tde'
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
    def optional_credentials_param_values(self):
        return [
            self._connection_username,
            self._connection_password,
            self._embed_credentials_flag,
            self._oauth_flag
            # 'true' if self._embed_credentials_flag is True
            # else 'false' if self._embed_credentials_flag is False
            # else None,
            # 'true' if self._oauth_flag is True
            # else 'false' if self._oauth_flag is False
            # else None
        ]

    def base_publish_datasource_request(self):
        self._request_body.update({
            'datasource': {
                'name': self._datasource_name,
                'project': {'id': self._project_id}
            }
        })
        return self._request_body

    def modified_publish_datasource_request(self):
        if any(self.optional_credentials_param_values):
            self._request_body['datasource'].update({'connectionCredentials': {}})
            self._request_body['datasource']['connectionCredentials'].update(
                self._get_parameters_dict(self.optional_credentials_param_keys,
                                          self.optional_credentials_param_values))
        return self._request_body

    def _file_requires_chunking(self):
        file_size = os.path.getsize(self._datasource_file_path)
        if file_size > FILE_SIZE_LIMIT:
            return True

    def get_datasource(self):
        datasource_file = os.path.basename(self._datasource_file_path)
        with open(self._datasource_file_path, 'rb') as f:
            datasource_bytes = f.read()
        file_extension = datasource_file.split('.')[-1]
        if file_extension in self.valid_file_extensions:
            pass
        else:
            raise InvalidFileTypeException(self.__class__.__name__,
                                           file_variety='datasource',
                                           file_extension=file_extension)
        return datasource_file, datasource_bytes

    # testing for chunk upload
    def publish_prep(self, publish_content_type, parameter_dict):
        parameter_dict = parameter_dict if parameter_dict else {'overwrite': 'overwrite=true'}
        filename = os.path.basename(self._datasource_file_path)
        file_extension = filename.split('.')[-1]

        if self._file_is_chunked:
            upload_session_id = self._connection.initiate_file_upload().json()['fileUpload']['uploadSessionId']
            parameter_dict.update({'param': 'uploadSessionId={}'.format(upload_session_id)})
            chunks = self.read_chunks(self._datasource_file_path)
            for chunk in chunks:
                request, append_content_type = self.chunk_req(chunk)
                file_upload = self._connection.append_to_file_upload(upload_session_id=upload_session_id,
                                                                     payload=request,
                                                                     content_type=append_content_type)

        publishing_headers = self._connection.default_headers.copy()
        publishing_headers.update({'content-type': publish_content_type})
        parameter_dict.update({'datasourceType': 'datasourceType={}'.format(file_extension)})
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
        request = self.modified_publish_datasource_request()
        parts = {'request_payload': (None, json.dumps(request), 'application/json')}
        return self._add_multipart(parts)

    def _publish_single_file_request(self):
        request = self.modified_publish_datasource_request()
        datasource_file, datasource_bytes = self.get_datasource()
        parts = {'request_payload': (None, json.dumps(request), 'application/json'),
                 'tableau_datasource': (datasource_file, datasource_bytes, 'application/octet-stream')}
        return self._add_multipart(parts)

    def get_request(self):
        if self._file_is_chunked:
            return self._publish_chunked_file_request()
        else:
            return self._publish_single_file_request()
