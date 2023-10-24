from tableau_api_lib.api_requests import BaseRequest


class UpdateDatasourceConnectionRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating datasource connections.
    :param class ts_connection: the Tableau Server connection object
    :param str server_address: the new server address for the connection
    :param str port: the new port for the connection
    :param str connection_username: the new username for the connection
    :param str connection_password: the new password for the connection
    :param bool embed_password_flag: True if embedding password in the datasource connection, False otherwise
    """
    def __init__(self,
                 ts_connection,
                 server_address=None,
                 port=None,
                 connection_username=None,
                 connection_password=None,
                 embed_password_flag=None):

        super().__init__(ts_connection)
        self._server_address = server_address
        self._port = port
        self._connection_username = connection_username
        self._connection_password = connection_password
        self._embed_password_flag = embed_password_flag
        self._validate_inputs()
        self.base_update_datasource_connection_request()

    @property
    def optional_parameter_keys(self):
        return [
            'serverAddress',
            'serverPort',
            'userName',
            'password',
            'embedPassword'
        ]

    @property
    def optional_parameter_values_exist(self):
        return [
            self._server_address,
            self._port,
            self._connection_username,
            self._connection_password,
            True if self._embed_password_flag is not None else None
        ]

    @property
    def optional_parameter_values(self):
        return [
            self._server_address,
            self._port,
            self._connection_username,
            self._connection_password,
            self._embed_password_flag
        ]

    def _validate_inputs(self):
        valid = True
        if self._embed_password_flag:
            if not(self._connection_username and self._connection_password):
                valid = False
        else:
            self._connection_password = ''
        if not valid:
            self._invalid_parameter_exception()

    def base_update_datasource_connection_request(self):
        self._request_body.update({'connection': {}})
        return self._request_body

    def modified_update_datasource_connection_request(self):
        if any(self.optional_parameter_values_exist):
            self._request_body['connection'].update(
                self._get_parameters_dict(self.optional_parameter_keys,
                                          self.optional_parameter_values))
        return self._request_body

    @staticmethod
    def _get_parameters_dict(param_keys, param_values):
        """Override the inherited _get_parameters_dict() method to allow passing boolean values directly"""
        params_dict = {}
        for i, key in enumerate(param_keys):
            if param_values[i] is not None:
                params_dict.update({key: param_values[i]})
        return params_dict

    def get_request(self):
        return self.modified_update_datasource_connection_request()
