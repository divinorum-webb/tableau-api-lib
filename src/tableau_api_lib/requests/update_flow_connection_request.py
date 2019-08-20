from tableau.client.requests import BaseRequest


class UpdateFlowConnectionRequest(BaseRequest):
    """
    Update flow connection request for generating API request URLs to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param server_address:      The new server address for the connection.
    :type server_address:       string
    :param port:                The new port for the connection.
    :type port:                 string
    :param connection_username: The new username for the connection.
    :type connection_username:  string
    :param connection_password: The new password for the connection.
    :type connection_password:  string
    :param embed_password_flag: Boolean flag; True if embedding password in the flow connection, False otherwise.
    :type embed_password_flag:  boolean
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
        self.base_update_flow_connection_request()

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
        if self._embed_password_flag:
            if self._connection_username and self._connection_password:
                pass
            else:
                raise self._invalid_parameter_exception()
        else:
            self._connection_password = ''

    def base_update_flow_connection_request(self):
        self._request_body.update({'connection': {}})
        return self._request_body

    def modified_update_flow_connection_request(self):
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
        return self.modified_update_flow_connection_request()
