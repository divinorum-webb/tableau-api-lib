from tableau_api_lib.api_requests import BaseRequest


class UpdateDQWarningRequest(BaseRequest):
    def __init__(self,
                 ts_connection,
                 warning_type,
                 message,
                 status=None):
        """
        Builds the request body for Tableau Server REST API requests updating data quality warnings.
        :param class ts_connection: the Tableau Server connection object
        :param string warning_type: the warning type [Deprecated, Warning, Stale data, Under maintenance]
        :param string message: a custom message accompanying the data warning
        :param boolean status: (optional) toggles the warning on or off [True for on, False for off]
        """

        super().__init__(ts_connection)
        self._warning_type = warning_type
        self._message = message
        self._status = status
        self._request_body = {'dataQualityWarning': {}}
        self._validate_inputs()

    @property
    def valid_warning_types(self):
        return[
            'Deprecated',
            'Warning',
            'Stale data',
            'Under maintenance'
        ]

    def _validate_inputs(self):
        valid = True
        if self._warning_type not in self.valid_warning_types:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def optional_param_keys(self):
        return [
            'type',
            'message',
            'isActive'
        ]

    @property
    def optional_param_values(self):
        return [
            self._warning_type,
            self._message,
            self._status
        ]

    def base_update_column_request(self):
        self._request_body['dataQualityWarning'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                                  self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.base_update_column_request()
