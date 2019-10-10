from tableau_api_lib.api_requests import BaseRequest


class UpdateColumnRequest(BaseRequest):
    def __init__(self,
                 ts_connection,
                 new_description_value=None):

        super().__init__(ts_connection)
        self._new_description_value = new_description_value
        self._request_body = {'column': {}}

    @property
    def optional_param_keys(self):
        return ['description']

    @property
    def optional_param_values(self):
        return [self._new_description_value]

    def base_update_column_request(self):
        self._request_body['column'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                      self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.base_update_column_request()
