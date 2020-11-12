from tableau_api_lib.api_requests import BaseRequest


class AddUserToGroupRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding users to groups.
    :param class ts_connection: the Tableau Server connection object
    :param str user_id: the user ID
    """
    def __init__(self,
                 ts_connection,
                 user_id):
        super().__init__(ts_connection)
        self._user_id = user_id
        self.base_add_user_request()

    @property
    def required_user_param_keys(self):
        return ['id']

    @property
    def required_user_param_values(self):
        return [self._user_id]

    def base_add_user_request(self):
        self._request_body.update({'user': {}})
        return self._request_body

    def modified_add_user_request(self):
        self._request_body['user'].update(
            self._get_parameters_dict(self.required_user_param_keys,
                                      self.required_user_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_add_user_request()
