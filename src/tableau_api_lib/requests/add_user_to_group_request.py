from tableau.client.requests import BaseRequest


class AddUserToGroupRequest(BaseRequest):
    """
    Add user to group request for generating API requests to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param user_id:             The user ID.
    :type user_id:              string
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
