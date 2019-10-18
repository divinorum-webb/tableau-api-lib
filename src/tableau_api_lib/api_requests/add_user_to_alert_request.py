from tableau_api_lib.api_requests import BaseRequest


class AddUserToAlertRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding users to data alerts.
    :param class ts_connection: the Tableau Server connection object
    :param str user_id: the user ID for the user being added to the alert
    """
    def __init__(self,
                 ts_connection,
                 user_id):
        super().__init__(ts_connection)
        self._user_id = user_id

    def base_add_user_request(self):
        self._request_body.update({'user': {'id': self._user_id}})
        return self._request_body

    def get_request(self):
        return self.base_add_user_request()
