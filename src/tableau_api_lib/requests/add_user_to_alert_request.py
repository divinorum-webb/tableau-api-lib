from tableau.client.requests import BaseRequest


class AddUserToAlertRequest(BaseRequest):
    """
    Add user to alert request for generating API requests to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param user_id:             The user ID for the user being added to the alert.
    :type user_id:              string
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
