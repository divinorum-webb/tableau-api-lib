from tableau_api_lib.api_requests import BaseRequest


class SignInRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests when signing in.
    :param class ts_connection: the Tableau Server connection object
    :param str username: the username credentials for signing in to Tableau Server
    :param str password: the password credentials for signing in to Tableau Server
    :param str user_to_impersonate: if impersonating another user, set this value with their user ID
    """
    def __init__(self,
                 ts_connection,
                 username,
                 password,
                 user_to_impersonate=None):

        super().__init__(ts_connection)
        self._username = username
        self._password = password
        self._user_to_impersonate = user_to_impersonate
        self.base_signin_request()

    def base_signin_request(self):
        self._request_body.update({
            "credentials": {
                "name": self._username,
                "password": self._password,
                "site": {"contentUrl": self._connection.site_url}
            }
        })
        return self._request_body

    def modified_signin_request(self):
        self._request_body.update({
            "user": {
                "id": self._user_to_impersonate
            }
        })
        return self._request_body

    def get_request(self):
        if self._username and self._password and not self._user_to_impersonate:
            return self._request_body
        elif self._username and self._password and self._user_to_impersonate:
            return self.modified_signin_request()
        else:
            self._invalid_parameter_exception()
