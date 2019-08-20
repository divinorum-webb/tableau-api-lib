from tableau.client.requests import BaseRequest


class SignInRequest(BaseRequest):
    """
    Sign in request for generating API request URLs to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param username:            The username credentials for signing in to Tableau Server.
    :type username:             string
    :param password:            The password credentials for signing in to Tableau Server.
    :type password:             string
    :param user_to_impersonate: If impersonating another user, set this value with their user ID.
    :type user_to_impersonate:  string
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
