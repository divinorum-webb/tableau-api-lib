from functools import wraps


def verify_signed_in(func):
    """
    Verifies that the Tableau Server object is signed in.
    This decorates the function calls which send API api_requests to the server.

    :param func:    The code that will be passed with the server's response body if the request was successful.
    :type func:     function
    :return:        Returns the results of the function this decorates.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if self.auth_token:
            return func(self, *args, **kwargs)
        else:
            raise Exception('The Tableau Server connection is not logged in.')
    return wrapper
