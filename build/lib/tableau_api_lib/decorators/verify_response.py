from functools import wraps


def verify_response(success_code):
    """
    Verifies that the request being executed has a response whose status code matches the success code provided.
    This decorates the function calls which send API api_requests to the server.

    :param success_code:    The code that will be passed with the server's response body if the request was successful.
    :type success_code:     int
    :return:                Returns the results of the function this decorates.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            response = requests.get(self.request_url)
            if response.status_code != success_code:
                raise Exception('The request to Tableau Server returned code \n'
                                ' {} instead of {} in function {}'.format(response.status_code,
                                                                          success_code,
                                                                          func.__name__))
            return func(self, *args, **kwargs)
        return wrapper
    return decorator
