import requests
import warnings
from functools import wraps

from tableau_api_lib.exceptions import InvalidRestApiVersion


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


def verify_connection(func):
    """
    Verifies that the Tableau Server object is signed in.
    This decorates the function calls which send API api_requests to the server.

    :param func:    The code that will be passed with the server's response body if the request was successful.
    :type func:     function
    :return:        Returns the results of the function this decorates.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        print(args, kwargs)
        if self.auth_token:
            print(self.auth_token)
            return func(self, *args, **kwargs)
        else:
            raise Exception('The Tableau Server connection is not logged in.')
    return wrapper


def verify_config_variables(func):
    """
    Verifies that the required config variables exist.
    This decorates the function calls which send API api_requests to the server.

    :param func:    The code that will be passed with the server's response body if the request was successful.
    :type func:     function
    :return:        Returns the results of the function this decorates.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        required_config_variables = [
            'server',
            'api_version',
            'site_name',
            'site_url'
        ]
        if (self._config and self._env) and type(self._config) == dict:
            config_variables = self._config[self._env].keys()
            missing_required_variables = [variable for variable in required_config_variables
                                          if variable not in config_variables]
            if not any(missing_required_variables):
                return func(self, *args, **kwargs)
            else:
                raise ValueError("""
                The configuration variables provided are invalid.
                Please provide the following missing configuration variables: {}
                """.format(missing_required_variables))
        raise Exception('Please provide a configuration dict to establish a connection.')
    return wrapper


def verify_rest_api_version(func):
    """
    Verifies that the Tableau Server object is signed in.
    This decorates the function calls which send API api_requests to the server.

    :param func:    The code that will be passed with the server's response body if the request was successful.
    :type func:     function
    :return:        Returns the results of the function this decorates.
    """
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if (self._config and self._env) and type(self._config) == dict:
            try:
                server_info = self.server_info().json()
                server_api_version = server_info['serverInfo']['restApiVersion']
                if self._config[self._env]['api_version'] > server_api_version:
                    raise Exception("""Your API version is too damn high!""")
                if self._config[self._env]['api_version'] < server_api_version:
                    warnings.warn("""
                    WARNING:
                    The Tableau Server REST API version you specified is lower than the version your server uses.
                    Your Tableau Server is on REST API version {0}.
                    The REST API version you specified is {1}.
                    For optimal results, please change the 'api_version' config variable to {0}.
                    """.format(server_api_version, self._config[self._env]['api_version']))
            except AttributeError:
                warnings.warn("""
                Warning: could not verify your Tableau Server's API version.
                If using a legacy version of Tableau Server, be sure to reference the legacy Tableau Server
                REST API documentation provided by Tableau.
                Some current API methods may exist that are not available on your legacy Tableau Server.
                """)
            return func(self, *args, **kwargs)
        else:
            raise Exception('The Tableau Server connection is not logged in.')
    return wrapper


def verify_api_method_exists(version_introduced):
    """
    Verifies that the connection's REST API version is equal to or greater than the version the method was introduced.

    :param str version_introduced: the REST API version when the method was introduced
    :return: decorated function
    """
    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            conn_api_version = self._config[self._env]['api_version']
            if conn_api_version < version_introduced:
                raise InvalidRestApiVersion(func,
                                            api_version_used=conn_api_version,
                                            api_version_required=version_introduced)
            return func(self, *args, **kwargs)
        return wrapper
    return decorator
