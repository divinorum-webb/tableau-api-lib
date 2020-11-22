from tableau_api_lib.exceptions import InvalidParameterException


class BaseRequest:
    """
    The base request from which all other API requests are built.
    :param class ts_connection: the Tableau Server connection object
    """

    def __init__(self, ts_connection):

        self._connection = ts_connection
        self._request_body = {}

    @staticmethod
    def _get_parameters_dict(param_keys, param_values):
        params_dict = {}
        for i, key in enumerate(param_keys):
            if param_values[i] or (param_values[i] is False):
                params_dict.update({key: param_values[i]})
        return params_dict

    def _invalid_parameter_exception(self):
        raise InvalidParameterException(
            class_name=self.__class__.__name__, parameters=vars(self)
        )
