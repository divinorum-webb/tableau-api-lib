from tableau.client.exceptions import InvalidParameterException


class BaseRequest:
    """
    Base request for issuing API requests to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    """
    def __init__(self,
                 ts_connection):

        self._connection = ts_connection
        self._request_body = {}

    @staticmethod
    def _get_parameters_dict(param_keys, param_values):
        params_dict = {}
        for i, key in enumerate(param_keys):
            if param_values[i]:
                params_dict.update({key: param_values[i]})
        return params_dict

    def _invalid_parameter_exception(self):
        raise InvalidParameterException(class_name=self.__class__.__name__,
                                        parameters=vars(self))
