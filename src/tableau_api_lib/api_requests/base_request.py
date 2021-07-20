from typing import Any, Dict, List

from tableau_api_lib.exceptions import InvalidParameterException
from tableau_api_lib.tableau_server_connection import TableauServerConnection


class BaseRequest:
    """The base request from which all other API requests are built."""

    def __init__(self, ts_connection: TableauServerConnection):

        self._connection = ts_connection
        self._request_body = {}

    @staticmethod
    def _get_parameters_dict(param_keys: List[str], param_values: List[Any]) -> Dict[str, Any]:
        """Returns zipped keys and values into a dict, but only for pairs where the param value is truthy."""
        params_dict = {}
        for i, key in enumerate(param_keys):
            if param_values[i] or (param_values[i] is False):
                params_dict.update({key: param_values[i]})
        return params_dict

    def _invalid_parameter_exception(self) -> None:
        raise InvalidParameterException(class_name=self.__class__.__name__, parameters=vars(self))
