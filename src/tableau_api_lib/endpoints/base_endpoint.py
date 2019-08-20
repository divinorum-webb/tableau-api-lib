from tableau.client.exceptions import InvalidParameterException


class BaseEndpoint:
    """
    Base endpoint for sending API request URLs to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    """
    def __init__(self,
                 ts_connection):
        self._connection = ts_connection
        self._parameter_dict = {}
        
    @property
    def _params_exist(self):
        if self._parameter_dict:
            return list(self._parameter_dict.keys())
        else:
            return []
        
    @property
    def _params_text(self):
        if self._parameter_dict:
            return list(self._parameter_dict.values())
        else:
            return []

    @property
    def base_url(self):
        return self._connection.base_url
    
    def _append_url_parameters(self, url):
        text_to_append = "?" if any(self._params_exist) else ""
        for i, text in enumerate(self._params_text):
            if self._params_exist[i]:
                text_to_append += text if text_to_append.endswith('?') else ('&' + text)
        return "{0}{1}".format(url, text_to_append)
    
    def _invalid_parameter_exception(self):
        raise InvalidParameterException(self.__class__.__name__,
                                        vars(self))
