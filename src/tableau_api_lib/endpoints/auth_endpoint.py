from tableau.client.endpoints import BaseEndpoint


class AuthEndpoint(BaseEndpoint):
    """
    Authorization endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param sign_in:             Boolean flag; True if signing in, False otherwise.
    :type sign_in:              boolean
    :param sign_out:            Boolean flag; True if signing out, False otherwise.
    :type sign_out:             boolean
    :param switch_site:         Boolean flag; True if switching site, False otherwise.
    :type switch_site:          boolean
    :param get_server_info:     Boolean flag; True if getting server info, False otherwise.
    :type get_server_info:      boolean
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self,
                 ts_connection,
                 sign_in=False,
                 sign_out=False,
                 switch_site=False,
                 get_server_info=False,
                 parameter_dict=None):
        
        super().__init__(ts_connection)
        self._sign_in = sign_in
        self._sign_out = sign_out
        self._switch_site = switch_site
        self._get_server_info = get_server_info
        self._parameter_dict = parameter_dict
        
    @property
    def base_auth_url(self):
        return "{0}/api/{1}/auth".format(self._connection.server, 
                                         self._connection.api_version)
    
    @property
    def base_sign_in_url(self):
        return "{0}/signin".format(self.base_auth_url)
    
    @property
    def base_sign_out_url(self):
        return "{0}/signout".format(self.base_auth_url)
    
    @property
    def base_switch_site_url(self):
        return "{0}/switchSite".format(self.base_auth_url)
    
    @property
    def base_server_info_url(self):
        return "{0}/api/{1}/serverinfo".format(self._connection.server,
                                               self._connection.api_version)
    
    def get_endpoint(self):
        if self._sign_in and not (self._sign_out or self._switch_site):
            url = self.base_sign_in_url
        elif self._sign_out and not (self._sign_in or self._switch_site):
            url = self.base_sign_out_url
        elif self._switch_site and not (self._sign_in or self._sign_out):
            url = self.base_switch_site_url
        elif self._get_server_info:
            url = self.base_server_info_url
        
        if url:
            return self._append_url_parameters(url)
        else:
            self._invalid_parameter_exception()   
