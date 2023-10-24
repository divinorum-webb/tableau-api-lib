from tableau_api_lib.api_endpoints import BaseEndpoint


class AuthEndpoint(BaseEndpoint):
    def __init__(
        self,
        ts_connection,
        sign_in=False,
        sign_out=False,
        switch_site=False,
        get_server_info=False,
        revoke_admin_pat=False,
        parameter_dict=None,
    ):
        """
        Builds the auth endpoint for Tableau Server REST API auth methods.
        :param class ts_connection: the Tableau Server connection object.
        :param bool sign_in: True if signing in, False otherwise.
        :param bool sign_out: True if signing out, False otherwise.
        :param bool switch_site: True if switching site, False otherwise.
        :param bool get_server_info: True if getting server info, False otherwise.
        :param bool revoke_admin_pat: True if revoking all administrator personal access tokens, False otherwise.
        :param dict parameter_dict: dictionary of URL parameters to append; the value in each key-value pair is the literal
        text that will be appended to the URL endpoint.
        """

        super().__init__(ts_connection)
        self._sign_in = sign_in
        self._sign_out = sign_out
        self._switch_site = switch_site
        self._get_server_info = get_server_info
        self._revoke_admin_pat = revoke_admin_pat
        self._parameter_dict = parameter_dict

    @property
    def base_auth_url(self) -> str:
        return "{0}/api/{1}/auth".format(self._connection.server, self._connection.api_version)

    @property
    def base_sign_in_url(self) -> str:
        return "{0}/signin".format(self.base_auth_url)

    @property
    def base_sign_out_url(self) -> str:
        return "{0}/signout".format(self.base_auth_url)

    @property
    def base_switch_site_url(self) -> str:
        return "{0}/switchSite".format(self.base_auth_url)

    @property
    def base_server_info_url(self) -> str:
        return "{0}/api/{1}/serverinfo".format(self._connection.server, self._connection.api_version)

    @property
    def base_revoke_admin_pat_url(self) -> str:
        return "{0}/serverAdminAccessTokens".format(self.base_auth_url)

    def get_endpoint(self):
        url = None
        if self._sign_in and not (self._sign_out or self._switch_site):
            url = self.base_sign_in_url
        elif self._sign_out and not (self._sign_in or self._switch_site):
            url = self.base_sign_out_url
        elif self._switch_site and not (self._sign_in or self._sign_out):
            url = self.base_switch_site_url
        elif self._get_server_info:
            url = self.base_server_info_url
        elif self._revoke_admin_pat:
            url = self.base_revoke_admin_pat_url

        if url:
            return self._append_url_parameters(url)
        else:
            self._invalid_parameter_exception()
