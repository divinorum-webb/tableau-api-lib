from tableau.client.requests import BaseRequest


class UpdateUserRequest(BaseRequest):
    """
    Update user request for sending API requests to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param new_full_name:       (Optional) The new name for the user. Users can change names without affecting the
                                groups they belong to. Tableau Server only.
                                Not available in Tableau Online.
    :type new_full_name:        string
    :param new_email:           (Optional) The new email address for the user. Tableau Server only.
                                Not available in Tableau Online.
    :type new_email:            string
    :param new_password:        (Optional) The new password for the user. Tableau Server only.
                                Not available in Tableau Online.
    :type new_password:         string
    :param new_site_role:       (Optional) The new site role. Valid role names are ServerAdministratorExplorer,
                                ExplorerCanPublish, SiteAdministrator, and Unlicensed.
                                See the Tableau Server REST API documentation for further details.
    :type new_site_role:        string
    :param new_auth_setting:    (Optional) The new authentication type for the user. You can assign the following
                                values for this attribute: SAML (the user signs in using SAML) or ServerDefault
                                (the user signs in using the authentication method that's set for the server).
                                See the Tableau Server REST API documentation for further details.
    :type new_auth_setting:     string
    """
    def __init__(self,
                 ts_connection,
                 new_full_name=None,
                 new_email=None,
                 new_password=None,
                 new_site_role=None,
                 new_auth_setting=None):

        super().__init__(ts_connection)
        self._new_full_name = new_full_name
        self._new_email = new_email
        self._new_password = new_password
        self._new_site_role = new_site_role
        self._new_auth_setting = new_auth_setting
        self.base_update_user_request()

    @property
    def optional_user_param_keys(self):
        return [
            'fullName',
            'email',
            'password',
            'siteRole',
            'authSetting'
        ]

    @property
    def optional_user_param_values(self):
        return [
            self._new_full_name,
            self._new_email,
            self._new_password,
            self._new_site_role,
            self._new_auth_setting
        ]

    def base_update_user_request(self):
        self._request_body.update({'user': {}})
        return self._request_body

    def modified_update_user_request(self):
        if any(self.optional_user_param_values):
            self._request_body['user'].update(
                self._get_parameters_dict(self.optional_user_param_keys,
                                          self.optional_user_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_update_user_request()
