import numpy as np
from tableau_api_lib.api_requests import BaseRequest


class UpdateUserRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating databases.
    :param class ts_connection: the Tableau Server connection object
    :param str new_full_name: (optional) the new name for the user. Users can change names without affecting the groups
    they belong to. Tableau Server only; not available in Tableau Online
    :param str new_email: (optional) the new email address for the user; Tableau Server only; not available in Tableau
    Online
    :param str new_password: (optional) the new password for the user; Tableau Server only; not available in Tableau
    Online
    :param str new_site_role: (optional) the new site role. Valid role names are
    [ServerAdministratorExplorer, ExplorerCanPublish, SiteAdministrator, or Unlicensed]; see the Tableau Server REST API
    documentation for further details.
    :param str new_auth_setting: (optional) the new authentication type for the user. You can assign the following
    values for this attribute: SAML (the user signs in using SAML) or ServerDefault (the user signs in using the
    authentication method that's set for the server); see the Tableau Server REST API documentation for further details.
    """
    def __init__(self,
                 ts_connection,
                 new_full_name=None,
                 new_email=None,
                 new_password=None,
                 new_site_role=None,
                 new_auth_setting=None):

        super().__init__(ts_connection)
        self._new_full_name = new_full_name if isinstance(new_full_name, str) else None
        self._new_email = new_email if isinstance(new_email, str) else None
        self._new_password = new_password
        self._new_site_role = new_site_role
        self._new_auth_setting = new_auth_setting if isinstance(new_auth_setting, str) else None
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
