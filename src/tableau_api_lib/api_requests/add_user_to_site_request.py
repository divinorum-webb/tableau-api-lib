from tableau_api_lib.api_requests import BaseRequest


class AddUserToSiteRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding users to sites.
    :param class ts_connection: the Tableau Server connection object
    :param str user_name: the username for the user being added
    :param str site_role: the site role to assign to the added user
    :param str auth_setting: the auth setting to assign to the added user
    """
    def __init__(self,
                 ts_connection,
                 user_name,
                 site_role='Viewer',
                 auth_setting=None):
        super().__init__(ts_connection)
        self._user_name = user_name if isinstance(user_name, str) else None
        self._site_role = site_role
        self._auth_setting = auth_setting if isinstance(auth_setting, str) else None
        self._validate_site_role()
        self.base_add_user_request()

    @property
    def required_user_param_keys(self):
        return [
            'name',
            'siteRole'
        ]

    @property
    def optional_user_param_keys(self):
        return ['authSetting']

    @property
    def required_user_param_values(self):
        return [
            self._user_name,
            self._site_role
        ]

    @property
    def optional_user_param_values(self):
        return [self._auth_setting]

    @property
    def valid_site_roles(self):
        return [
            'Creator',
            'creator',
            'Explorer',
            'explorer',
            'ExplorerCanPublish',
            'explorercanpublish',
            'siteAdministratorExplorer',
            'siteadministratorexplorer',
            'SiteAdministratorCreator',
            'siteadministratorcreator',
            'ServerAdministrator',
            'serverAdministrator',
            'Unlicensed',
            'unlicensed',
            'Viewer',
            'viewer'
        ]

    def _validate_site_role(self):
        valid = True
        if not(self._site_role in self.valid_site_roles):
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    def base_add_user_request(self):
        self._request_body.update({'user': {}})
        self._request_body['user'].update(
            self._get_parameters_dict(self.required_user_param_keys,
                                      self.required_user_param_values))
        return self._request_body

    def modified_add_user_request(self):
        if any(self.optional_user_param_values):
            self._request_body['user'].update(
                self._get_parameters_dict(self.optional_user_param_keys,
                                          self.optional_user_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_add_user_request()
