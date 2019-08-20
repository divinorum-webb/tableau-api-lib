from tableau.client.requests import BaseRequest


class UpdateGroupRequest(BaseRequest):
    """
    Update group request for API requests to Tableau Server.

    :param ts_connection:                   The Tableau Server connection object.
    :type ts_connection:                    class
    :param new_group_name:                  The new name for the group.
    :type new_group_name:                   string
    :param active_directory_group_name:     The name of the Active Directory group to synchronize with.
    :type active_directory_group_name:      string
    :param active_directory_domain_name:    The domain for the Active Directory group.
    :type active_directory_domain_name:     string
    :param default_site_role:               The site role to assign to users that are synchronized with
                                            Active Directory. You can assign the following roles:
                                            Creator, Explorer, ExplorerCanPublish, SiteAdministratorExplorer,
                                            SiteAdministratorCreator, Unlicensed, or Viewer.
    :type default_site_role:                string
    """
    def __init__(self,
                 ts_connection,
                 new_group_name,
                 active_directory_group_name=None,
                 active_directory_domain_name=None,
                 default_site_role=None):
        super().__init__(ts_connection)
        self._new_group_name = new_group_name
        self._active_directory_group_name = active_directory_group_name
        self._active_directory_domain_name = active_directory_domain_name
        self._default_site_role = default_site_role
        self.base_update_group_request()

    @property
    def required_group_param_keys(self):
        return ['name']

    @property
    def optional_import_param_keys(self):
        return [
            'source',
            'domainName',
            'siteRole'
        ]

    @property
    def required_group_param_values(self):
        return [self._new_group_name]

    @property
    def optional_import_param_values(self):
        return [
            self._active_directory_group_name,
            self._active_directory_domain_name,
            self._default_site_role
        ]

    def base_update_group_request(self):
        self._request_body.update({'group': {}})
        self._request_body['group'].update(
            self._get_parameters_dict(self.required_group_param_keys,
                                      self.required_group_param_values))
        return self._request_body

    def modified_update_group_request(self):
        if any(self.optional_import_param_values):
            self._request_body['group'].update({'import': {}})
            self._request_body['group']['import'].update(
                self._get_parameters_dict(self.optional_import_param_keys,
                                          self.optional_import_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_update_group_request()
