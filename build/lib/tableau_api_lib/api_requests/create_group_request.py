from tableau_api_lib.api_requests import BaseRequest


class CreateGroupRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests creating groups.
    :param class ts_connection: the Tableau Server connection object
    :param str new_group_name: the name of the group being created
    :param str active_directory_group_name: the active directory group name
    :param str active_directory_domain_name: the active directory domain name
    :param str default_site_role: the default site role for the created group
    """
    def __init__(self,
                 ts_connection,
                 new_group_name=None,
                 active_directory_group_name=None,
                 active_directory_domain_name=None,
                 default_site_role=None):

        super().__init__(ts_connection)
        self._new_group_name = new_group_name
        self._active_directory_group_name = active_directory_group_name
        self._active_directory_domain_name = active_directory_domain_name
        self._default_site_role = default_site_role
        self.base_create_group_request()

    @property
    def required_group_param_keys(self):
        return ['name']

    @property
    def optional_source_param_keys(self):
        return [
            'source',
            'domainName',
            'siteRole'
        ]

    @property
    def required_group_param_values(self):
        return [self._new_group_name]

    @property
    def optional_source_param_values(self):
        return [
            self._active_directory_group_name,
            self._active_directory_domain_name,
            self._default_site_role
        ]

    def base_create_group_request(self):
        if all(self.required_group_param_values):
            self._request_body.update({'group': {}})
            self._request_body['group'].update(
                self._get_parameters_dict(self.required_group_param_keys,
                                          self.required_group_param_values))
        else:
            self._invalid_parameter_exception()
        return self._request_body

    def modified_create_group_request(self):
        if any(self.optional_source_param_values):
            self._request_body['group'].update({'import': {}})
            self._request_body['group']['import'].update(
                self._get_parameters_dict(self.optional_source_param_keys,
                                          self.optional_source_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_create_group_request()
