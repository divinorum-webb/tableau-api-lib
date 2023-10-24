from tableau_api_lib.api_requests import BaseRequest


class CreateProjectRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests creating projects.
    :param class ts_connection: the Tableau Server connection object
    :param str project_name: the name of the project being created
    :param str project_description: the description of the project being created
    :param str parent_project_id: the project ID for the parent project, if creating a child project
    """
    def __init__(self,
                 ts_connection,
                 project_name,
                 project_description=None,
                 content_permissions='ManagedByOwner',
                 parent_project_id=None):
        super().__init__(ts_connection)
        self._project_name = project_name
        self._project_description = project_description
        self._content_permissions = content_permissions
        self._parent_project_id = parent_project_id
        self.base_create_project_request()

    @property
    def optional_project_param_keys(self):
        return [
            'parentProjectId',
            'description',
            'contentPermissions'
        ]

    @property
    def optional_project_param_values(self):
        return [
            self._parent_project_id,
            self._project_description,
            self._content_permissions
        ]

    def base_create_project_request(self):
        self._request_body.update({'project': {'name': self._project_name}})
        return self._request_body

    def modified_create_project_request(self):
        self._request_body['project'].update(
            self._get_parameters_dict(
                self.optional_project_param_keys,
                self.optional_project_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_create_project_request()
