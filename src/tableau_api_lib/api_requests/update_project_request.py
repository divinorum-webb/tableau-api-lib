from tableau_api_lib.api_requests import BaseRequest


class UpdateProjectRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating projects.
    :param class ts_connection: the Tableau Server connection object
    :param str project_name: (optional) the new name for the project
    :param str project_description: (optional) the new description for the project
    :param str content_permissions: (optional) the new permissions setting for the project; Specify LockedToProject to
    lock permissions so that users cannot overwrite the default permissions set for the project, or specify
    ManagedByOwner to allow users to manage permissions for content that they own; For more information, see Lock
    Content Permissions to the Project; The default value is ManagedByOwner
    :param str parent_project_id: (optional) the identifier of the parent project. Use this option to create project
    hierarchies;
    Note: To update a project without changing its placement in the project hierarchy, omit the parentProjectId
    attribute. To move a project to the top of the project hierarchy, provide an empty string ("") for the
    parentProjectId attribute
    """
    def __init__(self,
                 ts_connection,
                 project_name=None,
                 project_description=None,
                 content_permissions=None,
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
            'name',
            'parentProjectId',
            'description',
            'contentPermissions'
        ]

    @property
    def optional_project_param_values(self):
        return [
            self._project_name,
            self._parent_project_id,
            self._project_description,
            self._content_permissions
        ]

    def base_create_project_request(self):
        self._request_body.update({'project': {}})
        return self._request_body

    def modified_create_project_request(self):
        self._request_body['project'].update(
            self._get_parameters_dict(
                self.optional_project_param_keys,
                self.optional_project_param_values))
        if self._parent_project_id == "":
            self._request_body['project'].update({'parentProjectId': ''})
        return self._request_body

    def get_request(self):
        return self.modified_create_project_request()
