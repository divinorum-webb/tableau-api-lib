from tableau.client.endpoints import BaseEndpoint


class PermissionsEndpoint(BaseEndpoint):
    """
    User endpoint for Tableau Server API requests.

    :param ts_connection:                       The Tableau Server connection object.
    :type ts_connection:                        class
    :param add_object_permissions:              Boolean flag; True if adding object permissions, False otherwise.
    :type add_object_permissions:               boolean
    :param query_object_permissions:            Boolean flag; True if querying object permissions, False otherwise.
    :type query_object_permissions:             boolean
    :param delete_object_permissions:           Boolean flag; True if deleting object permissions, False otherwise.
    :type delete_object_permissions:            boolean
    :param object_type:                         The Tableau object type (workbook, etc.).
    :type object_type:                          string
    :param object_id:                           The Tableau object ID.
    :type object_id:                            string
    :param query_default_project_permissions:   Boolean flag; True if querying default project permissions,
                                                False otherwise.
    :type query_default_project_permissions:    boolean
    :param add_default_project_permissions:     Boolean flag; True if adding default project permissions,
                                                False otherwise.
    :type add_default_project_permissions:      boolean
    :param delete_default_project_permissions:  Boolean flag; True if deleting default project permissions,
                                                False otherwise.
    :type delete_default_project_permissions:   boolean
    :param project_permissions_object:          The project permissions object (workbook, etc.).
    :type project_permissions_object:           string
    :param delete_permissions_object:           The permissions object to delete (user, group, etc.).
    :type delete_permissions_object:            string
    :param delete_permissions_object_id:        The ID of the permissions object to delete (user ID, group ID, etc.).
    :type delete_permissions_object_id:         string
    :param capability_name:                     The permissions capability being targeted (only use when deleting).
    :type capability_name:                      string
    :param capability_mode:                     Set this value to 'Allow' when you want to delete a permission allowing
                                                a capability; set this value to 'Deny' when you want to delete a
                                                permission denying a capability.
    :type capability_mode:                      string
    :param project_id:                          The project ID.
    :type project_id:                           string
    :param parameter_dict:                      Dictionary of URL parameters to append. The value in each key-value pair
                                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:                       dict
    """
    def __init__(self,
                 ts_connection,
                 add_object_permissions=False,
                 query_object_permissions=False,
                 delete_object_permissions=False,
                 object_type=None,
                 object_id=None,
                 query_default_project_permissions=False,
                 add_default_project_permissions=False,
                 delete_default_project_permissions=False,
                 project_permissions_object=None,
                 delete_permissions_object=None,
                 delete_permissions_object_id=None,
                 capability_name=None,
                 capability_mode=None,
                 project_id=None,
                 parameter_dict=None):

        super().__init__(ts_connection)
        self._add_object_permissions = add_object_permissions
        self._query_object_permissions = query_object_permissions
        self._delete_object_permissions = delete_object_permissions
        self._object_type = self._enforce_plurals(object_type,
                                                  self.valid_project_permissions_objects)
        self._object_id = object_id
        self._query_default_project_permissions = query_default_project_permissions
        self._add_default_project_permissions = add_default_project_permissions
        self._delete_default_project_permissions = delete_default_project_permissions
        self._project_permissions_object = self._enforce_plurals(project_permissions_object,
                                                                 self.valid_project_permissions_objects)
        self._delete_permissions_object = self._enforce_plurals(delete_permissions_object,
                                                                self.valid_project_delete_permissions_objects)
        self._delete_permissions_object_id = delete_permissions_object_id
        self._capability_name = capability_name
        self._capability_mode = capability_mode
        self._project_id = project_id
        self._parameter_dict = parameter_dict

    @property
    def valid_project_permissions_objects(self):
        return [
            'datasources',
            'datasource',
            'workbooks',
            'workbook',
            'flows',
            'flow',
            'views',
            'view',
            'projects',
            'project'
        ]

    @property
    def valid_project_delete_permissions_objects(self):
        return [
            'users',
            'user',
            'groups',
            'group'
        ]

    # def _validate_project_permissions_object(self):
    #     if self._project_permissions_object:
    #         if self._project_permissions_object in self.valid_project_permissions_objects:
    #             if self._project_permissions_object[-1] == 's':
    #                 pass
    #             else:
    #                 self._project_permissions_object += 's'
    #         else:
    #             self._invalid_parameter_exception()

    def _enforce_plurals(self, permissions_object, valid_permissions_objects):
        print('permissions_object: ', permissions_object)
        print('valid_permissions_objects: ', valid_permissions_objects)
        if permissions_object:
            if permissions_object in valid_permissions_objects:
                if permissions_object[-1] == 's':
                    return permissions_object
                else:
                    return permissions_object + 's'
            else:
                self._invalid_parameter_exception()


    @property
    def base_permissions_url(self):
        return "{0}/api/{1}/sites/{2}".format(self._connection.server,
                                              self._connection.api_version,
                                              self._connection.site_id)

    @property
    def base_object_permissions_url(self):
        return "{0}/{1}/{2}/permissions".format(self.base_permissions_url,
                                                self._object_type,
                                                self._object_id)

    @property
    def base_query_default_permissions_url(self):
        return "{0}/projects/{1}/default-permissions/{2}".format(self.base_permissions_url,
                                                                 self._project_id,
                                                                 self._project_permissions_object)

    @property
    def base_delete_permission_url(self):
        return "{0}/{1}/{2}/{3}/{4}".format(self.base_object_permissions_url,
                                            self._delete_permissions_object,
                                            self._delete_permissions_object_id,
                                            self._capability_name,
                                            self._capability_mode)

    @property
    def base_delete_default_permissions_url(self):
        return "{0}/{1}/{2}/{3}/{4}".format(self.base_query_default_permissions_url,
                                             self._delete_permissions_object,
                                             self._delete_permissions_object_id,
                                             self._capability_name,
                                             self._capability_mode)

    def get_endpoint(self):
        if self._add_object_permissions and not (self._query_object_permissions or self._delete_object_permissions):
            url = self.base_object_permissions_url
        elif self._query_object_permissions and not (self._add_object_permissions or self._delete_object_permissions):
            url = self.base_object_permissions_url
        elif self._delete_object_permissions and not (self._add_object_permissions or self._query_object_permissions):
            url = self.base_delete_permission_url
        elif self._delete_default_project_permissions and not self._delete_object_permissions:
            url = self.base_delete_default_permissions_url
        elif self._query_default_project_permissions:
            url = self.base_query_default_permissions_url
        elif self._add_default_project_permissions:
            url = self.base_query_default_permissions_url
        else:
            self._invalid_parameter_exception()

        return self._append_url_parameters(url)
