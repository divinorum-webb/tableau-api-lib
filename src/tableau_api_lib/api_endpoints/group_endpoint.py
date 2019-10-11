from tableau_api_lib.api_endpoints import BaseEndpoint


class GroupEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 query_groups=False,
                 group_id=None,
                 update_group=False,
                 delete_group=False,
                 create_group=False,
                 get_users=False,
                 add_user=False,
                 remove_user=False,
                 user_id=None,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API group methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool query_groups: True if querying all groups on a specific site, False otherwise
        :param str group_id: the group ID
        :param bool update_group: True if updating a specific group's information, False otherwise
        :param bool delete_group: True if deleting a specific group, False otherwise
        :param bool get_users: True if querying all users in a specific group, False otherwise
        :param bool add_user: True if adding a user to a specific group, False otherwise
        :param bool remove_user: True if removing a user from a specific group, False otherwise
        :param str user_id: the user ID
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._query_groups = query_groups
        self._group_id = group_id
        self._update_group = update_group
        self._delete_group = delete_group
        self._create_group = create_group
        self._get_users = get_users
        self._add_user = add_user
        self._remove_user = remove_user
        self._user_id = user_id
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_groups,
            self._update_group,
            self._delete_group,
            self._create_group,
            self._get_users,
            self._add_user,
            self._remove_user
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_group_url(self):
        return "{0}/api/{1}/sites/{2}/groups".format(self._connection.server,
                                                     self._connection.api_version,
                                                     self._connection.site_id)

    @property
    def base_group_id_url(self):
        return "{0}/{1}".format(self.base_group_url, self._group_id)

    @property
    def base_group_user_url(self):
        return "{0}/users".format(self.base_group_id_url)

    @property
    def base_group_user_id_url(self):
        return "{0}/{1}".format(self.base_group_user_url,
                                self._user_id)

    def get_endpoint(self):
        url = None
        if self._group_id:
            if self._update_group:
                url = self.base_group_id_url
            elif self._delete_group:
                url = self.base_group_id_url
            elif self._get_users or self._add_user:
                url = self.base_group_user_url
            elif self._remove_user and self._user_id:
                url = self.base_group_user_id_url
            else:
                self._invalid_parameter_exception()
        else:
            url = self.base_group_url

        return self._append_url_parameters(url)
