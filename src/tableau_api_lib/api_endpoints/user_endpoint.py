from tableau_api_lib.api_endpoints import BaseEndpoint


class UserEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 add_user=False,
                 query_user=False,
                 query_users=False,
                 query_workbooks_for_user=False,
                 query_groups_for_user=False,
                 user_id=None,
                 update_user=False,
                 remove_user=False,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API user methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool add_user: True if adding a user, False otherwise
        :param bool query_user: True if querying a specific user, False otherwise
        :param bool query_users: True if querying all users on the site, False otherwise
        :param bool query_workbooks_for_user: True if querying a specific user, False otherwise
        :param bool query_groups_for_user: True if querying groups for a specific user, False otherwise
        :param str user_id: the user ID
        :param bool update_user: True if updating a specific user, False otherwise
        :param bool remove_user: True if removing a specific user from the site, False otherwise
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._add_user = add_user
        self._query_user = query_user
        self._query_users = query_users
        self._query_workbooks_for_user = query_workbooks_for_user
        self._query_groups_for_user = query_groups_for_user
        self._user_id = user_id
        self._update_user = update_user
        self._remove_user = remove_user
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._add_user,
            self._query_user,
            self._query_users,
            self._query_workbooks_for_user,
            self._query_groups_for_user,
            self._update_user,
            self._remove_user
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_user_url(self):
        return "{0}/api/{1}/sites/{2}/users".format(self._connection.server,
                                                    self._connection.api_version,
                                                    self._connection.site_id)

    @property
    def base_user_id_url(self):
        return "{0}/{1}".format(self.base_user_url,
                                self._user_id)

    @property
    def base_user_groups_url(self):
        return "{0}/groups".format(self.base_user_id_url)

    @property
    def base_user_workbooks_url(self):
        return "{0}/workbooks".format(self.base_user_id_url)

    def get_endpoint(self):
        url = None
        if self._user_id:
            if self._query_user:
                url = self.base_user_id_url
            elif self._update_user:
                url = self.base_user_id_url
            elif self._remove_user:
                url = self.base_user_id_url
            elif self._query_workbooks_for_user and self._user_id:
                url = self.base_user_workbooks_url
            elif self._query_groups_for_user:
                url = self.base_user_groups_url
            else:
                self._invalid_parameter_exception()
        else:
            url = self.base_user_url

        return self._append_url_parameters(url)
