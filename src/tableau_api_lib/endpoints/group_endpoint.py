from tableau.client.endpoints import BaseEndpoint


class GroupEndpoint(BaseEndpoint):
    """
    Group endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param query_groups:        Boolean flag; True if querying all groups on a specific site, False otherwise.
    :type query_groups:         boolean
    :param group_id:            The group ID.
    :type group_id:             string
    :param update_group:        Boolean flag; True if updating a specific group's information, False otherwise.
    :type update_group:         boolean
    :param delete_group:        Boolean flag; True if deleting a specific group, False otherwise.
    :type delete_group:         boolean
    :param get_users:           Boolean flag; True if querying all users in a specific group, False otherwise.
    :type get_users:            boolean
    :param add_user:            Boolean flag; True if adding a user to a specific group, False otherwise.
    :type add_user:             boolean
    :param remove_user:         Boolean flag; True if removing a user from a specific group, False otherwise.
    :type remove_user:          boolean
    :param user_id:             The user ID.
    :type user_id:              string
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
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
