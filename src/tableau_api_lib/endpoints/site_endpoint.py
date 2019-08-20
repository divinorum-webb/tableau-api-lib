from tableau.client.endpoints import BaseEndpoint


class SiteEndpoint(BaseEndpoint):
    """
    Site endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param site_id:             The site ID.
    :type site_id:              string
    :param site_name:           The site name (only required if deleting a site by name).
    :type site_name:            string
    :param content_url:         The site name (only required if deleting a site by contentUrl).
    :type content_url:          string
    :param create_site:         Boolean flag; True if creating a site, False otherwise.
    :type create_site:          boolean
    :param update_site:         Boolean flag; True if updating a specific site, False otherwise.
    :type update_site:          boolean
    :param delete_site:         Boolean flag; True if deleting a specific site, False otherwise.
    :type delete_site:          boolean
    :param query_site:          Boolean flag; True if querying a specific site, False otherwise.
    :type query_site:           boolean
    :param query_sites:         Boolean flag; True if querying all sites on the site, False otherwise.
    :type query_sites:          boolean
    :param get_users:           Boolean flag; True if getting all users, False otherwise.
    :type get_users:            boolean
    :param get_groups:          Boolean flag; True if getting all groups, False otherwise.
    :type get_groups:           boolean
    :param add_user:            Boolean flag; True if adding a user, False otherwise.
    :type add_user:             boolean
    :param add_group:           Boolean flag; True if adding a group, False otherwise.
    :type add_group:            boolean
    :param remove_user:         Boolean flag; True if removing a specific user, False otherwise.
    :type remove_user:          boolean
    :param remove_group:        Boolean flag; True if removing a specific group, False otherwise.
    :type remove_group:         boolean
    :param user_id:             The user ID.
    :type user_id:              string
    :param group_id:            The group ID.
    :type group_id:             string
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self, 
                 ts_connection, 
                 site_id=None,
                 site_name=None,
                 content_url=None,
                 create_site=False,
                 update_site=False,
                 delete_site=False,
                 query_site=False,
                 query_sites=False,
                 query_views=False,
                 get_users=False,
                 get_groups=False,
                 add_user=False,
                 add_group=False,
                 remove_user=False,
                 remove_group=False,
                 user_id=None,
                 group_id=None,
                 parameter_dict=None):
        
        super().__init__(ts_connection)
        self._site_id = site_id
        self._site_name = site_name
        self._content_url = content_url
        self._create_site = create_site
        self._update_site = update_site
        self._delete_site = delete_site
        self._query_site = query_site
        self._query_sites = query_sites
        self._query_views = query_views
        self._get_users = get_users
        self._get_groups = get_groups
        self._add_user = add_user
        self._add_group = add_group
        self._remove_user = remove_user
        self._remove_group = remove_group
        self._user_id = user_id
        self._group_id = group_id
        self._parameter_dict = parameter_dict
        
    @property
    def base_site_url(self):
        return "{0}/api/{1}/sites".format(self._connection.server, 
                                          self._connection.api_version)
    
    @property
    def base_site_id_url(self):
        return "{0}/{1}".format(self.base_site_url, 
                                self._site_id)
    
    @property
    def base_site_views_url(self):
        return "{0}/views".format(self.base_site_id_url)
    
    @property
    def base_site_user_url(self):
        return "{0}/users".format(self.base_site_id_url)
    
    @property
    def base_site_user_id_url(self):
        return "{0}/{1}".format(self.base_site_user_url,
                                self._user_id)
    
    @property
    def base_site_group_url(self):
        return "{0}/groups".format(self.base_site_id_url)
    
    @property
    def base_site_group_id_url(self):
        return "{0}/{1}".format(self.base_site_group_url,
                                self._group_id)

    @property
    def base_delete_site_url(self):
        if self._site_id:
            return "{0}/{1}".format(self.base_site_url,
                                    self._site_id)
        elif self._site_name:
            return "{0}/{1}?key=name".format(self.base_site_url,
                                             self._site_name)
        elif self._content_url:
            return "{0}/{1}?key=contentUrl".format(self.base_site_url,
                                                   self._content_url)
    
    def get_endpoint(self):
        if not self._delete_site and (self._site_id or self._user_id or self._group_id):
            if self._update_site:
                url = self.base_site_id_url
            elif self._get_users and not self._add_user:
                url = self.base_site_user_url
            elif self._add_user and not self._get_users:
                url = self.base_site_user_url
            elif self._get_groups and not self._add_group:
                url = self.base_site_group_url
            elif self._add_group and not self._get_groups:
                url = self.base_site_group_url
            elif self._remove_user and self._user_id and self._site_id:
                url = self.base_site_user_id_url
            elif self._remove_group and self._group_id and self._site_id:
                url = self.base_site_group_id_url
            elif self._query_site:
                url = self.base_site_id_url
            elif self._query_views:
                url = self.base_site_views_url
            else:
                self._invalid_parameter_exception()
        elif self._delete_site and (self._site_id or self._site_name or self._content_url):
            url = self.base_delete_site_url
        else:
            url = self.base_site_url

        return self._append_url_parameters(url)
