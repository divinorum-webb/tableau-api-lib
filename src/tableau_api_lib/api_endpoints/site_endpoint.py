from tableau_api_lib.api_endpoints import BaseEndpoint


class SiteEndpoint(BaseEndpoint):
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
                 get_recently_viewed=False,
                 get_recommendations=False,
                 add_user=False,
                 add_group=False,
                 remove_user=False,
                 remove_group=False,
                 user_id=None,
                 group_id=None,
                 include_usage_flag=False,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API site methods.
        :param class ts_connection: the Tableau Server connection object
        :param str site_id: the site ID
        :param str site_name: the site name (only required if deleting a site by name)
        :param str content_url: the site name (only required if deleting a site by contentUrl)
        :param bool create_site: True if creating a site, False otherwise
        :param bool update_site: True if updating a specific site, False otherwise
        :param bool delete_site: True if deleting a specific site, False otherwise
        :param bool query_site: True if querying a specific site, False otherwise
        :param bool query_sites: True if querying all sites on the site, False otherwise
        :param bool get_users: True if getting all users, False otherwise
        :param bool get_groups: True if getting all groups, False otherwise
        :param bool get_recently_viewed: True if getting recently viewed content, False otherwise
        :param bool get_recommendations: True if getting recommendations, False otherwise
        :param bool add_user: True if adding a user, False otherwise
        :param bool add_group: True if adding a group, False otherwise
        :param bool remove_user: True if removing a specific user, False otherwise
        :param bool remove_group: True if removing a specific group, False otherwise
        :param str user_id: the user ID
        :param str group_id: the group ID
        :param bool include_usage_flag: True if including usage metrics, False otherwise
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """
        
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
        self._get_recently_viewed = get_recently_viewed
        self._get_recommendations = get_recommendations
        self._add_user = add_user
        self._add_group = add_group
        self._remove_user = remove_user
        self._remove_group = remove_group
        self._user_id = user_id
        self._group_id = group_id
        self._include_usage_flag = include_usage_flag
        self._parameter_dict = parameter_dict
        self._validate_inputs()
        self.set_parameter_dict()

    @property
    def mutually_exclusive_params(self):
        return [
            self._create_site,
            self._update_site,
            self._delete_site,
            self._query_site,
            self._query_sites,
            self._query_views,
            self._get_users,
            self._get_groups,
            self._get_recently_viewed,
            self._get_recommendations,
            self._add_user,
            self._add_group,
            self._remove_user,
            self._remove_group
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    def set_parameter_dict(self):
        self._parameter_dict = self._parameter_dict or dict()
        if self._include_usage_flag is True:
            self._parameter_dict.update({"includeUsage": "includeUsage=True"})
        
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
    def base_site_content_recent_url(self):
        return "{0}/content/recent".format(self.base_site_id_url)

    @property
    def base_site_recommendations_url(self):
        return "{0}/recommendations".format(self.base_site_id_url)
    
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
        url = None
        if not self._delete_site and (self._site_id or self._user_id or self._group_id):
            if self._update_site:
                url = self.base_site_id_url
            elif self._get_users:
                url = self.base_site_user_url
            elif self._add_user:
                url = self.base_site_user_url
            elif self._get_groups:
                url = self.base_site_group_url
            elif self._get_recently_viewed:
                url = self.base_site_content_recent_url
            elif self._get_recommendations:
                url = self.base_site_recommendations_url
            elif self._add_group:
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
