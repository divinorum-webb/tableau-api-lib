from tableau.client.requests import BaseRequest


class UpdateSiteRequest(BaseRequest):
    """
    Update site request for API requests to Tableau Server.

    :param ts_connection:                   The Tableau Server connection object.
    :type ts_connection:                    class
    :param site_name:                       (Optional) The new name of the site.
    :type site_name:                        string
    :param content_url:                     (Optional) The new content url for the site (typically same as site name).
    :type content_url:                      string
    :param content_url:                     (Optional) The new site URL. This value can contain only characters that
                                            are valid in a URL.
    :type content_url:                      string
    :param admin_mode:                      (Optional) Specify ContentAndUsers to allow site administrators to use
                                            the server interface and tabcmd commands to add and remove users.
                                            (Specifying this option does not give site administrators permissions to
                                            manage users using the REST API.)
                                            Specify ContentOnly to prevent site administrators from adding or
                                            removing users.
                                            (Server administrators can always add or remove users.)
    :type admin_mode:                       string
    :param user_quota:                      (Optional) The maximum number of users for the site in each of the
                                            user-based license types (Creator, Explorer and Viewer). Only licensed
                                            users are counted and server administrators are excluded.
                                            Setting this value to -1 removes any value that was set previously.
                                            In that case, the limit depends on the type of licensing configured for
                                            the server. For user-based license types, the maximum number of users is
                                            set by the licenses activated on that server. For core-based licensing,
                                            there is no limit to the number of users.
    :type user_quota:                       string
    :param storage_quota:                   (Optional) The new maximum amount of space for the new site, in megabytes.
                                            If you set a quota and the site exceeds it, publishers will be prevented
                                            from uploading new content until the site is under the limit again.
    :type storage_quota:                    string
    :param disable_subscriptions_flag:      Boolean flag; True if users can subscribe to workbooks or views on the
                                            site, False otherwise.
    :type disable_subscriptions_flag:       boolean
    :param flows_enabled_flag:              Boolean flag; True if flows are enabled on the site, False otherwise.
    :type flows_enabled_flag:               boolean
    :param guest_access_enabled_flag:       Boolean flag; True if guest access is enabled on the site, False otherwise.
    :type guest_access_enabled_flag:        boolean
    :param cache_warmup_enabled_flag:       Boolean flag; True if cache warmup is enabled on the site, False otherwise.
    :type cache_warmup_enabled_flag:        boolean
    :param commenting_enabled_flag:         Boolean flag; True if commenting is enabled on the site, False otherwise.
    :type commenting_enabled_flag:          boolean
    :param revision_history_enabled_flag:   Boolean flag; True if the site maintains revisions for changes made to
                                            workbooks and datasources, False otherwise.
    :type revision_history_enabled_flag:    boolean
    :param revision_limit:                  (Optional) An integer (entered here as a string) between 2 and 10000 to
                                            indicate a limited number of revisions for content.
    :type revision_limit:                   string
    :param subscribe_others_enabled_flag:   Boolean; True if users should not be able to subscribe others to
                                            workbooks or views on the site, False otherwise.
    :type subscribe_others_enabled_flag:    boolean
    """
    def __init__(self,
                 ts_connection,
                 site_name=None,
                 content_url=None,
                 admin_mode=None,
                 user_quota=None,
                 state='Active',
                 storage_quota=None,
                 disable_subscriptions_flag=None,
                 flows_enabled_flag=None,
                 guest_access_enabled_flag=False,
                 cache_warmup_enabled_flag=False,
                 commenting_enabled_flag=False,
                 revision_history_enabled_flag=False,
                 revision_limit=None,
                 subscribe_others_enabled_flag=False
                 ):

        super().__init__(ts_connection)
        self._site_name = site_name
        self._content_url = content_url
        self._admin_mode = admin_mode
        self._user_quota = user_quota
        self._state = state
        self._storage_quota = storage_quota
        self._disable_subscriptions_flag = disable_subscriptions_flag
        self._flows_enabled_flag = flows_enabled_flag
        self._guest_access_enabled_flag = guest_access_enabled_flag
        self._cache_warmup_enabled_flag = cache_warmup_enabled_flag
        self._commenting_enabled_flag = commenting_enabled_flag
        self._revision_history_enabled_flag = revision_history_enabled_flag
        self._revision_limit = revision_limit
        self._subscribe_others_enabled_flag = subscribe_others_enabled_flag
        self._request_body = {'site': {}}

    @property
    def optional_param_keys(self):
        return [
            'name',
            'contentUrl',
            'adminMode',
            'state',
            'storageQuota',
            'disableSubscriptions',
            'flowsEnabled',
            'guestAccessEnabled',
            'cacheWarmupEnabled',
            'commentingEnabled',
            'revisionHistoryEnabled',
            'revisionLimit',
            'subscribeOthersEnabled'
        ]

    @property
    def optional_param_values(self):
        return [
            self._site_name,
            self._content_url,
            self._admin_mode,
            self._state,
            str(self._storage_quota) if self._storage_quota else None,
            'true' if self._disable_subscriptions_flag is True
            else 'false' if self._disable_subscriptions_flag is False else None,
            'true' if self._flows_enabled_flag is True
            else 'false' if self._flows_enabled_flag is False else None,
            'true' if self._guest_access_enabled_flag is True
            else 'false' if self._guest_access_enabled_flag is False else None,
            'true' if self._cache_warmup_enabled_flag is True
            else 'false' if self._cache_warmup_enabled_flag is False else None,
            'true' if self._commenting_enabled_flag is True
            else 'false' if self._commenting_enabled_flag is False else None,
            'true' if self._revision_history_enabled_flag is True
            else 'false' if self._revision_history_enabled_flag is False else None,
            str(self._revision_limit) if self._revision_limit else None,
            'true' if self._subscribe_others_enabled_flag is True
            else 'false' if self._subscribe_others_enabled_flag is False else None
        ]

    def base_update_site_request(self):
        if self._user_quota and self._admin_mode != 'ContentOnly':
            self._request_body['site'].update({'userQuota': str(self._user_quota)})
        elif self._user_quota:
            self._invalid_parameter_exception()

        self._request_body['site'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                    self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.base_update_site_request()
