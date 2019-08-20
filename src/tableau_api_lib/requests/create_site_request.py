from tableau.client.requests import BaseRequest


class CreateSiteRequest(BaseRequest):
    """
    Create site request for generating API request URLs to Tableau Server.

    :param ts_connection:                   The Tableau Server connection object.
    :type ts_connection:                    class
    :param site_name:                       The name for the site being created.
    :type site_name:                        string
    :param content_url:                     The content url for the site (typically is the same as site name).
    :type content_url:                      string
    :param admin_mode:                      Set this to 'ContentAndUsers' to allow site administrators to use the
                                            server interface and tabcmd commands to add and remove users.
                                            Set this to 'ContentOnly' to prevent site administrators from adding or
                                            removing users.
    :type admin_mode:                       string
    :param user_quota:                      The maximum number of users for the site in each of the user-based license
                                            types (Creator, Explorer, Viewer).
    :type user_quota:                       string
    :param storage_quota:                   The maximum amount of space for storage.
    :type storage_quota:                    string
    :param disable_subscriptions_flag:      Boolean flag; True if disabling subscriptions, False otherwise.
    :type disable_subscriptions_flag:       boolean
    :param flows_enabled_flag:              Boolean flag; True if flows are enabled, False otherwise.
    :type flows_enabled_flag:               boolean
    :param guest_access_enabled_flag:       Boolean flag; True if guest access is enabled, False otherwise.
    :type guest_access_enabled_flag:        boolean
    :param cache_warmup_enabled_flag:       Boolean flag; True if cache warmup is enabled, False otherwise.
    :type cache_warmup_enabled_flag:        boolean
    :param commenting_enabled_flag:         Boolean flag; True if commenting is enabled, False otherwise.
    :type commenting_enabled_flag:          boolean
    :param revision_history_enabled_flag:   Boolean flag; True if revision history is enabled, False otherwise.
    :type revision_history_enabled_flag:    boolean
    :param revision_limit:                  The maximum number of revisions stored on the server. The number can be
                                            between 2 and 10,000, or set to -1 in order to remove the limit entirely.
    :type revision_limit:                   string
    :param subscribe_others_enabled_flag:   Boolean flag; True if owners can subscribe other users, False otherwise.
    :type subscribe_others_enabled_flag:    boolean
    """
    def __init__(self,
                 ts_connection,
                 site_name,
                 content_url,
                 admin_mode='ContentAndUsers',
                 user_quota=None,
                 storage_quota=None,
                 disable_subscriptions_flag=False,
                 flows_enabled_flag=None,
                 guest_access_enabled_flag=False,
                 cache_warmup_enabled_flag=False,
                 commenting_enabled_flag=False,
                 revision_history_enabled_flag=False,
                 revision_limit=None,
                 subscribe_others_enabled_flag=False):

        super().__init__(ts_connection)
        self._site_name = site_name
        self._content_url = content_url
        self._admin_mode = admin_mode
        self._user_quota = user_quota
        self._storage_quota = storage_quota
        self._disable_subscriptions_flag = disable_subscriptions_flag
        self._flows_enabled_flag = flows_enabled_flag
        self._guest_access_enabled_flag = guest_access_enabled_flag
        self._cache_warmup_enabled_flag = cache_warmup_enabled_flag
        self._commenting_enabled_flag = commenting_enabled_flag
        self._revision_history_enabled_flag = revision_history_enabled_flag
        self._revision_limit = revision_limit
        self._subscribe_others_enabled_flag = subscribe_others_enabled_flag
        self.base_create_site_request()

    @property
    def optional_param_keys(self):
        return [
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
            str(self._storage_quota) if self._storage_quota else None,
            'true' if self._disable_subscriptions_flag else None,
            'true' if self._flows_enabled_flag else None,
            'true' if self._guest_access_enabled_flag else None,
            'true' if self._cache_warmup_enabled_flag else None,
            'true' if self._commenting_enabled_flag else None,
            'true' if self._revision_history_enabled_flag else None,
            str(self._revision_limit) if self._revision_limit else None,
            'true' if self._subscribe_others_enabled_flag else None
        ]

    def base_create_site_request(self):
        self._request_body.update({
            'site': {
                'name': self._site_name,
                'contentUrl': self._content_url,
                'adminMode': self._admin_mode
            }
        })
        return self._request_body

    def modified_create_site_request(self):
        if self._user_quota and self._admin_mode != 'ContentOnly':
            self._request_body['site'].update({'userQuota': str(self._user_quota)})
        elif self._user_quota:
            self._invalid_parameter_exception()

        self._request_body['site'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                    self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_create_site_request()
