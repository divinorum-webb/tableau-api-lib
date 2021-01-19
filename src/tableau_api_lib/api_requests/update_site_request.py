from tableau_api_lib.api_requests import BaseRequest


class UpdateSiteRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating sites.
    :param class ts_connection: the Tableau Server connection object
    :param str site_name: (optional) the new name of the site
    :param str content_url: (optional) the new content url for the site (typically same as site name)
    :param str content_url: (optional) the new site URL. This value can contain only characters that are valid in a URL
    :param str admin_mode: (optional) Specify ContentAndUsers to allow site administrators to use the server interface
    and tabcmd commands to add and remove users; specifying this option does not give site administrators permissions to
    manage users using the REST API; specify ContentOnly to prevent site administrators from adding or removing users;
    server administrators can always add or remove users
    :param str user_quota: (optional) the maximum number of users for the site in each of the user-based license types
    (Creator, Explorer and Viewer). Only licensed users are counted and server administrators are excluded.
    Setting this value to -1 removes any value that was set previously.
    In that case, the limit depends on the type of licensing configured for the server. For user-based license types,
    the maximum number of users is set by the licenses activated on that server. For core-based licensing, there is no
    limit to the number of users
    :param str storage_quota: (optional) the new maximum amount of space for the new site, in megabytes; if you set a
    quota and the site exceeds it, publishers will be prevented from uploading new content until the site is under the
    limit again
    :param bool disable_subscriptions_flag: True if users can subscribe to workbooks or views on the site, False
    otherwise
    :param bool flows_enabled_flag: True if flows are enabled on the site, False otherwise
    :param bool guest_access_enabled_flag: True if guest access is enabled on the site, False otherwise
    :param bool allow_subscription_attachments_flag: True if subscription attachments are enabled, False otherwise
    :param bool cache_warmup_enabled_flag: True if cache warmup is enabled on the site, False otherwise
    :param bool commenting_enabled_flag: True if commenting is enabled on the site, False otherwise
    :param bool revision_history_enabled_flag: True if the site maintains revisions for changes made to workbooks and
    datasources, False otherwise
    :param str revision_limit: (optional) an integer (entered here as a string) between 2 and 10000 to indicate a
    limited number of revisions for content
    :param bool subscribe_others_enabled_flag: boolean; True if users should not be able to subscribe others to
    workbooks or views on the site, False otherwise
    :param str extract_encryption_mode: enables, disables, or enforces extract encryption
    [enforced, enabled, or disabled]
    """
    def __init__(self,
                 ts_connection,
                 site_name=None,
                 content_url=None,
                 admin_mode=None,
                 user_quota=None,
                 state='Active',
                 tier_creator_capacity=None,
                 tier_explorer_capacity=None,
                 tier_viewer_capacity=None,
                 storage_quota=None,
                 disable_subscriptions_flag=None,
                 editing_flows_enabled_flag=None,
                 scheduling_flows_enabled_flag=None,
                 flows_enabled_flag=None,
                 guest_access_enabled_flag=None,
                 allow_subscription_attachments_flag=None,
                 cache_warmup_enabled_flag=None,
                 commenting_enabled_flag=None,
                 revision_history_enabled_flag=None,
                 revision_limit=None,
                 subscribe_others_enabled_flag=None,
                 extract_encryption_mode=None,
                 request_access_enabled_flag=None,
                 run_now_enabled_flag=None,
                 data_alerts_enabled_flag=None,
                 commenting_mentions_enabled_flag=None,
                 catalog_obfuscation_enabled_flag=None,
                 flow_auto_save_enabled_flag=None,
                 web_extraction_enabled_flag=None,
                 metrics_content_type_enabled_flag=None,
                 notify_site_admins_on_throttle_flag=None,
                 authoring_enabled_flag=None,
                 custom_subscription_email_enabled_flag=None,
                 custom_subscription_email=None,
                 custom_subscription_footer_enabled_flag=None,
                 custom_subscription_footer=None,
                 ask_data_mode="EnabledByDefault",
                 named_sharing_enabled_flag=None,
                 mobile_biometrics_enabled_flag=None,
                 sheet_image_enabled_flag=None,
                 cataloging_enabled_flag=None,
                 derived_permissions_enabled_flag=None,
                 user_visibility_mode="FULL",
                 use_default_time_zone_flag=None,
                 time_zone=None,
                 auto_suspend_refresh_enabled_flag=None,
                 auto_suspend_refresh_inactivity_window=None,
                 ):

        super().__init__(ts_connection)
        self._site_name = site_name
        self._content_url = content_url
        self._admin_mode = admin_mode
        self._user_quota = user_quota
        self._state = state
        self._tier_creator_capacity = (
            str(tier_creator_capacity) if tier_creator_capacity else None
        )
        self._tier_explorer_capacity = (
            str(tier_explorer_capacity) if tier_explorer_capacity else None
        )
        self._tier_viewer_capacity = (
            str(tier_viewer_capacity) if tier_viewer_capacity else None
        )
        self._storage_quota = str(storage_quota) if storage_quota else None
        self._disable_subscriptions_flag = disable_subscriptions_flag
        self._editing_flows_enabled_flag = editing_flows_enabled_flag
        self._scheduling_flows_enabled_flag = scheduling_flows_enabled_flag
        self._flows_enabled_flag = flows_enabled_flag
        self._guest_access_enabled_flag = guest_access_enabled_flag
        self._allow_subscription_attachments_flag = allow_subscription_attachments_flag
        self._cache_warmup_enabled_flag = cache_warmup_enabled_flag
        self._commenting_enabled_flag = commenting_enabled_flag
        self._revision_history_enabled_flag = revision_history_enabled_flag
        self._revision_limit = str(revision_limit) if revision_limit else None
        self._subscribe_others_enabled_flag = subscribe_others_enabled_flag
        self._extract_encryption_mode = (
            str(extract_encryption_mode).lower() if extract_encryption_mode else None
        )
        self._request_acces_enabled_flag = request_access_enabled_flag
        self._run_now_enabled_flag = run_now_enabled_flag
        self._data_alerts_enabled_flag = (data_alerts_enabled_flag,)
        self._commenting_mentions_enabled_flag = (commenting_mentions_enabled_flag,)
        self._catalog_obfuscation_enabled_flag = (catalog_obfuscation_enabled_flag,)
        self._flow_auto_save_enabled_flag = (flow_auto_save_enabled_flag,)
        self._web_extraction_enabled_flag = (web_extraction_enabled_flag,)
        self._metrics_content_type_enabled_flag = (metrics_content_type_enabled_flag,)
        self._notify_site_admins_on_throttle_flag = (
            notify_site_admins_on_throttle_flag,
        )
        self._authoring_enabled_flag = (authoring_enabled_flag,)
        self._custom_subscription_email_enabled_flag = (
            custom_subscription_email_enabled_flag,
        )
        self._custom_subscription_email = (custom_subscription_email,)
        self._custom_subscription_footer_enabled_flag = (
            custom_subscription_footer_enabled_flag,
        )
        self._custom_subscription_footer = (custom_subscription_footer,)
        self._ask_data_mode = (ask_data_mode,)
        self._named_sharing_enabled_flag = (named_sharing_enabled_flag,)
        self._mobile_biometrics_enabled_flag = (mobile_biometrics_enabled_flag,)
        self._sheet_image_enabled_flag = (sheet_image_enabled_flag,)
        self._cataloging_enabled_flag = (cataloging_enabled_flag,)
        self._derived_permissions_enabled_flag = (derived_permissions_enabled_flag,)
        self._user_visibility_mode = (user_visibility_mode,)
        self._use_default_time_zone_flag = (use_default_time_zone_flag,)
        self._time_zone = (time_zone,)
        self._auto_suspend_refresh_enabled_flag = (auto_suspend_refresh_enabled_flag,)
        self._auto_suspend_refresh_inactivity_window = (
            str(auto_suspend_refresh_inactivity_window)
            if auto_suspend_refresh_inactivity_window
            else None,
        )
        self._validate_inputs()
        self._request_body = {'site': {}}

    @property
    def valid_extract_encryption_modes(self):
        return [
            'enforced',
            'enabled',
            'disabled',
            None
        ]

    def _validate_inputs(self):
        valid = True
        if self._extract_encryption_mode not in self.valid_extract_encryption_modes:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def optional_param_keys(self):
        return [
            'name',
            'contentUrl',
            'adminMode',
            'state',
            "tierCreatorCapacity",
            "tierExplorerCapacity",
            "tierViewerCapacity",
            "storageQuota",
            "disableSubscriptions",
            "editingFlowsEnabled",
            "schedulingFlowsEnabled",
            "flowsEnabled",
            "guestAccessEnabled",
            "allowSubscriptionAttachments",
            "cacheWarmupEnabled",
            "commentingEnabled",
            "revisionHistoryEnabled",
            "revisionLimit",
            "subscribeOthersEnabled",
            "extractEncryptionMode",
            "requestAccessEnabled",
            "runNowEnabled",
            "dataAlertsEnabled",
            "commentingMentionsEnabled",
            "catalogObfuscationEnabled",
            "flowAutoSaveEnabled",
            "webExtractionEnabled",
            "metricsContentTypeEnabled",
            "notifySiteAdminsOnThrottle",
            "authoringEnabled",
            "customSubscriptionEmailEnabled",
            "customSubscriptionEmail",
            "customSubscriptionFooterEnabled",
            "customSubscriptionFooter",
            "askDataMode",
            "namedSharingEnabled",
            "mobileBiometricsEnabled",
            "sheetImageEnabled",
            "catalogingEnabled",
            "derivedPermissionsEnabled",
            "userVisibilityMode",
            "useDefaultTimeZone",
            "timeZone",
            "autoSuspendRefreshEnabled",
            "autoSuspendRefreshInactivityWindow",
        ]

    @property
    def optional_param_values(self):
        return [
            self._site_name,
            self._content_url,
            self._admin_mode,
            self._state,
            self._tier_creator_capacity,
            self._tier_explorer_capacity,
            self._tier_viewer_capacity,
            self._storage_quota,
            self._disable_subscriptions_flag,
            self._editing_flows_enabled_flag,
            self._scheduling_flows_enabled_flag,
            self._flows_enabled_flag,
            self._guest_access_enabled_flag,
            self._allow_subscription_attachments_flag,
            self._cache_warmup_enabled_flag,
            self._commenting_enabled_flag,
            self._revision_history_enabled_flag,
            self._revision_limit,
            self._subscribe_others_enabled_flag,
            self._extract_encryption_mode,
            self._request_acces_enabled_flag,
            self._run_now_enabled_flag,
            self._data_alerts_enabled_flag,
            self._commenting_mentions_enabled_flag,
            self._catalog_obfuscation_enabled_flag,
            self._flow_auto_save_enabled_flag,
            self._web_extraction_enabled_flag,
            self._metrics_content_type_enabled_flag,
            self._notify_site_admins_on_throttle_flag,
            self._authoring_enabled_flag,
            self._custom_subscription_email_enabled_flag,
            self._custom_subscription_email,
            self._custom_subscription_footer_enabled_flag,
            self._custom_subscription_footer,
            self._ask_data_mode,
            self._named_sharing_enabled_flag,
            self._mobile_biometrics_enabled_flag,
            self._sheet_image_enabled_flag,
            self._cataloging_enabled_flag,
            self._derived_permissions_enabled_flag,
            self._user_visibility_mode,
            self._use_default_time_zone_flag,
            self._time_zone,
            self._auto_suspend_refresh_enabled_flag,
            self._auto_suspend_refresh_inactivity_window,
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
