import requests

from tableau_api_lib.api_endpoints import AuthEndpoint, DataAlertEndpoint, DatabaseEndpoint, DatasourceEndpoint, \
    FavoritesEndpoint, FileUploadEndpoint, FlowEndpoint, GroupEndpoint, JobsEndpoint, PermissionsEndpoint, \
    ProjectEndpoint, SchedulesEndpoint, SiteEndpoint, SubscriptionsEndpoint, UserEndpoint, TableEndpoint, \
    TasksEndpoint, ViewEndpoint, WorkbookEndpoint, ColumnEndpoint, DQWarningEndpoint, EncryptionEndpoint, \
    GraphqlEndpoint, WebhookEndpoint
from tableau_api_lib.api_requests import AddDatasourcePermissionsRequest, AddDatasourceToFavoritesRequest, \
    AddDatasourceToScheduleRequest, AddDefaultPermissionsRequest, AddFlowPermissionsRequest, \
    AddFlowToScheduleRequest, AddProjectPermissionsRequest, AddProjectToFavoritesRequest, \
    AddTagsRequest, AddUserToAlertRequest, AddUserToGroupRequest, AddUserToSiteRequest, \
    AddViewPermissionsRequest, AddViewToFavoritesRequest, AddWorkbookPermissionsRequest, \
    AddWorkbookToFavoritesRequest, AddWorkbookToScheduleRequest, CreateExtractsForWorkbookRequest, CreateGroupRequest, \
    CreateProjectRequest, CreateScheduleRequest, CreateSiteRequest, CreateSubscriptionRequest, CreateWebhookRequest, \
    EmptyRequest, GraphqlRequest, PublishDatasourceRequest, PublishFlowRequest, PublishWorkbookRequest, SignInRequest, \
    SwitchSiteRequest, UpdateDataAlertRequest, UpdateDatabaseRequest, UpdateDatasourceConnectionRequest, \
    UpdateDatasourceRequest, UpdateFlowConnectionRequest, UpdateFlowRequest, UpdateGroupRequest, \
    UpdateProjectRequest, UpdateScheduleRequest, UpdateSiteRequest, UpdateSubscriptionRequest, \
    UpdateUserRequest, UpdateWorkbookConnectionRequest, UpdateWorkbookRequest, UpdateTableRequest, \
    UpdateColumnRequest, AddDQWarningRequest, UpdateDQWarningRequest
from tableau_api_lib.decorators import verify_signed_in, verify_config_variables, \
    verify_rest_api_version, verify_api_method_exists


class TableauServerConnection:
    def __init__(self,
                 config_json,
                 env='tableau_prod',
                 ssl_verify=True):
        """
        A connection to Tableau Server built upon the configuration details provided.
        :param dict config_json: a dict or JSON object containing configuration details
        :param str env: the configuration environment to reference from the configuration dict
        :param bool ssl_verify: verifies SSL certs for HTTP requests if True, skips verification if False.
        """
        self._env = env
        self._config = config_json
        self._auth_token = None
        self.ssl_verify = ssl_verify
        self.site_url = self._config[self._env]['site_url']
        self.site_name = self._config[self._env]['site_name']
        self.site_id = None
        self.user_id = None
        self.active_endpoint = None
        self.active_request = None
        self.active_headers = None
        self.auth_method = self._get_auth_method()

    @property
    def server(self):
        return self._config[self._env]['server']

    @property
    def api_version(self):
        return self._config[self._env]['api_version']

    @property
    def username(self):
        config_keys = self._config[self._env].keys()
        if 'username' in config_keys and 'password' in config_keys:
            username = self._config[self._env]['username']
        else:
            username = None
        return username

    @property
    def password(self):
        config_keys = self._config[self._env].keys()
        if 'password' in config_keys and 'username' in config_keys:
            password = self._config[self._env]['password']
        else:
            password = None
        return password

    @property
    def personal_access_token_name(self):
        config_keys = self._config[self._env].keys()
        if 'personal_access_token_name' in config_keys and 'personal_access_token_secret' in config_keys:
            personal_access_token_name = self._config[self._env]['personal_access_token_name']
        else:
            personal_access_token_name = None
        return personal_access_token_name

    @property
    def personal_access_token_secret(self):
        config_keys = self._config[self._env].keys()
        if 'personal_access_token_name' in config_keys and 'personal_access_token_secret' in config_keys:
            personal_access_token_secret = self._config[self._env]['personal_access_token_secret']
        else:
            personal_access_token_secret = None
        return personal_access_token_secret

    @property
    def sign_in_headers(self):
        return {
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

    @property
    def x_auth_header(self):
        return {
            "X-Tableau-Auth": self.auth_token
        }

    @property
    def default_headers(self):
        headers = self.sign_in_headers.copy()
        headers.update({"X-Tableau-Auth": self.auth_token})
        return headers

    @property
    def graphql_headers(self):
        headers = {"X-Tableau-Auth": self.auth_token}
        return headers

    @property
    def auth_token(self):
        return self._auth_token

    @auth_token.setter
    def auth_token(self, token_value):
        if token_value != self._auth_token or token_value is None:
            self._auth_token = token_value
        else:
            raise Exception('You are already signed in with a valid auth token.')

    def _get_auth_method(self):
        if self.username and self.password:
            if not (self.personal_access_token_name or self.personal_access_token_secret):
                return 'user_and_password'
        elif self.personal_access_token_name and self.personal_access_token_secret:
            if not (self.username or self.password):
                return 'personal_access_token'
        raise Exception("""
        The Tableau Server configuration provided contains username, password, and personal access token credentials.
        Successful authentication requires either username & password OR personal access token.
        Please update the configuration details to only provide details for one of the available authentication methods.
        """)

    # authentication

    @verify_rest_api_version
    @verify_config_variables
    def sign_in(self, user_to_impersonate=None):
        """
        Signs in to Tableau Server.
        :param str user_to_impersonate: (optional) the user ID for the user being impersonated
        :return: HTTP response
        """
        request = SignInRequest(ts_connection=self,
                                auth_method=self.auth_method,
                                username=self.username,
                                password=self.password,
                                personal_access_token_name=self.personal_access_token_name,
                                personal_access_token_secret=self.personal_access_token_secret,
                                user_to_impersonate=user_to_impersonate).get_request()
        endpoint = AuthEndpoint(ts_connection=self, sign_in=True).get_endpoint()
        response = requests.post(url=endpoint, json=request, headers=self.sign_in_headers, verify=self.ssl_verify)
        if response.status_code == 200:
            self.auth_token = response.json()['credentials']['token']
            self.site_id = response.json()['credentials']['site']['id']
            self.user_id = response.json()['credentials']['user']['id']
        return response

    @verify_signed_in
    def sign_out(self):
        """
        Signs out from Tableau Server.
        :return: HTTP response
        """
        endpoint = AuthEndpoint(ts_connection=self, sign_out=True).get_endpoint()
        response = requests.post(url=endpoint, headers=self.x_auth_header, verify=self.ssl_verify)
        if response.status_code == 204:
            self.auth_token = None
            self.site_id = None
            self.user_id = None
        return response

    @verify_signed_in
    @verify_api_method_exists('2.6')
    def switch_site(self, content_url):
        """
        Switches the connection to the specified site, whose site name is provided as 'content_url'.
        :param string content_url: The 'content_url' is the site name as displayed in the url
        :return: HTTP response
        """
        self.active_request = SwitchSiteRequest(ts_connection=self, site_name=content_url).get_request()
        self.active_endpoint = AuthEndpoint(ts_connection=self, switch_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        if response.status_code == 200:
            self.auth_token = response.json()['credentials']['token']
            self.site_id = response.json()['credentials']['site']['id']
            self.site_name = self.query_site().json()['site']['name']
            self.site_url = response.json()['credentials']['site']['contentUrl']
            self.user_id = response.json()['credentials']['user']['id']
        return response

    @verify_api_method_exists('2.4')
    def server_info(self):
        """
        Provides information about the active Tableau Server connection.
        :return: HTTP response
        """
        self.active_endpoint = AuthEndpoint(ts_connection=self, get_server_info=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # sites

    @verify_api_method_exists('2.3')
    def create_site(self,
                    site_name,
                    content_url,
                    admin_mode='ContentAndUsers',
                    user_quota=None,
                    storage_quota=None,
                    disable_subscriptions_flag=None,
                    flows_enabled_flag=None,
                    guest_access_enabled_flag=None,
                    allow_subscription_attachments_flag=None,
                    cache_warmup_enabled_flag=None,
                    commenting_enabled_flag=None,
                    revision_history_enabled_flag=None,
                    revision_limit=None,
                    subscribe_others_enabled_flag=None,
                    extract_encryption_mode=None):
        """
        Creates a new site via the active Tableau Server connection.
        :param str site_name: The name for the new site.
        :param str content_url: The content url for the new site (can be different than the site name).
        :param str admin_mode: The admin mode for the new site.
        :param str user_quota: The user quota for the site.
        :param str storage_quota: The storage size quota for the site, in megabytes.
        :param bool disable_subscriptions_flag: True if disabling subscriptions, defaults to False.
        :param bool flows_enabled_flag: True if flows are enabled, defaults to True.
        :param bool guest_access_enabled_flag: True if guest access is enabled, defaults to False.
        :param bool allow_subscription_attachments_flag: True if subscription attachments are enabled, defaults to False
        :param bool cache_warmup_enabled_flag: True if cache warmup is enabled, defaults to False.
        :param bool commenting_enabled_flag: True if commenting is enabled, defaults to False.
        :param bool revision_history_enabled_flag: True if revision history is enabled, defaults to False.
        :param string revision_limit: The maximum number of revisions stored on the server. The number can be
        between 2 and 10,000, or set to -1 in order to remove the limit.
        :param bool subscribe_others_enabled_flag: True if owners can subscribe other users, False otherwise.
        :param str extract_encryption_mode: enables, disables, or enforces extract encryption
        [enforced, enabled, or disabled]
        :return: HTTP response
        """
        # This method can only be called by server administrators.
        self.active_request = CreateSiteRequest(ts_connection=self,
                                                site_name=site_name,
                                                content_url=content_url,
                                                admin_mode=admin_mode,
                                                user_quota=user_quota,
                                                storage_quota=storage_quota,
                                                disable_subscriptions_flag=disable_subscriptions_flag,
                                                flows_enabled_flag=flows_enabled_flag,
                                                guest_access_enabled_flag=guest_access_enabled_flag,
                                                allow_subscription_attachments_flag=allow_subscription_attachments_flag,
                                                cache_warmup_enabled_flag=cache_warmup_enabled_flag,
                                                commenting_enabled_flag=commenting_enabled_flag,
                                                revision_history_enabled_flag=revision_history_enabled_flag,
                                                revision_limit=revision_limit,
                                                subscribe_others_enabled_flag=subscribe_others_enabled_flag,
                                                extract_encryption_mode=extract_encryption_mode
                                                ).get_request()
        self.active_endpoint = SiteEndpoint(ts_connection=self, create_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_site(self, include_usage_flag=False, parameter_dict=None):
        """
        Queries details for the active site.
        :param bool include_usage_flag: True if including usage metrics, False otherwise
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            query_site=True,
                                            site_id=self.site_id,
                                            include_usage_flag=include_usage_flag,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_sites(self, parameter_dict=None):
        """
        Query details for all sites on the server.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            query_sites=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def get_recently_viewed_for_site(self):
        """
        Gets the details of the views and workbooks on a site that have been most recently created, updated, or
        accessed by the signed in user.
        :return: HTTP response
        """
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            site_id=self.site_id,
                                            get_recently_viewed=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_views_for_site(self, site_id, parameter_dict=None):
        """
        Query details for all views on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :param str site_id: the site ID to be queried
        :return: HTTP response
        """
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            site_id=site_id,
                                            query_views=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_site(self,
                    site_id,
                    site_name=None,
                    content_url=None,
                    admin_mode=None,
                    user_quota=None,
                    state=None,
                    storage_quota=None,
                    disable_subscriptions_flag=None,
                    flows_enabled_flag=None,
                    guest_access_enabled_flag=None,
                    allow_subscription_attachments_flag=None,
                    cache_warmup_enabled_flag=None,
                    commenting_enabled_flag=None,
                    revision_history_enabled_flag=None,
                    revision_limit=None,
                    subscribe_others_enabled_flag=None,
                    extract_encryption_mode=None
                    ):
        """
        Update details for the specified site.
        :param string site_id: the site ID
        :param string site_name: the site's user-friendly name
        :param string content_url: the site's name as displayed in the url
        :param string admin_mode: the site's admin mode
        :param string user_quota: sets a user quota value for the site
        :param string state: sets the state for the site
        :param string storage_quota: sets the storage quota in megabytes for the site
        :param boolean disable_subscriptions_flag: enables or disables subscriptions
        :param boolean flows_enabled_flag: enables or disables flows
        :param boolean guest_access_enabled_flag: enables or disables guest access
        :param bool allow_subscription_attachments_flag: enables or disables subscription attachments
        :param boolean cache_warmup_enabled_flag: enables or disables cache warmup
        :param boolean commenting_enabled_flag: enables or disables commenting
        :param boolean revision_history_enabled_flag: enables or disables versioning
        :param string revision_limit: sets the maximum number of revisions allowed for a versioned object
        :param boolean subscribe_others_enabled_flag: True if users can subscribe on behalf of others, False otherwise
        :param str extract_encryption_mode: enables, disables, or enforces extract encryption
        [enforced, enabled, or disabled]
        :return: HTTP response
        """
        # This method can only be called by server administrators.
        self.active_request = UpdateSiteRequest(ts_connection=self,
                                                site_name=site_name,
                                                content_url=content_url,
                                                admin_mode=admin_mode,
                                                user_quota=user_quota,
                                                state=state,
                                                storage_quota=storage_quota,
                                                disable_subscriptions_flag=disable_subscriptions_flag,
                                                flows_enabled_flag=flows_enabled_flag,
                                                guest_access_enabled_flag=guest_access_enabled_flag,
                                                allow_subscription_attachments_flag=allow_subscription_attachments_flag,
                                                cache_warmup_enabled_flag=cache_warmup_enabled_flag,
                                                commenting_enabled_flag=commenting_enabled_flag,
                                                revision_history_enabled_flag=revision_history_enabled_flag,
                                                revision_limit=revision_limit,
                                                subscribe_others_enabled_flag=subscribe_others_enabled_flag,
                                                extract_encryption_mode=extract_encryption_mode
                                                ).get_request()
        self.active_endpoint = SiteEndpoint(ts_connection=self, site_id=site_id, update_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_site(self,
                    site_id=None,
                    site_name=None,
                    content_url=None):
        """
        Deletes the specified site.
        :param string site_id: the site ID
        :param string site_name: the site's user-friendly name
        :param string content_url: the site's name as it appears in the url
        :return: HTTP response
        """
        # This method can only be called by server administrators.
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            delete_site=True,
                                            site_id=site_id,
                                            site_name=site_name,
                                            content_url=content_url).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # data driven alerts

    @verify_api_method_exists('3.2')
    def delete_data_driven_alert(self, data_alert_id):
        """
        Deletes the specified data driven alert.
        :param string data_alert_id: the data driven alert ID
        :return: HTTP response
        """
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def query_data_driven_alert_details(self, data_alert_id):
        """
        Queries details for the specified data driven alert.
        :param string data_alert_id: the data driven alert ID
        :return: HTTP response
        """
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 query_data_alert=True,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def query_data_driven_alerts(self, parameter_dict=None):
        """
        Queries the data driven alerts for the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 query_data_alerts=True,
                                                 parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def add_user_to_data_driven_alert(self,
                                      user_id,
                                      data_alert_id):
        """
        Adds the specified user to the specified data driven alert.
        :param user_id: the user ID for the user being added to the alert
        :param data_alert_id: the data driven alert ID
        :return: HTTP response
        """
        # this appears to be broken on Tableau's side, always returning an internal server error
        self.active_request = AddUserToAlertRequest(ts_connection=self,
                                                    user_id=user_id).get_request()
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 add_user=True,
                                                 user_id=user_id,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def delete_user_from_data_driven_alert(self,
                                           user_id,
                                           data_alert_id):
        """
        Removes the specified user from the specified data driven alert.
        :param user_id: the user ID for the user being removed from the alert
        :param data_alert_id: the data driven alert ID
        :return: HTTP response
        """
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 remove_user=True,
                                                 user_id=user_id,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def update_data_driven_alert(self,
                                 data_alert_id,
                                 data_alert_subject=None,
                                 data_alert_frequency=None,
                                 data_alert_owner_id=None,
                                 is_public_flag=None):
        """
        Updates the specified data driven alert.
        :param string data_alert_id: the data driven alert ID
        :param string data_alert_subject: the subject for the data driven alert
        :param string data_alert_frequency: the frequency for the data driven alert
        :param string data_alert_owner_id: the user ID for the owner of the data driven alert
        :param boolean is_public_flag: determines whether the data driven alert is public or private
        :return: HTTP response
        """
        self.active_request = UpdateDataAlertRequest(ts_connection=self,
                                                     data_alert_subject=data_alert_subject,
                                                     data_alert_frequency=data_alert_frequency,
                                                     data_alert_owner_id=data_alert_owner_id,
                                                     is_public_flag=is_public_flag).get_request()
        self.active_endpoint = DataAlertEndpoint(ts_connection=self, data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    # flows

    @verify_api_method_exists('3.3')
    def query_flow(self, flow_id):
        """
        Queries details for the specified flow.
        :param string flow_id: the flow ID
        :return: HTTP response
        """
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            flow_id=flow_id,
                                            query_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def delete_flow(self, flow_id):
        """
        Deletes the specified flow.
        :param string flow_id: the flow ID
        :return: HTTP response
        """
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            flow_id=flow_id,
                                            delete_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def download_flow(self, flow_id):
        """
        Downloads the specified flow.
        :param string flow_id: the flow ID
        :return: HTTP response
        """
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            flow_id=flow_id,
                                            download_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def query_flow_connections(self, flow_id):
        """
        Queries the connection details for the specified flow.
        :param string flow_id: the flow ID
        :return: HTTP response
        """
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            flow_id=flow_id,
                                            query_flow_connections=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def query_flows_for_site(self, parameter_dict=None):
        """
        Queries details for all flows on the active site.
        :return: HTTP Response
        """
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            query_flows_for_site=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def query_flows_for_user(self,
                             user_id,
                             parameter_dict=None):
        """
        Queries details for all flows belonging to the specified user.
        :param string user_id: the user ID for the user whose flows are being queried
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            user_id=user_id,
                                            query_flows_for_user=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def update_flow(self,
                    flow_id,
                    new_project_id=None,
                    new_owner_id=None):
        """
        Updates details for the specified flow.
        :param string flow_id: the flow ID
        :param string new_project_id: (optional) the new project ID the flow will belong to
        :param string new_owner_id: (optional) the new onwer ID the flow will belong to
        :return: HTTP response
        """
        self.active_request = UpdateFlowRequest(ts_connection=self,
                                                new_project_id=new_project_id,
                                                new_owner_id=new_owner_id).get_request()
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            flow_id=flow_id,
                                            update_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def update_flow_connection(self,
                               flow_id,
                               connection_id,
                               server_address=None,
                               port=None,
                               connection_username=None,
                               connection_password=None,
                               embed_password_flag=None):
        """
        Updates details for the specified connection in the specified flow.
        Note that you must set the connection_password='' if changing the embed_password_flag from True to False
        :param string flow_id: the flow ID
        :param string connection_id: the connection ID
        :param string server_address: (optional) the server address for the connection
        :param string port: (optional) the port for the connection
        :param string connection_username: (optional) the username for the connection
        :param string connection_password: (optional) the password for the connection
        :param boolean embed_password_flag: (optional) True if embedding the credentials, false otherwise.
        :return: HTTP response
        """
        self.active_request = UpdateFlowConnectionRequest(ts_connection=self,
                                                          server_address=server_address,
                                                          port=port,
                                                          connection_username=connection_username,
                                                          connection_password=connection_password,
                                                          embed_password_flag=embed_password_flag).get_request()
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            flow_id=flow_id,
                                            connection_id=connection_id,
                                            update_flow_connection=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    # projects

    @verify_api_method_exists('2.3')
    def create_project(self,
                       project_name,
                       project_description=None,
                       content_permissions='ManagedByOwner',
                       parent_project_id=None,
                       parameter_dict=None):
        """
        Creates a new project on the active site.
        :param string project_name: the project name
        :param string project_description: the project description
        :param string content_permissions: the content permissions for the project
        :param string parent_project_id: the parent project ID, if the new project exists within a parent project
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_request = CreateProjectRequest(ts_connection=self,
                                                   project_name=project_name,
                                                   project_description=project_description,
                                                   content_permissions=content_permissions,
                                                   parent_project_id=parent_project_id).get_request()
        self.active_endpoint = ProjectEndpoint(ts_connection=self,
                                               create_project=True,
                                               parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_projects(self, parameter_dict=None):
        """
        Queries details for all projects on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = ProjectEndpoint(ts_connection=self,
                                               query_projects=True,
                                               parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_project(self,
                       project_id,
                       project_name=None,
                       project_description=None,
                       content_permissions=None,
                       parent_project_id=None):
        """
        Updates details for the specified project.
        :param string project_id: the project ID
        :param string project_name: (optional) the new project name
        :param string project_description: (optional) the new project description
        :param string content_permissions: (optional) the new project content permissions
        :param string parent_project_id: (optional) the new parent project ID
        :return: HTTP response
        """
        self.active_request = UpdateProjectRequest(ts_connection=self,
                                                   project_name=project_name,
                                                   project_description=project_description,
                                                   content_permissions=content_permissions,
                                                   parent_project_id=parent_project_id).get_request()
        self.active_endpoint = ProjectEndpoint(ts_connection=self,
                                               update_project=True,
                                               project_id=project_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_project(self, project_id):
        """
        Deletes the specified project.
        :param string project_id: the project ID
        :return: HTTP response
        """
        self.active_endpoint = ProjectEndpoint(ts_connection=self,
                                               project_id=project_id,
                                               delete_project=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # workbooks and views

    @verify_api_method_exists('2.6')
    def add_tags_to_view(self, view_id, tags):
        """
        Adds one or more tags to the specified view.
        :param string view_id: the view ID
        :param list tags: a list of tags to add to the view
        :return: HTTP response
        """
        self.active_request = AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, add_tags=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_tags_to_workbook(self, workbook_id, tags):
        """
        Adds tags to the specified workbook.
        :param string workbook_id: the workbook ID
        :param list tags: a list of tags to add to the workbook
        :return: HTTP response
        """
        self.active_request = AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                add_tags=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_views_for_workbook(self,
                                 workbook_id,
                                 parameter_dict=None):
        """
        Queries details for all views in the specified workbook.
        :param string workbook_id: the workbook ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                query_views=True,
                                                workbook_id=workbook_id,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.8')
    def query_view_data(self,
                        view_id,
                        parameter_dict=None):
        """
        Queries the underlying data within the specified view.
        Note that the CSV content is accessible via the 'content' attribute, for example response.content
        :param string view_id: the view ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = ViewEndpoint(ts_connection=self,
                                            view_id=view_id,
                                            query_view_data=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.5')
    def query_view_image(self,
                         view_id,
                         parameter_dict=None):
        """
        Downloads a PNG of the specified view.
        Note that the PNG content is accessible via the 'content' attribute, for example response.content
        :param string view_id: the view ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = ViewEndpoint(ts_connection=self,
                                            view_id=view_id,
                                            query_view_image=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.8')
    def query_view_pdf(self,
                       view_id,
                       parameter_dict=None):
        """
        Downloads a PDF of the specified view.
        Note that the PDF content is accessible via the 'content' attribute, for example response.content
        :param string view_id: the view ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = ViewEndpoint(ts_connection=self,
                                            view_id=view_id,
                                            query_view_pdf=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_view_preview_image(self,
                                 workbook_id,
                                 view_id,
                                 parameter_dict=None):
        """
        Downloads the preview image for the specified view within the specified workbook.
        Note that the image content is accessible via the 'content' attribute, for example response.content
        :param string workbook_id: the workbook ID
        :param string view_id: the view ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                view_id=view_id,
                                                query_workbook_view_preview_img=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.0')
    def get_view(self, view_id):
        """
        Queries details for the specified view.
        :param string view_id: the view ID
        :return: HTTP response
        """
        self.active_endpoint = ViewEndpoint(ts_connection=self,
                                            view_id=view_id,
                                            query_view=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.6')
    def get_view_by_path(self, view_name):
        """
        Gets the details of all views in a site with a specified name.
        :param str view_name: the name all view names will be matched against
        :return: HTTP response
        """
        self.active_endpoint = ViewEndpoint(ts_connection=self,
                                            query_views=True,
                                            parameter_dict={
                                                'filter': f'filter=viewUrlName:eq:{view_name.replace(" ", "")}'
                                            }).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.7')
    def get_recommendations_for_views(self):
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            site_id=self.site_id,
                                            get_recommendations=True,
                                            parameter_dict={'type': 'type=view'}).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    def query_view(self, view_id):
        """
        Queries details for the specified view.
        Note that this extra method exists because the official method 'get_view' appears to break naming conventions.
        :param string view_id: the view ID
        :return: HTTP response
        """
        self.active_endpoint = ViewEndpoint(ts_connection=self,
                                            view_id=view_id,
                                            query_view=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_workbook(self,
                       workbook_id,
                       parameter_dict=None):
        """
        Queries details for the specified workbook.
        :param string workbook_id: the workbook ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                query_workbook=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_workbook_connections(self,
                                   workbook_id,
                                   parameter_dict=None):
        """
        Queries connection details for the specified workbook.
        :param string workbook_id: the workbook ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                query_connections=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def get_workbook_revisions(self,
                               workbook_id,
                               parameter_dict=None):
        """
        Queries revision details for the specified workbook.
        :param string workbook_id: the workbook ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                get_workbook_revisions=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    def get_workbook_downgrade_info(self,
                                    workbook_id,
                                    downgrade_target_version):
        """
        Queries details regarding the impact of downgrading the workbook to the older target version.
        Requires API version 3.6 or higher.
        :param workbook_id: the workbook ID
        :param downgrade_target_version: the desired Tableau Desktop version to downgrade to
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                downgrade_target_version=downgrade_target_version,
                                                get_workbook_downgrade_info=True,).get_endpoint()
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def remove_workbook_revision(self,
                                 workbook_id,
                                 revision_number):
        """
        Deletes the specified revision for the specified workbook.
        :param string workbook_id: the workbook ID
        :param string revision_number: the revision number to delete
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                revision_number=revision_number,
                                                remove_workbook_revision=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_workbook_preview_image(self,
                                     workbook_id,
                                     parameter_dict=None):
        """
        Downloads the preview image for the specified workbook.
        Note that the image content is accessible via the 'content' attribute, for example response.content
        :param string workbook_id: the workbook ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        # the preview image returned is in the response body as response.content
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                query_workbook_preview_img=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_workbooks_for_site(self, parameter_dict=None):
        """
        Queries details for all workbooks on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                query_workbooks=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_workbooks_for_user(self,
                                 user_id,
                                 parameter_dict=None):
        """
        Queries details for all workbooks belonging to the specified user.
        :param string user_id: the user ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = UserEndpoint(ts_connection=self,
                                            user_id=user_id,
                                            query_workbooks_for_user=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def download_workbook(self,
                          workbook_id,
                          parameter_dict=None):
        """
        Downloads the specified workbook.
        Note that the workbook content is accessible via the 'content' attribute, for example response.content
        :param string workbook_id: the workbook ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                download_workbook=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.4')
    def download_workbook_pdf(self,
                              workbook_id,
                              parameter_dict=None):
        """
        Downloads a PDF of the specified workbook.
        Note that the PDF content is accessible via the 'content' attribute, for example response.content
        :param string workbook_id: the workbook ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP Response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                download_workbook_pdf=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def download_workbook_revision(self,
                                   workbook_id,
                                   revision_number,
                                   parameter_dict=None):
        """
        Downloads an older version of the specified workbook.
        Note that the current version of the workbook is not a revision, so cannot be downloaded via this method.
        :param string workbook_id: the workbook ID
        :param string revision_number: the workbook revision number to download
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                revision_number=revision_number,
                                                download_workbook_revision=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_workbook(self,
                        workbook_id,
                        show_tabs_flag=None,
                        new_project_id=None,
                        new_owner_id=None):
        """
        Updates the details of the specified workbook.
        :param string workbook_id: the workbook ID
        :param boolean show_tabs_flag: (optional) enables or disables showing tabs
        :param string new_project_id: (optional) the new project ID
        :param string new_owner_id: (optional) the new owner ID
        :return: HTTP response
        """
        self.active_request = UpdateWorkbookRequest(ts_connection=self,
                                                    show_tabs_flag=show_tabs_flag,
                                                    project_id=new_project_id,
                                                    owner_id=new_owner_id).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                update_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_workbook_connection(self,
                                   workbook_id,
                                   connection_id,
                                   server_address=None,
                                   port=None,
                                   connection_username=None,
                                   connection_password=None,
                                   embed_password_flag=None,
                                   parameter_dict=None):
        """
        Updates the specified connection for the specified workbook.
        :param string workbook_id: the workbook ID
        :param string connection_id: (optional) the connection ID
        :param string server_address: (optional) the connection's server address
        :param string port: (optional) the connection's server port
        :param string connection_username: (optional) the connection's username
        :param string connection_password: (optional) the connection's password
        :param boolean embed_password_flag: (optional) enables or disables embedding the connection's password
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        # fails to execute correctly on Tableau Server's side
        self.active_request = UpdateWorkbookConnectionRequest(ts_connection=self,
                                                              server_address=server_address,
                                                              port=port,
                                                              connection_username=connection_username,
                                                              connection_password=connection_password,
                                                              embed_password_flag=embed_password_flag).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                connection_id=connection_id,
                                                update_workbook_connection=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.8')
    def update_workbook_now(self, workbook_id, ):
        """
        Immediately executes extract refreshes for the specified workbook.
        :param string workbook_id: the workbook ID
        :return: HTTP response
        """
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                refresh_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_workbook(self, workbook_id):
        """
        Deletes the specified workbook.
        :param string workbook_id: the workbook ID
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                delete_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.6')
    def delete_tag_from_view(self,
                             view_id,
                             tag_name):
        """
        Deletes the named tag from the specified view.
        :param string view_id: the view ID
        :param string tag_name: the tag name to delete
        :return: HTTP response
        """
        self.active_endpoint = ViewEndpoint(ts_connection=self,
                                            view_id=view_id,
                                            tag_name=tag_name,
                                            delete_tag=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_tag_from_workbook(self, workbook_id, tag_name):
        """
        Deletes the named tag from the specified workbook.
        :param string workbook_id: the workbook ID
        :param string tag_name: the tag name to delete
        :return: HTTP response
        """
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                tag_name=tag_name,
                                                delete_tag=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # data sources

    @verify_api_method_exists('2.6')
    def add_tags_to_data_source(self,
                                datasource_id,
                                tags):
        """
        Adds one or more tags to the specified datasource.
        :param string datasource_id: the datasource ID
        :param list tags: a list of tags to add to the datasource
        :return: HTTP response
        """
        self.active_request = AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  add_tags=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.6')
    def delete_tag_from_data_source(self,
                                    datasource_id,
                                    tag_name):
        """
        Deletes a named tag from the specified datasource.
        :param string datasource_id: the datasource ID
        :param string tag_name: the named tag to delete
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id, tag_name=tag_name,
                                                  delete_tag=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_data_source(self, datasource_id):
        """
        Queries details for the specified datasource.
        :param string datasource_id: the datasource ID
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  query_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_signed_in
    @verify_api_method_exists('2.3')
    def query_data_sources(self, parameter_dict=None):
        """
        Queries details for all datasources on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, query_datasources=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_data_source_connections(self, datasource_id):
        """
        Queries details for the connections belonging to the specified datasource.
        :param string datasource_id: the datasource ID
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  query_datasource_connections=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def get_data_source_revisions(self,
                                  datasource_id,
                                  parameter_dict=None):
        """
        Queries revision details for the specified datasource.
        :param string datasource_id: the datasource ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  get_datasource_revisions=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def download_data_source(self,
                             datasource_id,
                             parameter_dict=None):
        """
        Downloads the specified datasource.
        :param string datasource_id: the datasource ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  download_datasource=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def download_data_source_revision(self,
                                      datasource_id,
                                      revision_number,
                                      parameter_dict=None):
        """
        Downloads the specified revision number for the specified datasource.
        :param string datasource_id: the datasource ID
        :param string revision_number: the revision number to be downloaded.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  revision_number=revision_number,
                                                  download_datasource_revision=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_data_source(self, datasource_id,
                           new_project_id=None,
                           new_owner_id=None,
                           is_certified_flag=None,
                           certification_note=None):
        """
        Updates details for the specified datasource.
        Note that assigning a new project ID to an embedded extract will not actually change the extract's project ID,
        even if the response indicates it has moved.
        :param string datasource_id: the datasource ID
        :param string new_project_id: (optional) the new project ID
        :param string new_owner_id: (optional) the new owner ID
        :param boolean is_certified_flag: (optional) notes whether or not the datasource is certified
        :param string certification_note: (optional) the datasource certification note
        :return: HTTP response
        """
        self.active_request = UpdateDatasourceRequest(ts_connection=self,
                                                      new_project_id=new_project_id,
                                                      new_owner_id=new_owner_id,
                                                      is_certified_flag=is_certified_flag,
                                                      certification_note=certification_note).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  update_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_data_source_connection(self,
                                      datasource_id,
                                      connection_id,
                                      server_address=None,
                                      port=None,
                                      connection_username=None,
                                      connection_password=None,
                                      embed_password_flag=None):
        """
        Updates details for the specified connection in the specified datasource.
        Note that you must set the connection_password='' if changing the embed_password_flag from True to False
        :param string datasource_id: the datasource ID
        :param string connection_id: the connection ID
        :param string server_address: (optional) the connection's server address
        :param string port: (optional) the connection's port
        :param string connection_username: (optional) the connection's username
        :param string connection_password: (optional) the connection's password
        :param boolean embed_password_flag: (optional) enables or disables embedding the connection's password
        :return: HTTP response
        """
        self.active_request = UpdateDatasourceConnectionRequest(ts_connection=self,
                                                                server_address=server_address,
                                                                port=port,
                                                                connection_username=connection_username,
                                                                connection_password=connection_password,
                                                                embed_password_flag=embed_password_flag).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  connection_id=connection_id,
                                                  update_datasource_connection=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.8')
    def update_data_source_now(self, datasource_id):
        """
        Immediately executes an extract refresh for the specified datasource.
        :param string datasource_id: the datasource ID
        :return: HTTP response
        """
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  refresh_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_data_source(self, datasource_id):
        """
        Deletes the specified datasource.
        :param string datasource_id: the datasource ID
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  delete_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def remove_data_source_revision(self,
                                    datasource_id,
                                    revision_number):
        """
        Deletes the specified revision number for the specified datasource.
        :param string datasource_id: the datasource ID
        :param string revision_number: the revision number to delete
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  revision_number=revision_number,
                                                  remove_datasource_revision=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # users and groups

    @verify_api_method_exists('2.3')
    def create_group(self,
                     new_group_name,
                     active_directory_group_name=None,
                     active_directory_domain_name=None,
                     default_site_role=None, parameter_dict=None):
        """
        Creates a group on the active site.
        :param string new_group_name: the group name
        :param string active_directory_group_name: (optional) the name of the active directory group to import
        :param string active_directory_domain_name: (optional) the domain of the active directory group to import
        :param string default_site_role: the default site role for users imported into the group via active directory
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_request = CreateGroupRequest(ts_connection=self,
                                                 new_group_name=new_group_name,
                                                 active_directory_group_name=active_directory_group_name,
                                                 active_directory_domain_name=active_directory_domain_name,
                                                 default_site_role=default_site_role).get_request()
        self.active_endpoint = GroupEndpoint(ts_connection=self, create_group=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_user_to_group(self,
                          group_id,
                          user_id):
        """
        Adds the specified user to the specified group.
        :param string group_id: the group ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_request = AddUserToGroupRequest(ts_connection=self, user_id=user_id).get_request()
        self.active_endpoint = GroupEndpoint(ts_connection=self, group_id=group_id, add_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_user_to_site(self,
                         user_name,
                         site_role,
                         auth_setting=None):
        """
        Adds a user to the active site.
        :param string user_name: the username assigned to the new user
        :param string site_role: the site role assigned to the new user
        :param string auth_setting: (optional) the authentication type for the new user [SAML, ServerDefault]
        :return: HTTP response
        """
        self.active_request = AddUserToSiteRequest(ts_connection=self,
                                                   user_name=user_name,
                                                   site_role=site_role,
                                                   auth_setting=auth_setting).get_request()
        self.active_endpoint = UserEndpoint(ts_connection=self, add_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.7')
    def get_groups_for_a_user(self,
                              user_id,
                              parameter_dict=None):
        """
        Gets a list of groups of which the specified user is a member.
        :param str user_id: the ID for the user whose group membership is being queried
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = UserEndpoint(ts_connection=self,
                                            user_id=user_id,
                                            query_groups_for_user=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def get_users_in_group(self,
                           group_id,
                           parameter_dict=None):
        """
        Queries details for all users within the specified group.
        :param string group_id: the group ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = GroupEndpoint(ts_connection=self,
                                             group_id=group_id,
                                             get_users=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def get_users_on_site(self, parameter_dict=None):
        """
        Queries details for all users on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = UserEndpoint(ts_connection=self,
                                            query_users=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_groups(self, parameter_dict=None):
        """
        Queries details for all groups on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = GroupEndpoint(ts_connection=self,
                                             query_groups=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_user_on_site(self, user_id):
        """
        Queries details for the specified user on the active site.
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = UserEndpoint(ts_connection=self,
                                            user_id=user_id,
                                            query_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_group(self,
                     group_id,
                     new_group_name=None,
                     active_directory_group_name=None,
                     active_directory_domain_name=None,
                     default_site_role=None,
                     parameter_dict=None):
        """
        Updates details for the specified group.
        :param string group_id: the group ID
        :param string new_group_name: (optional) the new group name
        :param string active_directory_group_name: (optional) the new active directory group name
        :param string active_directory_domain_name: (optional) the new active directory domain name
        :param string default_site_role: (optional) the new default site role
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_request = UpdateGroupRequest(ts_connection=self,
                                                 new_group_name=new_group_name,
                                                 active_directory_group_name=active_directory_group_name,
                                                 active_directory_domain_name=active_directory_domain_name,
                                                 default_site_role=default_site_role).get_request()
        self.active_endpoint = GroupEndpoint(ts_connection=self,
                                             group_id=group_id,
                                             update_group=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_user(self,
                    user_id,
                    new_full_name=None,
                    new_email=None,
                    new_password=None,
                    new_site_role=None,
                    new_auth_setting=None):
        """
        Updates details for the specified user.
        :param string user_id: the user ID
        :param string new_full_name: (optional) the new full name for the user
        :param string new_email: (optional) the new email address for the user
        :param string new_password: (optional) the new password for the user
        :param string new_site_role: (optional) the new site role for the user
        :param string new_auth_setting: (optional) the new auth setting for the user [SAML, ServerDefault]
        :return: HTTP response
        """
        self.active_request = UpdateUserRequest(ts_connection=self,
                                                new_full_name=new_full_name,
                                                new_email=new_email,
                                                new_password=new_password,
                                                new_site_role=new_site_role,
                                                new_auth_setting=new_auth_setting).get_request()
        self.active_endpoint = UserEndpoint(ts_connection=self, user_id=user_id, update_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.default_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def remove_user_from_group(self,
                               group_id,
                               user_id):
        """
        Removes the specified user from the specified group.
        :param string group_id: the group ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = GroupEndpoint(ts_connection=self,
                                             group_id=group_id,
                                             user_id=user_id,
                                             remove_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def remove_user_from_site(self, user_id):
        """
        Removes the specified user from the active site.
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = UserEndpoint(ts_connection=self,
                                            user_id=user_id,
                                            remove_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_group(self, group_id):
        """
        Deletes the specified group from the active site.
        :param string group_id: the group ID
        :return: HTTP response
        """
        self.active_endpoint = GroupEndpoint(ts_connection=self,
                                             group_id=group_id,
                                             delete_group=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # permissions

    @verify_api_method_exists('2.3')
    def add_data_source_permissions(self,
                                    datasource_id,
                                    user_capability_dict=None,
                                    group_capability_dict=None,
                                    user_id=None,
                                    group_id=None):
        """
        Adds permissions rules for the specified datasource.
        Note that you cannot add permissions for users and groups in a single call to this function.
        :param string datasource_id: the datasource ID
        :param dict user_capability_dict: permissions definitions for the specified user
        :param dict group_capability_dict: permissions definitions for the specified group
        :param string user_id: the user ID for the user whose permissions are being defined
        :param string group_id: the group ID for the group whose permissions are being defined
        :return: HTTP response
        """
        self.active_request = AddDatasourcePermissionsRequest(ts_connection=self,
                                                              datasource_id=datasource_id,
                                                              user_id=user_id,
                                                              group_id=group_id,
                                                              user_capability_dict=user_capability_dict,
                                                              group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='datasource',
                                                   object_id=datasource_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    def add_flow_permissions(self,
                             flow_id,
                             user_capability_dict=None,
                             group_capability_dict=None,
                             user_id=None,
                             group_id=None):
        """
        Adds permissions rules for the specified flow.
        Note that you cannot add permissions for users and groups in a single call to this function.
        :param flow_id: the flow ID
        :param user_capability_dict: permissions definitions for the specified user
        :param group_capability_dict: permissions definitions for the specified group
        :param user_id: the user ID for the user whose permissions are being defined
        :param group_id: the group ID for the group whose permissions are being defined
        :return: HTTP response
        """
        self.active_request = AddFlowPermissionsRequest(ts_connection=self,
                                                        user_id=user_id,
                                                        group_id=group_id,
                                                        user_capability_dict=user_capability_dict,
                                                        group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='flow',
                                                   object_id=flow_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_project_permissions(self,
                                project_id,
                                user_capability_dict=None,
                                group_capability_dict=None,
                                user_id=None,
                                group_id=None):
        """
        Adds permissions rules for the specified project.
        Note that you cannot add permissions for users and groups in a single call to this function.
        :param string project_id: the project ID
        :param dict user_capability_dict: permissions definitions for the specified user
        :param dict group_capability_dict: permissions definitions for the specified group
        :param string user_id: the user ID for the user whose permissions are being defined
        :param string group_id: the group ID for the group whose permissions are being defined
        :return: HTTP response
        """
        self.active_request = AddProjectPermissionsRequest(ts_connection=self,
                                                           user_id=user_id,
                                                           group_id=group_id,
                                                           user_capability_dict=user_capability_dict,
                                                           group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='project',
                                                   object_id=project_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_default_permissions(self,
                                project_id,
                                project_permissions_object,
                                group_id=None,
                                user_id=None,
                                user_capability_dict=None,
                                group_capability_dict=None):
        """
        Adds default permissions rules to the specified project.
        Note that you cannot add permissions for users and groups in a single call to this function.
        :param string project_id: the project ID
        :param string project_permissions_object: the object type to add default permissions for; one of the following
        [workbook, datasource, flow]
        :param string group_id: the group ID for the group whose permissions are being defined
        :param string user_id: the user ID for the user whose permissions are being defined
        :param dict user_capability_dict: permissions definitions for the specified user
        :param dict group_capability_dict: permissions definitions for the specified group
        :return: HTTP response
        """
        self.active_request = AddDefaultPermissionsRequest(ts_connection=self,
                                                           group_id=group_id,
                                                           user_id=user_id,
                                                           group_capability_dict=group_capability_dict,
                                                           user_capability_dict=user_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   project_id=project_id,
                                                   project_permissions_object=project_permissions_object,
                                                   add_default_project_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def add_view_permissions(self,
                             view_id,
                             user_capability_dict=None,
                             group_capability_dict=None,
                             user_id=None,
                             group_id=None):
        """
        Adds permissions rules for the specified view.
        Note that you cannot add permissions for users and groups in a single call to this function.
        :param string view_id: the view ID
        :param dict user_capability_dict: permissions definitions for the specified user
        :param dict group_capability_dict: permissions definitions for the specified group
        :param string user_id: the user ID for the user whose permissions are being defined
        :param string group_id: the group ID for the group whose permissions are being defined
        :return: HTTP response
        """
        self.active_request = AddViewPermissionsRequest(ts_connection=self,
                                                        view_id=view_id,
                                                        user_id=user_id,
                                                        group_id=group_id,
                                                        user_capability_dict=user_capability_dict,
                                                        group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='view', object_id=view_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_workbook_permissions(self,
                                 workbook_id,
                                 user_capability_dict=None,
                                 group_capability_dict=None,
                                 user_id=None,
                                 group_id=None):
        """
        Adds permissions rules for the specified view.
        Note that you cannot add permissions for users and groups in a single call to this function.
        :param string workbook_id: the workbook ID
        :param dict user_capability_dict: permissions definitions for the specified user
        :param dict group_capability_dict: permissions definitions for the specified group
        :param string user_id: the user ID for the user whose permissions are being defined
        :param string group_id: the group ID for the group whose permissions are being defined
        :return: HTTP response
        """
        self.active_request = AddWorkbookPermissionsRequest(ts_connection=self,
                                                            workbook_id=workbook_id,
                                                            user_id=user_id,
                                                            group_id=group_id,
                                                            user_capability_dict=user_capability_dict,
                                                            group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='workbook', object_id=workbook_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers.copy()
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_data_source_permissions(self, datasource_id):
        """
        Queries permissions details for the specified datasource.
        :param string datasource_id: the datasource ID
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='datasource',
                                                   object_id=datasource_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_flow_permissions(self, flow_id):
        """
        Queries permissions details for the specified flow.
        :param string flow_id: the flow ID
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='flow',
                                                   object_id=flow_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_project_permissions(self, project_id):
        """
        Queries permissions details for the specified project.
        :param string project_id: the project ID
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='project',
                                                   object_id=project_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_default_permissions(self,
                                  project_id,
                                  project_permissions_object):
        """
        Queries permissions details for the specified object variety within the specified project.
        :param string project_id: the project ID
        :param string project_permissions_object: the object variety [workbook, datasource, flow]
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   project_id=project_id,
                                                   project_permissions_object=project_permissions_object,
                                                   query_default_project_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def query_view_permissions(self, view_id):
        """
        Queries permissions details for the specified view.
        :param string view_id: the view ID
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='view',
                                                   object_id=view_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_workbook_permissions(self, workbook_id):
        """
        Query permissions details for the specified workbook.
        :param string workbook_id: the workbook ID
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='workbook',
                                                   object_id=workbook_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_data_source_permission(self,
                                      datasource_id,
                                      delete_permissions_object,
                                      delete_permissions_object_id,
                                      capability_name, capability_mode):
        """
        Deletes the specified permission for the specified datasource.
        :param string datasource_id: the datasource ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='datasource',
                                                   object_id=datasource_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    def delete_flow_permission(self,
                               flow_id,
                               delete_permissions_object,
                               delete_permissions_object_id,
                               capability_name,
                               capability_mode):
        """
        Deletes the specified permission for the specified flow.
        :param string flow_id: the flow ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='flow',
                                                   object_id=flow_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_project_permission(self,
                                  project_id,
                                  delete_permissions_object,
                                  delete_permissions_object_id,
                                  capability_name, capability_mode):
        """
        Deletes the specified permission for the specified project.
        :param string project_id: the project ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='project',
                                                   object_id=project_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_default_permission(self,
                                  project_id,
                                  project_permissions_object,
                                  delete_permissions_object,
                                  delete_permissions_object_id,
                                  capability_name,
                                  capability_mode):
        """
        Deletes the specified default permission for the specified object within the specified project.
        :param string project_id: the project ID
        :param string project_permissions_object: one of the following [workbooks, datasources, or flows]
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   project_id=project_id,
                                                   project_permissions_object=project_permissions_object,
                                                   delete_default_project_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.2')
    def delete_view_permission(self,
                               view_id,
                               delete_permissions_object,
                               delete_permissions_object_id,
                               capability_name,
                               capability_mode):
        """
        Deletes the specified permission for the specified view.
        :param string view_id: the view ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='view',
                                                   object_id=view_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_workbook_permission(self, workbook_id, delete_permissions_object, delete_permissions_object_id,
                                   capability_name, capability_mode):
        """
        Deletes the specified permission for the specified workbook.
        :param string workbook_id: the workbook ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='workbook',
                                                   object_id=workbook_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # jobs, tasks, and schedules

    @verify_api_method_exists('2.8')
    def add_data_source_to_schedule(self,
                                    datasource_id,
                                    schedule_id):
        """
        Adds the specified datasource to the specified schedule.
        :param string datasource_id: the datasource ID
        :param string schedule_id: the schedule ID
        :return: HTTP response
        """
        self.active_request = AddDatasourceToScheduleRequest(ts_connection=self,
                                                             datasource_id=datasource_id).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self,
                                                 schedule_id=schedule_id,
                                                 add_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    def add_flow_task_to_schedule(self,
                                  flow_id,
                                  schedule_id):
        """
        Adds the specified flow task to the specified schedule.
        :param string flow_id: the flow ID
        :param string schedule_id: the schedule ID
        :return: HTTP response
        """
        self.active_request = AddFlowToScheduleRequest(ts_connection=self,
                                                       flow_id=flow_id).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self,
                                                 schedule_id=schedule_id,
                                                 add_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.8')
    def add_workbook_to_schedule(self,
                                 workbook_id,
                                 schedule_id):
        """
        Adds the specified workbook to the specified schedule.
        :param string workbook_id: the workbook ID
        :param string schedule_id: the schedule ID
        :return: HTTP response
        """
        self.active_request = AddWorkbookToScheduleRequest(ts_connection=self, workbook_id=workbook_id).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self,
                                                 schedule_id=schedule_id,
                                                 add_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.1')
    def cancel_job(self, job_id):
        """
        Cancels the specified job.
        :param string job_id: the job ID
        :return: HTTP response
        """
        self.active_endpoint = JobsEndpoint(ts_connection=self, job_id=job_id, cancel_job=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_job(self, job_id):
        """
        Queries the specified job.
        :param string job_id: the job ID
        :return: HTTP response
        """
        self.active_endpoint = JobsEndpoint(ts_connection=self, job_id=job_id, query_job=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.1')
    def query_jobs(self, parameter_dict=None):
        """
        Queries details for all jobs on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = JobsEndpoint(ts_connection=self, query_jobs=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.6')
    def get_extract_refresh_task(self, task_id):
        """
        Query details for the specified extract refresh task.
        :param string task_id: the extract refresh task ID
        :return: HTTP response
        """
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, get_refresh_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def get_extract_refresh_tasks_for_site(self):
        """
        Query details for all extract refresh tasks on the active site.
        :return: HTTP response
        """
        self.active_endpoint = TasksEndpoint(ts_connection=self, get_refresh_tasks=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    def get_extract_refresh_tasks_for_schedule(self, schedule_id):
        """
        Queries details for all extract refresh tasks belonging to the specified schedule.
        R
        :param string schedule_id: the schedule ID
        :return: HTTP response
        """
        self.active_endpoint = SchedulesEndpoint(ts_connection=self,
                                                 schedule_id=schedule_id,
                                                 query_extract_schedules=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    def get_flow_run_task(self, task_id):
        """
        Queries details for the specified flow run task.
        :param string task_id: the flow run task ID
        :return: HTTP response
        """
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, get_flow_run_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    def get_flow_run_tasks(self):
        """
        Queries details for all flow run tasks on the active site.
        :return: HTTP response
        """
        self.active_endpoint = TasksEndpoint(ts_connection=self, get_flow_run_tasks=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def create_schedule(self,
                        schedule_name,
                        schedule_priority=50,
                        schedule_type='Extract',
                        schedule_execution_order='Parallel',
                        schedule_frequency='Weekly',
                        start_time='07:00:00',
                        end_time='23:00:00',
                        interval_expression_list=[{'weekDay': 'Monday'}]):
        """
        Creates a new schedule for the server.
        :param string schedule_name: the new schedule's name
        :param string schedule_priority: the new shcedule's execution priority value [1-100]
        :param string schedule_type: the new schedule type [Flow, Extract, or Subscription]
        :param string schedule_execution_order: the new schedule execution order [Parallel or Serial]
        :param string schedule_frequency: the new schedule's frequency [Hourly, Daily, Weekly, or Monthly]
        :param string start_time: the new schedule's start time [HH:MM:SS]
        :param string end_time: the new schedule's end time [HH:MM:SS]
        :param list interval_expression_list: schedule interval details, please see Tableau's REST API documentation.
        :return: HTTP response
        """
        self.active_request = CreateScheduleRequest(ts_connection=self,
                                                    schedule_name=schedule_name,
                                                    schedule_priority=schedule_priority,
                                                    schedule_type=schedule_type,
                                                    schedule_execution_order=schedule_execution_order,
                                                    schedule_frequency=schedule_frequency,
                                                    start_time=start_time, end_time=end_time,
                                                    interval_expression_list=interval_expression_list).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, create_schedule=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    def query_extract_refresh_tasks_for_schedule(self,
                                                 schedule_id,
                                                 parameter_dict=None):
        """
        Queries details for all extract refresh tasks belonging to the specified schedule.
        Requires API version 3.6 or higher.
        :param string schedule_id: the schedule ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = TasksEndpoint(ts_connection=self,
                                             query_schedule_refresh_tasks=True,
                                             schedule_id=schedule_id,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_schedules(self, parameter_dict=None):
        """
        Queries details for all schedules on the server.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = SchedulesEndpoint(ts_connection=self,
                                                 query_schedules=True,
                                                 parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.6')
    def run_extract_refresh_task(self, task_id):
        """
        Runs the specified extract refresh task.
        Note that this task must belong to a schedule, and this will execute with the task's default priority value.
        :param string task_id: the extract refresh task ID
        :return: HTTP response
        """
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, run_refresh_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    def run_flow_task(self, task_id):
        """
        Runs the specified flow run task.
        Note that this task must belong to a schedule.
        :param string task_id: the flow run task ID
        :return: HTTP response
        """
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, run_flow_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_schedule(self,
                        schedule_id,
                        schedule_name=None,
                        schedule_priority=None,
                        schedule_type=None,
                        schedule_state=None,
                        schedule_execution_order=None,
                        schedule_frequency=None,
                        start_time=None,
                        end_time=None,
                        interval_expression_list=None):
        """
        Updates details for the specified schedule.
        :param string schedule_id: the schedule ID
        :param string schedule_name: the new schedule's name
        :param string schedule_priority: the new shcedule's execution priority value [1-100]
        :param string schedule_type: the new schedule type [Flow, Extract, or Subscription]
        :param string schedule_state: the new schedule state [Active,
        :param string schedule_execution_order: the new schedule execution order [Parallel or Serial]
        :param string schedule_frequency: the new schedule's frequency [Hourly, Daily, Weekly, or Monthly]
        :param string start_time: the new schedule's start time [HH:MM:SS]
        :param string end_time: the new schedule's end time [HH:MM:SS]
        :param list interval_expression_list: list of interval expression key-value dicts,
        please see Tableau's REST API documentation for details on the valid interval expressions.
        :return: HTTP response
        """
        self.active_request = UpdateScheduleRequest(ts_connection=self, schedule_name=schedule_name,
                                                    schedule_priority=schedule_priority,
                                                    schedule_type=schedule_type,
                                                    schedule_state=schedule_state,
                                                    schedule_execution_order=schedule_execution_order,
                                                    schedule_frequency=schedule_frequency,
                                                    start_time=start_time,
                                                    end_time=end_time,
                                                    interval_expression_list=interval_expression_list).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self,
                                                 schedule_id=schedule_id,
                                                 update_schedule=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_schedule(self, schedule_id):
        """
        Deletes the specified schedule.
        :param string schedule_id: the schedule ID
        :return: HTTP response
        """
        self.active_endpoint = SchedulesEndpoint(ts_connection=self,
                                                 schedule_id=schedule_id,
                                                 delete_schedule=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # subscriptions

    @verify_api_method_exists('2.3')
    def create_subscription(self,
                            subscription_subject,
                            content_type,
                            content_id,
                            schedule_id,
                            user_id,
                            attach_image_flag=False,
                            attach_pdf_flag=False):
        """
        Creates a new subscription for the specified user to receive the specified content on the specified schedule.
        :param string subscription_subject: the subject for the new subscription.
        :param string content_type: the content type for the new subscription [Workbook or View]
        :param string content_id: the content ID [workbook ID or view ID]
        :param string schedule_id: the schedule ID the subscription will be executed on
        :param string user_id: the user ID for the user being subscribed to the content
        :param bool attach_image_flag: True if an image will be attached to the subscription email, False otherwise
        :param bool attach_pdf_flag: True if a PDF will be attached to the subscription email, False otherwise
        :return: HTTP response
        """
        self.active_request = CreateSubscriptionRequest(ts_connection=self,
                                                        subscription_subject=subscription_subject,
                                                        content_type=content_type,
                                                        content_id=content_id,
                                                        schedule_id=schedule_id,
                                                        user_id=user_id,
                                                        attach_image_flag=attach_image_flag,
                                                        attach_pdf_flag=attach_pdf_flag).get_request()
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self, create_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_subscription(self, subscription_id):
        """
        Queries details for the specified subscription.
        :param string subscription_id: the subscription ID
        :return: HTTP response
        """
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self,
                                                     subscription_id=subscription_id,
                                                     query_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def query_subscriptions(self, parameter_dict=None):
        """
        Queries details for all subscriptions on the site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self,
                                                     query_subscriptions=True,
                                                     parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def update_subscription(self,
                            subscription_id,
                            new_subscription_subject=None,
                            new_schedule_id=None):
        """
        Updates details for the specified subscription.
        :param string subscription_id: the subscription ID
        :param string new_subscription_subject: (optional) the new subscription subject
        :param string new_schedule_id: (optional) the new schedule ID for the subscription
        :return: HTTP response
        """
        self.active_request = UpdateSubscriptionRequest(ts_connection=self,
                                                        new_schedule_id=new_schedule_id,
                                                        new_subscription_subject=new_subscription_subject).get_request()
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self,
                                                     subscription_id=subscription_id,
                                                     update_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_subscription(self, subscription_id):
        """
        Deletes the specified subscription.
        :param string subscription_id: the subscription ID
        :return: HTTP response
        """
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self,
                                                     subscription_id=subscription_id,
                                                     delete_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # favorites

    @verify_api_method_exists('2.3')
    def add_data_source_to_favorites(self,
                                     datasource_id,
                                     user_id,
                                     favorite_label):
        """
        Adds the specified datasource to the favorites for the specified user.
        :param string datasource_id: the datasource ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the datasource being added as a favorite
        :return: HTTP response
        """
        self.active_request = AddDatasourceToFavoritesRequest(ts_connection=self,
                                                              datasource_id=datasource_id,
                                                              favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.1')
    def add_project_to_favorites(self,
                                 project_id,
                                 user_id,
                                 favorite_label):
        """
        Adds the specified project to the favorites for the specified user.
        :param string project_id: the project ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the project being added as a favorite
        :return: HTTP response
        """
        self.active_request = AddProjectToFavoritesRequest(ts_connection=self, project_id=project_id,
                                                           favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_view_to_favorites(self,
                              view_id,
                              user_id,
                              favorite_label):
        """
        Adds the specified view to the favorites for the specified user.
        :param string view_id: the view ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the view being added as a favorite
        :return: HTTP response
        """
        self.active_request = AddViewToFavoritesRequest(ts_connection=self,
                                                        view_id=view_id,
                                                        favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def add_workbook_to_favorites(self,
                                  workbook_id,
                                  user_id,
                                  favorite_label):
        """
        Adds the specified workbook to the favorites for the specified user.
        :param string workbook_id: the workbook ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the workbook being added as a favorite
        :return: HTTP response
        """
        self.active_request = AddWorkbookToFavoritesRequest(ts_connection=self,
                                                            workbook_id=workbook_id,
                                                            favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_data_source_from_favorites(self,
                                          datasource_id,
                                          user_id):
        """
        Deletes the specified datasource from the specified user's favorites list.
        :param string datasource_id: the datasource ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 object_type='datasource',
                                                 object_id=datasource_id,
                                                 user_id=user_id,
                                                 delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.1')
    def delete_project_from_favorites(self,
                                      project_id,
                                      user_id):
        """
        Deletes the specified project from the specified user's favorites list.
        :param string project_id: the project ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 object_type='project',
                                                 object_id=project_id,
                                                 user_id=user_id,
                                                 delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_view_from_favorites(self,
                                   view_id,
                                   user_id):
        """
        Deletes the specified view from the specified user's favorites list.
        :param string view_id: the view ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 object_type='view',
                                                 object_id=view_id,
                                                 user_id=user_id,
                                                 delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def delete_workbook_from_favorites(self,
                                       workbook_id,
                                       user_id):
        """
        Deletes the specified workbook from the specified user's favorites list.
        :param string workbook_id: the workbook ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 object_type='workbook',
                                                 object_id=workbook_id,
                                                 user_id=user_id,
                                                 delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.5')
    def get_favorites_for_user(self, user_id):
        """
        Queries the favorite items for a specified user.
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = FavoritesEndpoint(ts_connection=self,
                                                 get_user_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # publishing

    @verify_api_method_exists('2.3')
    def initiate_file_upload(self):
        """
        Initiates a file upload session with Tableau Server.
        :return: HTTP response
        """
        self.active_endpoint = FileUploadEndpoint(ts_connection=self, initiate_file_upload=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def append_to_file_upload(self,
                              upload_session_id,
                              payload,
                              content_type):
        """
        Appends file data to an existing file upload session.
        :param string upload_session_id: the upload session ID
        :param payload: the payload
        :param string content_type: the content type header
        :return: HTTP response
        """
        self.active_endpoint = FileUploadEndpoint(ts_connection=self,
                                                  append_to_file_upload=True,
                                                  upload_session_id=upload_session_id).get_endpoint()
        self.active_headers = self.default_headers.copy()
        self.active_headers.update({'content-type': content_type})
        response = requests.put(url=self.active_endpoint, data=payload, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def publish_data_source(self,
                            datasource_file_path,
                            datasource_name,
                            project_id,
                            connection_username=None,
                            connection_password=None,
                            embed_credentials_flag=False,
                            oauth_flag=False,
                            parameter_dict=None):
        """
        Publishes a datasource file to Tableau Server.
        :param string datasource_file_path: the path to the datasource file
        :param string datasource_name: the desired name for the datasource
        :param string project_id: the project ID where the file will be published
        :param string connection_username: the username for the datasource's connection
        :param string connection_password: the password for the datasource's connection
        :param boolean embed_credentials_flag: enables or disables embedding the connection's password
        :param boolean oauth_flag: enables or disables OAuth authentication
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        publish_request = PublishDatasourceRequest(ts_connection=self,
                                                   datasource_name=datasource_name,
                                                   datasource_file_path=datasource_file_path,
                                                   project_id=project_id,
                                                   connection_username=connection_username,
                                                   connection_password=connection_password,
                                                   embed_credentials_flag=embed_credentials_flag,
                                                   oauth_flag=oauth_flag)
        self.active_request, content_type = publish_request.get_request()
        self.active_headers, parameter_dict = publish_request.publish_prep(content_type, parameter_dict=parameter_dict)
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  publish_datasource=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        response = requests.post(url=self.active_endpoint, data=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('2.3')
    def publish_workbook(self,
                         workbook_file_path,
                         workbook_name,
                         project_id,
                         show_tabs_flag=False,
                         user_id=None,
                             server_address=None,
                         port_number=None,
                         connection_username=None,
                         connection_password=None,
                         embed_credentials_flag=False,
                         oauth_flag=False,
                         workbook_views_to_hide=None,
                         hide_view_flag=False,
                         parameter_dict=None):
        """
        Publishes a workbook file to Tableau Server.
        :param string workbook_file_path: the path to the workbook file
        :param string workbook_name: the desired name for the published workbook
        :param string project_id: the project ID where the workbook will be published
        :param boolean show_tabs_flag: enables or disables showing tabs
        :param string user_id: the user ID for the user who owns the workbook
        :param list or str server_address: the connection's server address
        :param list or str port_number: the connection's port number
        :param list or str connection_username: the connection's username
        :param list or str connection_password: the connection's password
        :param list or bool embed_credentials_flag: enables or disables embedding the connection's password
        :param list or bool oauth_flag: enables or disables OAuth authentication
        :param list workbook_views_to_hide: a list of workbook views to be hidden when published
        :param boolean hide_view_flag: enables or disables hiding workbook views
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        publish_request = PublishWorkbookRequest(ts_connection=self,
                                                 workbook_name=workbook_name,
                                                 workbook_file_path=workbook_file_path,
                                                 project_id=project_id,
                                                 show_tabs_flag=show_tabs_flag,
                                                 user_id=user_id,
                                                 server_address=server_address,
                                                 port_number=port_number,
                                                 connection_username=connection_username,
                                                 connection_password=connection_password,
                                                 embed_credentials_flag=embed_credentials_flag,
                                                 oauth_flag=oauth_flag,
                                                 workbook_views_to_hide=workbook_views_to_hide,
                                                 hide_view_flag=hide_view_flag)
        self.active_request, content_type = publish_request.get_request()
        self.active_headers, parameter_dict = publish_request.publish_prep(content_type, parameter_dict=parameter_dict)
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                publish_workbook=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        response = requests.post(url=self.active_endpoint, data=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.3')
    def publish_flow(self,
                     flow_file_path,
                     flow_name,
                     project_id,
                     flow_description=None,
                     server_address=None,
                     port_number=None,
                     connection_username=None,
                     connection_password=None,
                     embed_credentials_flag=False,
                     oauth_flag=False,
                     parameter_dict=None):
        """
        Publishes a flow file to Tableau Server.
        :param str flow_file_path: the path to the flow file
        :param str flow_name: the desired name for the published flow
        :param str project_id: the project ID where the flow will be published
        :param str flow_description: the description for the published flow
        :param list or str server_address: the connection's server address
        :param list or str port_number: the connection's port
        :param list or str connection_username: the connection's username
        :param list or str connection_password: the connection's password
        :param list or bool embed_credentials_flag: enables or disables embedding the connection's password
        :param list or bool oauth_flag: enables or disables OAuth authentication
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        publish_request = PublishFlowRequest(ts_connection=self,
                                             flow_file_path=flow_file_path,
                                             flow_name=flow_name,
                                             project_id=project_id,
                                             flow_description=flow_description,
                                             server_address=server_address,
                                             port_number=port_number,
                                             connection_username=connection_username,
                                             connection_password=connection_password,
                                             embed_credentials_flag=embed_credentials_flag,
                                             oauth_flag=oauth_flag)
        self.active_request, content_type = publish_request.get_request()
        self.active_headers, parameter_dict = publish_request.publish_prep(content_type, parameter_dict=parameter_dict)
        self.active_endpoint = FlowEndpoint(ts_connection=self, publish_flow=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        response = requests.post(url=self.active_endpoint, data=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    # metadata methods

    @verify_api_method_exists('3.5')
    def query_database(self, database_id):
        """
        Query details for the specified database.
        :param str database_id: the database ID
        :return: HTTP response
        """
        self.active_endpoint = DatabaseEndpoint(self, query_database=True, database_id=database_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def query_databases(self):
        """
        Queries details for databases stored on Tableau Server.
        :return: HTTP response
        """
        self.active_endpoint = DatabaseEndpoint(self, query_databases=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def update_database(self,
                        database_id,
                        certification_status=None,
                        certification_note=None,
                        new_description_value=None,
                        new_contact_id=None):
        """
        Updates the details for the specified database.
        :param str database_id: the database ID
        :param bool certification_status: certifies (True) or removes certification (False) for the specified database
        :param str certification_note: custom text to accompany the certification status
        :param str new_description_value: custom text describing the database
        :param str new_contact_id: the ID for the Tableau Server user who is the contact for the specified database
        :return: HTTP response
        """
        self.active_request = UpdateDatabaseRequest(self,
                                                    certification_status=certification_status,
                                                    certification_note=certification_note,
                                                    new_description_value=new_description_value,
                                                    new_contact_id=new_contact_id).get_request()
        self.active_endpoint = DatabaseEndpoint(self, database_id=database_id, update_database=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def remove_database(self, database_id):
        """
        Removes the database asset.
        :param str database_id: the database ID
        :return: HTTP response
        """
        self.active_endpoint = DatabaseEndpoint(self, database_id=database_id, remove_database=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def query_table(self, table_id):
        """
        Queries details for the specified database table.
        :param str table_id: the table ID
        :return: HTTP response
        """
        self.active_endpoint = TableEndpoint(self, table_id=table_id, query_table=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def query_tables(self):
        """
        Queries details for all tables on the active site.
        :return: HTTP response
        """
        self.active_endpoint = TableEndpoint(self, query_tables=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def update_table(self,
                     table_id,
                     certification_status=None,
                     certification_note=None,
                     new_description_value=None,
                     new_contact_id=None):
        """
        Updates details for the specified database table.
        :param str table_id: the table ID
        :param bool certification_status: certifies (True) or removes certification (False) for the specified table
        :param str certification_note: custom text to accompany the certification status
        :param str new_description_value: custom text describing the table
        :param str new_contact_id: the ID for the Tableau Server user who is the contact for the specified database
        :return: HTTP response
        """
        self.active_request = UpdateTableRequest(self,
                                                 certification_status=certification_status,
                                                 certification_note=certification_note,
                                                 new_description_value=new_description_value,
                                                 new_contact_id=new_contact_id).get_request()
        self.active_endpoint = TableEndpoint(self, table_id=table_id, update_table=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def remove_table(self, table_id):
        """
        Removes the database table asset.
        :param str table_id:
        :return: HTTP response
        """
        self.active_endpoint = TableEndpoint(self, table_id=table_id, remove_table=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def query_table_column(self, table_id, column_id):
        """
        Queries details for the specified column in the specified database table.
        :param str table_id: the database table ID
        :param str column_id: the column ID
        :return: HTTP response
        """
        self.active_endpoint = ColumnEndpoint(self,
                                              table_id=table_id,
                                              column_id=column_id,
                                              query_column=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def query_table_columns(self, table_id):
        """
        Queries details for all columns in the specified database table.
        :param str table_id: the database table ID
        :return: HTTP response
        """
        self.active_endpoint = ColumnEndpoint(self, table_id=table_id, query_columns=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def update_column(self, table_id, column_id, new_description_value=None):
        """
        Updates details for the specified column in the specified database table.
        :param str table_id: the database table ID
        :param str column_id: the column ID
        :param str new_description_value: custom text describing the column
        :return: HTTP response
        """
        self.active_request = UpdateColumnRequest(self, new_description_value=new_description_value).get_request()
        self.active_endpoint = ColumnEndpoint(self,
                                              table_id=table_id,
                                              column_id=column_id,
                                              update_column=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def remove_column(self, table_id, column_id):
        """
        Removes the specified column asset.
        :param str table_id: the database table ID
        :param str column_id: the column ID
        :return: HTTP response
        """
        self.active_endpoint = ColumnEndpoint(self,
                                              table_id=table_id,
                                              column_id=column_id,
                                              remove_column=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def add_data_quality_warning(self,
                                 content_type,
                                 content_id,
                                 warning_type,
                                 message,
                                 status=None):
        """
        Adds a data quality warning to the specified content on Tableau Server.
        :param str content_type: the content type receiving the data quality warning
        [datasource, table, flow, or database]
        :param str content_id: the content ID for the specific content receiving the data quality warning
        :param str warning_type: the type of data quality warning
        [Deprecated, Warning, Stale data, or Under maintenance]
        :param str message: (optional) custom text accompanying the data quality warning
        :param bool status: toggles the data quality warning on (True) or off (False)
        :return: HTTP response
        """
        self.active_request = AddDQWarningRequest(self,
                                                  warning_type=warning_type,
                                                  message=message,
                                                  status=status).get_request()
        self.active_endpoint = DQWarningEndpoint(self,
                                                 content_type=content_type,
                                                 content_id=content_id,
                                                 add_warning=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def query_data_quality_warning_by_id(self, warning_id):
        """
        Queries details for the specified data quality warning, identified by its ID
        :param str warning_id: the data quality warning ID
        :return: HTTP response
        """
        self.active_endpoint = DQWarningEndpoint(self, warning_id=warning_id, query_by_id=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    def query_data_quality_warning_by_asset(self, content_type, content_id):
        """
        Queries details for the data quality warning belonging to a specific piece of content on Tableau Server.
        :param str content_type: the content type receiving the data quality warning
        [datasource, table, flow, or database]
        :param str content_id: the content ID for the specific content receiving the data quality warning
        :return: HTTP response
        """
        self.active_endpoint = DQWarningEndpoint(self,
                                                 content_type=content_type,
                                                 content_id=content_id,
                                                 query_by_content=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def update_data_quality_warning(self,
                                    warning_id,
                                    warning_type=None,
                                    message=None,
                                    status=None):
        """
        Updates details for the specified data quality warning.
        :param str warning_id: the data quality warning ID
        :param str warning_type: the type of data quality warning
        [Deprecated, Warning, Stale data, or Under maintenance]
        :param str message: (optional) custom text accompanying the data quality warning
        :param bool status: toggles the data quality warning on (True) or off (False)
        :return: HTTP response
        """
        self.active_request = UpdateDQWarningRequest(self,
                                                     warning_type=warning_type,
                                                     message=message,
                                                     status=status).get_request()
        self.active_endpoint = DQWarningEndpoint(self, warning_id=warning_id, update_warning=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def delete_data_quality_warning_by_id(self, warning_id):
        """
        Removes the data quality warning from Tableau Server.
        :param str warning_id: the data quality warning ID
        :return: HTTP response
        """
        self.active_endpoint = DQWarningEndpoint(self,
                                                 warning_id=warning_id,
                                                 delete_by_id=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def delete_data_quality_warning_by_content(self, content_type, content_id):
        """
        Removes the data quality warning from the specified piece of content on Tableau Server.
        :param str content_type: the content type receiving the data quality warning
        [datasource, table, flow, or database]
        :param str content_id: the content ID for the specific content receiving the data quality warning
        :return: HTTP response
        """
        self.active_endpoint = DQWarningEndpoint(self,
                                                 content_type=content_type,
                                                 content_id=content_id,
                                                 delete_by_content=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def metadata_graphql_query(self, query):
        """
        Builds a GraphQL query to run against the Metadata API.
        :param str query: the GraphQL query body (raw text)
        :return: HTTP response
        """
        self.active_request = GraphqlRequest(self, query).get_request()
        self.active_endpoint = GraphqlEndpoint(self).get_endpoint()
        self.active_headers = self.graphql_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    # encryption methods

    @verify_api_method_exists('3.5')
    def encrypt_extracts(self):
        """
        Encrypts all extracts on the active site (encrypts .hyper extracts at rest).
        :return: HTTP response
        """
        self.active_endpoint = EncryptionEndpoint(self, encrypt_extracts=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def decrypt_extracts(self):
        """
        Decrypts all extracts on the active site (decrypts .hyper extracts).
        :return: HTTP response
        """
        self.active_endpoint = EncryptionEndpoint(self, decrypt_extracts=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def reencrypt_extracts(self):
        """
        Reencrypts all .hyper extracts on the active site with new encryption keys.
        :return: HTTP response
        """
        self.active_endpoint = EncryptionEndpoint(self, reencrypt_extracts=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    # extract methods

    @verify_api_method_exists('3.5')
    def create_extract_for_datasource(self, datasource_id, encryption_flag=False):
        """
        Creates an extract for the specified published datasource.
        :param str datasource_id: the ID of the datasource being converted into an extract
        :param bool encryption_flag: True if encrypting the new extract, False otherwise
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  encryption_flag=encryption_flag,
                                                  create_extract=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def delete_extract_from_datasource(self, datasource_id):
        """
        Deletes an extract for the specified published datasource.
        :param str datasource_id: the ID of the datasource being converted from an extract to a live connection
        :return: HTTP response
        """
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  delete_extract=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def create_extracts_for_workbook(self,
                                     workbook_id,
                                     encryption_flag=False,
                                     extract_all_datasources_flag=True,
                                     datasource_ids=None):
        """
        Creates extracts for all embedded datasources or a subset of specified embedded datasources.
        :param str workbook_id: the ID of the workbook whose embedded datasources are being converted
        :param bool encryption_flag: True if the new extracts will be encrypted, False otherwise
        :param bool extract_all_datasources_flag: True if extracting all datasources, False otherwise
        :param list datasource_ids: a list of datasource IDs if only converting a subset of datasources to extracts
        :return: HTTP response
        """
        self.active_request = CreateExtractsForWorkbookRequest(ts_connection=self,
                                                               extract_all_datasources_flag=extract_all_datasources_flag,
                                                               datasource_ids=datasource_ids).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                create_extracts=True,
                                                encryption_flag=encryption_flag).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.5')
    def delete_extracts_from_workbook(self, workbook_id):
        """
        Deletes all extracts from the workbook; the connections are converted from extract to live.
        :param str workbook_id: the ID of the workbook whose extracts will be deleted
        :return: HTTP response
        """
        self.active_request = {"datasources": {"includeAll": True}}
        self.active_endpoint = WorkbookEndpoint(ts_connection=self,
                                                workbook_id=workbook_id,
                                                delete_extracts=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    #  webhook methods

    @verify_api_method_exists('3.6')
    def create_webhook(self,
                       webhook_name=None,
                       webhook_source_api_event_name=None,
                       url=None):
        """
        Creates a new webhook for a site.
        :param str webhook_name: the name of the new webhook
        :param str webhook_source_api_event_name: the API event name for the source event
        :param str url: the destination URL for the webhook; must be https and have a valid certificate
        :return: HTTP response
        """
        self.active_request = CreateWebhookRequest(self,
                                                   webhook_name=webhook_name,
                                                   webhook_source_api_event_name=webhook_source_api_event_name,
                                                   http_request_method='POST',
                                                   url=url).get_request()
        self.active_endpoint = WebhookEndpoint(self, create_webhook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers,
                                 verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.6')
    def query_webhook(self, webhook_id):
        """
        Queries information for the specified webhook.
        :param str webhook_id: the ID of the webhook being queried
        :return: HTTP response
        """
        self.active_endpoint = WebhookEndpoint(self, webhook_id=webhook_id, query_webhook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.6')
    def query_webhooks(self):
        """
        Queries all webhooks for the active site.
        :return: HTTP response
        """
        self.active_endpoint = WebhookEndpoint(self, query_webhook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.6')
    def test_webhook(self, webhook_id):
        """
        Tests the specified webhook, sending a payload to the webhook's destination URL.
        :param str webhook_id: the ID of the webhook being tested
        :return: HTTP response
        """
        self.active_endpoint = WebhookEndpoint(self, webhook_id=webhook_id, test_webhook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response

    @verify_api_method_exists('3.6')
    def delete_webhook(self, webhook_id):
        """
        Deletes the specified webhook.
        :param str webhook_id: the ID of the webhook being deleted
        :return: HTTP response
        """
        self.active_endpoint = WebhookEndpoint(self, webhook_id=webhook_id, delete_webhook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        return response
