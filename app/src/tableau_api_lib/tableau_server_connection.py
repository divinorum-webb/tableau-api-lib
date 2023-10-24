from typing import Any, Dict, List, Optional, Union
from urllib import parse

import requests

from tableau_api_lib import api_endpoints, api_requests, decorators


class TableauServerConnection:
    def __init__(
        self,
        config_json: Dict[str, Dict[str, Any]],
        env: str = "tableau_prod",
        ssl_verify: bool = True,
        use_apparent_encoding: bool = False,
    ):
        """Initializes a connection to Tableau Server using the environment configuration details provided.

        Args:
            config_json: A dict (ie: a Python JSON-like format) object containing Tableau Server configuration details.
            env: (optional) The environment within the `config_json` object that will be used.
            ssl_verify: (optional) True if using and verifying SSL certificates for HTTP requests; set to False if using HTTP.
            use_apparent_encoding: (optional) When this value is True then responses from the Server are encoded using the apparent format.
        """
        self._env = env
        self._config = config_json
        self._use_apparent_encoding = use_apparent_encoding
        self._auth_token = None
        self.ssl_verify = ssl_verify
        self.site_url = self._config.get(self._env, dict()).get("site_url")
        self.site_name = self._config.get(self._env, dict()).get("site_name")
        self.site_id = None
        self.user_id = None
        self.active_endpoint = None
        self.active_request = None
        self.active_headers = None
        self._validate_env()
        self.auth_method = self._get_auth_method()

    def _validate_env(self) -> None:
        """Raises an exception if the specified environment, "env", is not described in the configuration provided."""
        try:
            self._config[self._env]
        except KeyError:
            raise KeyError(f"The environment `{self._env}` is not an environment defined in the config provided.")

    @property
    def server(self) -> Union[str, None]:
        """Returns the server address for the TableauServerConnection credentials configuration."""
        server = self._config.get(self._env, dict()).get("server")
        server_has_scheme = parse.urlsplit(server).scheme
        if not server_has_scheme:
            raise ValueError(
                f"""
            The Tableau Server address provided is not valid.
            Server addresses must contain a scheme (ie: http, https). Try something like this instead:
            https://{server}
            http://{server}
            """
            )
        return server

    @property
    def api_version(self) -> Union[str, None]:
        """Returns the REST API version for the TableauServerConnection credentials configuration."""
        return self._config.get(self._env, dict()).get("api_version")

    @property
    def username(self) -> Union[str, None]:
        """Returns the username for the TableauServerConnection credentials configuration."""
        config_keys = self._config.get(self._env, dict()).keys()
        if "username" in config_keys and "password" in config_keys:
            username = self._config.get(self._env, dict()).get("username")
        else:
            username = None
        return username

    @property
    def password(self) -> Union[str, None]:
        """Returns the password for the TableauServerConnection credentials configuration."""
        config_keys = self._config.get(self._env, dict()).keys()
        if "password" in config_keys and "username" in config_keys:
            password = self._config.get(self._env, dict()).get("password")
        else:
            password = None
        return password

    @property
    def personal_access_token_name(self) -> Union[str, None]:
        """Returns the PAT name for the TableauServerConnection credentials configuration."""
        config_keys = self._config.get(self._env, dict()).keys()
        if "personal_access_token_name" in config_keys and "personal_access_token_secret" in config_keys:
            personal_access_token_name = self._config.get(self._env, dict()).get("personal_access_token_name")
        else:
            personal_access_token_name = None
        return personal_access_token_name

    @property
    def personal_access_token_secret(self) -> Union[str, None]:
        """Returns the PAT secret for the TableauServerConnection credentials configuration."""
        config_keys = self._config.get(self._env, dict()).keys()
        if "personal_access_token_name" in config_keys and "personal_access_token_secret" in config_keys:
            personal_access_token_secret = self._config.get(self._env, dict()).get("personal_access_token_secret")
        else:
            personal_access_token_secret = None
        return personal_access_token_secret

    @property
    def sign_in_headers(self) -> Dict[str, str]:
        """Returns headers that will be used to sign into the target Tableau Server."""
        return {"Content-Type": "application/json", "Accept": "application/json"}

    @property
    def x_auth_header(self) -> Dict[str, str]:
        """Returns the 'X-Tableau-Auth' header that is used to authenticate REST API calls after signing in."""
        return {"X-Tableau-Auth": self.auth_token}

    @property
    def default_headers(self) -> Dict[str, str]:
        """Returns a combination of the default (sign_in) headers and the authentication header for the REST API."""
        headers = self.sign_in_headers.copy()
        headers.update({"X-Tableau-Auth": self.auth_token})
        return headers

    @property
    def graphql_headers(self) -> Dict[str, str]:
        """Returns headers used when querying the Metadata API via the REST API."""
        headers = {"X-Tableau-Auth": self.auth_token}
        return headers

    @property
    def auth_token(self) -> Union[str, None]:
        return self._auth_token

    @auth_token.setter
    def auth_token(self, token_value: str) -> None:
        self._auth_token = token_value

    def _get_auth_method(self) -> str:
        """Returns the relevant key associated with the appropriate value for configuring authentication details."""
        if self.username and self.password:
            if not (self.personal_access_token_name or self.personal_access_token_secret):
                return "user_and_password"
        elif (self.personal_access_token_name and self.personal_access_token_secret) and not (
            self.username or self.password
        ):
            return "personal_access_token"
        raise ValueError(
            """
        The Tableau Server configuration provided contains username, password, and personal access token credentials.
        Successful authentication requires either username & password OR personal access token.
        Please update the configuration details to only provide details for one of the available authentication methods.
        """
        )

    def _set_response_encoding(self, response: requests.Response) -> requests.Response:
        """Returns the response with encoding modified if `apparent_encoding` is True for the connection instance."""
        if self._use_apparent_encoding:
            response.encoding = response.apparent_encoding
        return response

    @staticmethod
    def _set_local_vars(local_vars: Dict[str, Any]) -> Dict[str, Any]:
        """Returns a dict containing all local vars except for the `self` representing the class instance."""
        vars_to_remove = [var for var in ["self", "parameter_dict"] if var in local_vars.keys()]
        for var in vars_to_remove:
            del local_vars[var]
        return local_vars

    # authentication

    @decorators.verify_api_method_exists("3.11")
    def revoke_administrator_personal_access_tokens(self):
        """Revokes all personal access tokens belonging to administrators on the Tableau Server."""
        self.active_endpoint = api_endpoints.AuthEndpoint(ts_connection=self, revoke_admin_pat=True).get_endpoint()
        response = requests.delete(url=self.active_endpoint, headers=self.default_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_rest_api_version
    @decorators.verify_config_variables
    def sign_in(self, user_to_impersonate: Optional[str] = None) -> requests.Response:
        """Signs in to Tableau Server and stores an auth token to be used in follow-up REST API calls.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_authentication.htm#sign_in

        Args:
            user_to_impersonate: (optional) The user ID (luid) for the Tableau user being impersonated.
        """
        self.active_request = api_requests.SignInRequest(
            ts_connection=self,
            auth_method=self.auth_method,
            username=self.username,
            password=self.password,
            personal_access_token_name=self.personal_access_token_name,
            personal_access_token_secret=self.personal_access_token_secret,
            user_to_impersonate=user_to_impersonate,
        ).get_request()
        self.active_endpoint = api_endpoints.AuthEndpoint(ts_connection=self, sign_in=True).get_endpoint()
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.sign_in_headers,
            verify=self.ssl_verify,
        )
        if response.status_code == 200:
            response = self._set_response_encoding(response=response)
            self.auth_token = response.json().get("credentials", dict()).get("token")
            self.site_id = response.json().get("credentials", dict()).get("site").get("id")
            self.user_id = response.json().get("credentials", dict()).get("user").get("id")
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_signed_in
    def sign_out(self) -> requests.Response:
        """Signs out from Tableau Server and invalidates the connection's active auth token."""
        endpoint = api_endpoints.AuthEndpoint(ts_connection=self, sign_out=True).get_endpoint()
        response = requests.post(url=endpoint, headers=self.x_auth_header, verify=self.ssl_verify)
        if response.status_code == 204:
            response = self._set_response_encoding(response=response)
            self.auth_token = None
            self.site_id = None
            self.user_id = None
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_signed_in
    @decorators.verify_api_method_exists("2.6")
    def switch_site(self, content_url: str) -> requests.Response:
        """Switches the connection to use the specified site, which is identified by the provided 'content_url'.

        Args:
            content_url: The 'content_url' is the site's content URL, which is the site name as seen within the URL.
        """
        self.active_request = api_requests.SwitchSiteRequest(ts_connection=self, site_name=content_url).get_request()
        self.active_endpoint = api_endpoints.AuthEndpoint(ts_connection=self, switch_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        if response.status_code == 200:
            response = self._set_response_encoding(response=response)
            self.auth_token = response.json().get("credentials", dict()).get("token")
            self.site_id = response.json().get("credentials", dict()).get("site").get("id")
            self.site_name = self.query_site().json().get("site", dict()).get("name")
            self.site_url = response.json().get("credentials", dict()).get("site").get("contentUrl")
            self.user_id = response.json().get("credentials", dict()).get("user").get("id")
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.4")
    def server_info(self) -> requests.Response:
        """Returns information about the active Tableau Server connection."""
        self.active_endpoint = api_endpoints.AuthEndpoint(ts_connection=self, get_server_info=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    # sites

    @decorators.verify_api_method_exists("2.3")
    def create_site(
        self,
        site_name: str,
        content_url: str,
        admin_mode: str = "ContentAndUsers",
        user_quota: Optional[str] = None,
        tier_creator_capacity: Optional[str] = None,
        tier_explorer_capacity: Optional[str] = None,
        tier_viewer_capacity: Optional[str] = None,
        storage_quota: Optional[str] = None,
        disable_subscriptions_flag: Optional[bool] = None,
        editing_flows_enabled_flag: Optional[bool] = None,
        scheduling_flows_enabled_flag: Optional[bool] = None,
        flows_enabled_flag: Optional[bool] = None,
        guest_access_enabled_flag: Optional[bool] = None,
        allow_subscription_attachments_flag: Optional[bool] = None,
        cache_warmup_enabled_flag: Optional[bool] = None,
        commenting_enabled_flag: Optional[bool] = None,
        revision_history_enabled_flag: Optional[bool] = None,
        revision_limit: Optional[str] = None,
        subscribe_others_enabled_flag: Optional[bool] = None,
        extract_encryption_mode: Optional[str] = None,
        request_access_enabled_flag: Optional[bool] = None,
        run_now_enabled_flag: Optional[bool] = None,
        data_alerts_enabled_flag: Optional[bool] = None,
        commenting_mentions_enabled_flag: Optional[bool] = None,
        catalog_obfuscation_enabled_flag: Optional[bool] = None,
        flow_auto_save_enabled_flag: Optional[bool] = None,
        web_extraction_enabled_flag: Optional[bool] = None,
        metrics_content_type_enabled_flag: Optional[bool] = None,
        notify_site_admins_on_throttle_flag: Optional[bool] = None,
        authoring_enabled_flag: Optional[bool] = None,
        custom_subscription_email_enabled_flag: Optional[bool] = None,
        custom_subscription_email: Optional[str] = None,
        custom_subscription_footer_enabled_flag: Optional[bool] = None,
        custom_subscription_footer: Optional[str] = None,
        ask_data_mode: Optional[str] = "EnabledByDefault",
        named_sharing_enabled_flag: Optional[bool] = None,
        mobile_biometrics_enabled_flag: Optional[bool] = None,
        sheet_image_enabled_flag: Optional[bool] = None,
        cataloging_enabled_flag: Optional[bool] = None,
        derived_permissions_enabled_flag: Optional[bool] = None,
        user_visibility_mode: str = "FULL",
        use_default_time_zone_flag: Optional[bool] = None,
        time_zone: Optional[str] = None,
        auto_suspend_refresh_enabled_flag: Optional[bool] = None,
        auto_suspend_refresh_inactivity_window: Optional[str] = None,
    ) -> requests.Response:
        """Creates a new site using the active Tableau Server connection.

        This method can only be called by Server Administrators.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#create_site

        RequiredArgs:
            site_name: (required) The name for the new site.
            content_url: (required) The content url for the new site (can be different than the site name).
        """
        local_vars = self._set_local_vars(local_vars=locals())
        self.active_request = api_requests.CreateSiteRequest(ts_connection=self, **local_vars).get_request()
        self.active_endpoint = api_endpoints.SiteEndpoint(ts_connection=self, create_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_site(
        self, include_usage_flag: Optional[bool] = None, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Queries details for the active site.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#query_site

        Args:
            include_usage_flag: True if usage metrics are desired in the results of the site query, False otherwise.
            parameter_dict: A Python dict defining URL parameters to modify or filter the underlying API endpoint.
        """
        self.active_endpoint = api_endpoints.SiteEndpoint(
            ts_connection=self,
            query_site=True,
            site_id=self.site_id,
            include_usage_flag=include_usage_flag,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_sites(self, parameter_dict: Dict[str, Any] = None) -> requests.Response:
        """Queries details for all sites on the server.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#query_sites

        Args:
            parameter_dict: A Python dict defining URL parameters to modify or filter the underlying API endpoint.
        """
        self.active_endpoint = api_endpoints.SiteEndpoint(
            ts_connection=self, query_sites=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def get_recently_viewed_for_site(self) -> requests.Response:
        """Gets the details of the views and workbooks on a site that the signed in user has recently engaged with."""
        self.active_endpoint = api_endpoints.SiteEndpoint(
            ts_connection=self, site_id=self.site_id, get_recently_viewed=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_views_for_site(self, site_id: str, parameter_dict: Dict[str, Any] = None) -> requests.Response:
        """Queries details for all views on the active site.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#query_views_for_site

        Args:
            site_id: The site ID (luid) for the site being queried.
            parameter_dict: A Python dict defining URL parameters to modify or filter the underlying API endpoint.
        """
        self.active_endpoint = api_endpoints.SiteEndpoint(
            ts_connection=self,
            site_id=site_id,
            query_views=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_site(
        self,
        site_id,
        site_name: Optional[str] = None,
        content_url: Optional[str] = None,
        admin_mode: Optional[str] = None,
        state: Optional[str] = None,
        user_quota: Optional[str] = None,
        tier_creator_capacity: Optional[str] = None,
        tier_explorer_capacity: Optional[str] = None,
        tier_viewer_capacity: Optional[str] = None,
        storage_quota: Optional[str] = None,
        disable_subscriptions_flag: Optional[bool] = None,
        editing_flows_enabled_flag: Optional[bool] = None,
        scheduling_flows_enabled_flag: Optional[bool] = None,
        flows_enabled_flag: Optional[bool] = None,
        guest_access_enabled_flag: Optional[bool] = None,
        allow_subscription_attachments_flag: Optional[bool] = None,
        cache_warmup_enabled_flag: Optional[bool] = None,
        commenting_enabled_flag: Optional[bool] = None,
        revision_history_enabled_flag: Optional[bool] = None,
        revision_limit: Optional[str] = None,
        subscribe_others_enabled_flag: Optional[bool] = None,
        extract_encryption_mode: Optional[str] = None,
        request_access_enabled_flag: Optional[bool] = None,
        run_now_enabled_flag: Optional[bool] = None,
        data_alerts_enabled_flag: Optional[bool] = None,
        commenting_mentions_enabled_flag: Optional[bool] = None,
        catalog_obfuscation_enabled_flag: Optional[bool] = None,
        flow_auto_save_enabled_flag: Optional[bool] = None,
        web_extraction_enabled_flag: Optional[bool] = None,
        metrics_content_type_enabled_flag: Optional[bool] = None,
        notify_site_admins_on_throttle_flag: Optional[bool] = None,
        authoring_enabled_flag: Optional[bool] = None,
        custom_subscription_email_enabled_flag: Optional[bool] = None,
        custom_subscription_email: Optional[str] = None,
        custom_subscription_footer_enabled_flag: Optional[bool] = None,
        custom_subscription_footer: Optional[str] = None,
        ask_data_mode: str = "EnabledByDefault",
        named_sharing_enabled_flag: Optional[bool] = None,
        mobile_biometrics_enabled_flag: Optional[bool] = None,
        sheet_image_enabled_flag: Optional[bool] = None,
        cataloging_enabled_flag: Optional[bool] = None,
        derived_permissions_enabled_flag: Optional[bool] = None,
        user_visibility_mode: str = "FULL",
        use_default_time_zone_flag: Optional[bool] = None,
        time_zone: Optional[str] = None,
        auto_suspend_refresh_enabled_flag: Optional[bool] = None,
        auto_suspend_refresh_inactivity_window: Optional[str] = None,
    ) -> requests.Response:
        """Updates details and configurations for the specified site.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#update_site

        RequiredArgs:
            site_id: (required) The site ID (luid) for the site being updated.
        """
        # This method can only be called by server administrators.
        local_vars = self._set_local_vars(local_vars=locals())
        self.active_request = api_requests.UpdateSiteRequest(ts_connection=self, **local_vars).get_request()
        self.active_endpoint = api_endpoints.SiteEndpoint(
            ts_connection=self, site_id=site_id, update_site=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_site(
        self, site_id: Optional[str] = None, site_name: Optional[str] = None, content_url: Optional[str] = None
    ) -> requests.Response:
        """Deletes the specified site.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_site.htm#delete_site

        Args:
            site_id: (optional) The site ID (luid) for the site being deleted.
            site_name: (optional) The name of the site being deleted.
            content_url: (optional) The site's name as it appears in the URL.
        """
        # This method can only be called by server administrators.
        self.active_endpoint = api_endpoints.SiteEndpoint(
            ts_connection=self,
            delete_site=True,
            site_id=site_id,
            site_name=site_name,
            content_url=content_url,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # data driven alerts

    @decorators.verify_api_method_exists("3.2")
    def delete_data_driven_alert(self, data_alert_id: str) -> requests.Response:
        """Deletes the specified data driven alert.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data-driven_alerts.htm#delete_data-driven_alert

        Args:
            data_alert_id: The data driven alert ID for the alert being deleted.
        """
        self.active_endpoint = api_endpoints.DataAlertEndpoint(
            ts_connection=self, data_alert_id=data_alert_id, delete_data_alert=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def query_data_driven_alert_details(self, data_alert_id: str) -> requests.Response:
        """Queries details for the specified data driven alert.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data-driven_alerts.htm#query_data-driven_alert_details

        Args:
            data_alert_id: The data driven alert ID for the alert being queried.
        """
        self.active_endpoint = api_endpoints.DataAlertEndpoint(
            ts_connection=self, query_data_alert=True, data_alert_id=data_alert_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def query_data_driven_alerts(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries the data driven alerts for the active site.

        Args:
            parameter_dict: A Python dict defining URL parameters to modify or filter the underlying API endpoint.
        """
        self.active_endpoint = api_endpoints.DataAlertEndpoint(
            ts_connection=self, query_data_alerts=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def add_user_to_data_driven_alert(self, user_id: str, data_alert_id: str) -> requests.Response:
        """Adds the specified user to the specified data driven alert.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data-driven_alerts.htm#add_user_to_data-driven_alert

        Args:
            user_id: The user ID (luid) for the user being added to the alert.
            data_alert_id: The data driven alert ID for the alert the user is being added to.
        """
        self.active_request = api_requests.AddUserToAlertRequest(ts_connection=self, user_id=user_id).get_request()
        self.active_endpoint = api_endpoints.DataAlertEndpoint(
            ts_connection=self,
            add_user=True,
            user_id=user_id,
            data_alert_id=data_alert_id,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def delete_user_from_data_driven_alert(self, user_id: str, data_alert_id: str) -> requests.Response:
        """Removes the specified user from the specified data driven alert.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data-driven_alerts.htm#delete_user_from_data-driven_alert

        Args:
            user_id: The user ID (luid) for the user being removed from the alert.
            data_alert_id: The data driven alert ID for the alert the user is being removed from.
        """
        self.active_endpoint = api_endpoints.DataAlertEndpoint(
            ts_connection=self,
            remove_user=True,
            user_id=user_id,
            data_alert_id=data_alert_id,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def update_data_driven_alert(
        self,
        data_alert_id: str,
        data_alert_subject: Optional[str] = None,
        data_alert_frequency: Optional[str] = None,
        data_alert_owner_id: Optional[str] = None,
        is_public_flag: Optional[bool] = None,
    ) -> requests.Response:
        """Updates details and configurations for the specified data driven alert.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_data-driven_alerts.htm#update_data-driven_alert

        RequiredArgs:
            data_alert_id: (required) The ID for the data-driven alert being updated.
        """
        self.active_request = api_requests.UpdateDataAlertRequest(
            ts_connection=self,
            data_alert_subject=data_alert_subject,
            data_alert_frequency=data_alert_frequency,
            data_alert_owner_id=data_alert_owner_id,
            is_public_flag=is_public_flag,
        ).get_request()
        self.active_endpoint = api_endpoints.DataAlertEndpoint(
            ts_connection=self, data_alert_id=data_alert_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # flows

    @decorators.verify_api_method_exists("3.3")
    def query_flow(self, flow_id: str) -> requests.Response:
        """Queries details for the specified flow."""
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, flow_id=flow_id, query_flow=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def delete_flow(self, flow_id: str) -> requests.Response:
        """Deletes the specified flow."""
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, flow_id=flow_id, delete_flow=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def download_flow(self, flow_id: str) -> requests.Response:
        """Downloads the specified flow."""
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, flow_id=flow_id, download_flow=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def query_flow_connections(self, flow_id: str) -> requests.Response:
        """Queries the connection details for the specified flow."""
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, flow_id=flow_id, query_flow_connections=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def query_flows_for_site(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all flows on the active site."""
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, query_flows_for_site=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def query_flows_for_user(self, user_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all flows belonging to the specified user."""
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self,
            user_id=user_id,
            query_flows_for_user=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def update_flow(
        self, flow_id: str, new_project_id: Optional[str] = None, new_owner_id: Optional[str] = None
    ) -> requests.Response:
        """Updates details for the specified flow."""
        self.active_request = api_requests.UpdateFlowRequest(
            ts_connection=self, new_project_id=new_project_id, new_owner_id=new_owner_id
        ).get_request()
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, flow_id=flow_id, update_flow=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def update_flow_connection(
        self,
        flow_id: str,
        connection_id: str,
        server_address: Optional[str] = None,
        port: Optional[str] = None,
        connection_username: Optional[str] = None,
        connection_password: Optional[str] = None,
        embed_password_flag: Optional[bool] = None,
    ) -> requests.Response:
        """Updates details for the specified connection in the specified flow."""
        self.active_request = api_requests.UpdateFlowConnectionRequest(
            ts_connection=self,
            server_address=server_address,
            port=port,
            connection_username=connection_username,
            connection_password=connection_password,
            embed_password_flag=embed_password_flag,
        ).get_request()
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self,
            flow_id=flow_id,
            connection_id=connection_id,
            update_flow_connection=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # projects

    @decorators.verify_api_method_exists("2.3")
    def create_project(
        self,
        project_name: str,
        project_description: Optional[str] = None,
        content_permissions: Optional[str] = "ManagedByOwner",
        parent_project_id: Optional[str] = None,
        parameter_dict: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Creates a new project on the active site."""
        self.active_request = api_requests.CreateProjectRequest(
            ts_connection=self,
            project_name=project_name,
            project_description=project_description,
            content_permissions=content_permissions,
            parent_project_id=parent_project_id,
        ).get_request()
        self.active_endpoint = api_endpoints.ProjectEndpoint(
            ts_connection=self, create_project=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_projects(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all projects on the active site."""
        self.active_endpoint = api_endpoints.ProjectEndpoint(
            ts_connection=self, query_projects=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_project(
        self,
        project_id: str,
        project_name: Optional[str] = None,
        project_description: Optional[str] = None,
        content_permissions: Optional[str] = None,
        parent_project_id: Optional[str] = None,
    ) -> requests.Response:
        """Updates details for the specified project."""
        self.active_request = api_requests.UpdateProjectRequest(
            ts_connection=self,
            project_name=project_name,
            project_description=project_description,
            content_permissions=content_permissions,
            parent_project_id=parent_project_id,
        ).get_request()
        self.active_endpoint = api_endpoints.ProjectEndpoint(
            ts_connection=self, update_project=True, project_id=project_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_project(self, project_id: str) -> requests.Response:
        """Deletes the specified project."""
        self.active_endpoint = api_endpoints.ProjectEndpoint(
            ts_connection=self, project_id=project_id, delete_project=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # workbooks and views

    @decorators.verify_api_method_exists("2.6")
    def add_tags_to_view(self, view_id: str, tags: List[str]) -> requests.Response:
        """Adds one or more tags to the specified view."""
        self.active_request = api_requests.AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self, view_id=view_id, add_tags=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_tags_to_workbook(self, workbook_id: str, tags: List[str]) -> requests.Response:
        """Adds tags to the specified workbook."""
        self.active_request = api_requests.AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self, workbook_id=workbook_id, add_tags=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_views_for_workbook(
        self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Queries details for all views in the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            query_views=True,
            workbook_id=workbook_id,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.8")
    def query_view_data(self, view_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries the underlying data within the specified view."""
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self,
            view_id=view_id,
            query_view_data=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.5")
    def query_view_image(self, view_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Downloads a PNG of the specified view."""
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self,
            view_id=view_id,
            query_view_image=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.8")
    def query_view_pdf(self, view_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Downloads a PDF of the specified view."""
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self,
            view_id=view_id,
            query_view_pdf=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_view_preview_image(
        self, workbook_id: str, view_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Downloads the preview image for the specified view within the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            view_id=view_id,
            query_workbook_view_preview_img=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.0")
    def get_view(self, view_id: str) -> requests.Response:
        """Queries details for the specified view."""
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self, view_id=view_id, query_view=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.6")
    def get_view_by_path(self, view_name: str) -> requests.Response:
        """Gets the details of all views in a site with a specified name."""
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self,
            query_views=True,
            parameter_dict={"filter": f'filter=viewUrlName:eq:{view_name.replace(" ", "")}'},
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.7")
    def get_recommendations_for_views(self) -> requests.Response:
        self.active_endpoint = api_endpoints.SiteEndpoint(
            ts_connection=self,
            site_id=self.site_id,
            get_recommendations=True,
            parameter_dict={"type": "type=view"},
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def query_view(self, view_id: str) -> requests.Response:
        """Queries details for the specified view."""
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self, view_id=view_id, query_view=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_workbook(self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            query_workbook=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_workbook_connections(
        self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Queries connection details for the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            query_connections=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def get_workbook_revisions(
        self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Queries revision details for the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            get_workbook_revisions=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.6")
    def get_workbook_downgrade_info(self, workbook_id: str, downgrade_target_version: str) -> requests.Response:
        """Queries details regarding the impact of downgrading the workbook to the older target version."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            downgrade_target_version=downgrade_target_version,
            get_workbook_downgrade_info=True,
        ).get_endpoint()
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def remove_workbook_revision(self, workbook_id: str, revision_number: str) -> requests.Response:
        """Deletes the specified revision for the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            revision_number=revision_number,
            remove_workbook_revision=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_workbook_preview_image(
        self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Downloads the preview image for the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            query_workbook_preview_img=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_workbooks_for_site(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all workbooks on the active site."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self, query_workbooks=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_workbooks_for_user(
        self, user_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Queries details for all workbooks belonging to the specified user."""
        self.active_endpoint = api_endpoints.UserEndpoint(
            ts_connection=self,
            user_id=user_id,
            query_workbooks_for_user=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def download_workbook(self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Downloads the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            download_workbook=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.4")
    def download_workbook_pdf(
        self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Downloads a PDF version of the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            download_workbook_pdf=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.8")
    def download_workbook_powerpoint(
        self, workbook_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Downloads a Powerpoint (.pptx) version of the specified workbook.

        Args:
            workbook_id: The ID (luid) for the workbook being downloaded.
            parameter_dict: (optional) A Python dict whose values define additional URL parameters.
        """
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            download_workbook_pptx=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def download_workbook_revision(
        self, workbook_id: str, revision_number: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Downloads an older version of the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            revision_number=revision_number,
            download_workbook_revision=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_workbook(
        self,
        workbook_id: str,
        show_tabs_flag: Optional[bool] = None,
        new_project_id: Optional[str] = None,
        new_owner_id: Optional[str] = None,
    ) -> requests.Response:
        """Updates the details of the specified workbook."""
        self.active_request = api_requests.UpdateWorkbookRequest(
            ts_connection=self,
            show_tabs_flag=show_tabs_flag,
            project_id=new_project_id,
            owner_id=new_owner_id,
        ).get_request()
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self, workbook_id=workbook_id, update_workbook=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_workbook_connection(
        self,
        workbook_id: str,
        connection_id: str,
        server_address: Optional[str] = None,
        port: Optional[str] = None,
        connection_username: Optional[str] = None,
        connection_password: Optional[str] = None,
        embed_password_flag: Optional[bool] = None,
        parameter_dict: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Updates the specified connection for the specified workbook."""
        self.active_request = api_requests.UpdateWorkbookConnectionRequest(
            ts_connection=self,
            server_address=server_address,
            port=port,
            connection_username=connection_username,
            connection_password=connection_password,
            embed_password_flag=embed_password_flag,
        ).get_request()
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            connection_id=connection_id,
            update_workbook_connection=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.8")
    def update_workbook_now(self, workbook_id: str) -> requests.Response:
        """Immediately executes extract refreshes for the specified workbook."""
        self.active_request = api_requests.EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self, workbook_id=workbook_id, refresh_workbook=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_workbook(self, workbook_id: str) -> requests.Response:
        """Deletes the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self, workbook_id=workbook_id, delete_workbook=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.6")
    def delete_tag_from_view(self, view_id: str, tag_name: str) -> requests.Response:
        """Deletes the named tag from the specified view."""
        self.active_endpoint = api_endpoints.ViewEndpoint(
            ts_connection=self, view_id=view_id, tag_name=tag_name, delete_tag=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_tag_from_workbook(self, workbook_id: str, tag_name: str) -> requests.Response:
        """Deletes the named tag from the specified workbook."""
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            tag_name=tag_name,
            delete_tag=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # data sources

    @decorators.verify_api_method_exists("2.6")
    def add_tags_to_data_source(self, datasource_id: str, tags: List[str]) -> requests.Response:
        """Adds one or more tags to the specified datasource."""
        self.active_request = api_requests.AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, datasource_id=datasource_id, add_tags=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.6")
    def delete_tag_from_data_source(self, datasource_id: str, tag_name: str) -> requests.Response:
        """Deletes a named tag from the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            tag_name=tag_name,
            delete_tag=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_data_source(self, datasource_id: str) -> requests.Response:
        """Queries details for the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, datasource_id=datasource_id, query_datasource=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_signed_in
    @decorators.verify_api_method_exists("2.3")
    def query_data_sources(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all datasources on the active site."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, query_datasources=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_data_source_connections(self, datasource_id: str) -> requests.Response:
        """Queries details for the connections belonging to the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            query_datasource_connections=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def get_data_source_revisions(
        self, datasource_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Queries revision details for the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            get_datasource_revisions=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def download_data_source(
        self, datasource_id: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Downloads the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            download_datasource=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def download_data_source_revision(
        self, datasource_id: str, revision_number: str, parameter_dict: Optional[Dict[str, Any]] = None
    ) -> requests.Response:
        """Downloads the specified revision number for the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            revision_number=revision_number,
            download_datasource_revision=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_data_source(
        self,
        datasource_id: str,
        new_project_id: Optional[str] = None,
        new_owner_id: Optional[str] = None,
        is_certified_flag: Optional[bool] = None,
        certification_note: Optional[str] = None,
    ) -> requests.Response:
        """Updates details for the specified datasource.

        Note that assigning a new project ID to an embedded extract will not actually change the extract's project ID,
        even if the response indicates it has moved.
        """
        self.active_request = api_requests.UpdateDatasourceRequest(
            ts_connection=self,
            new_project_id=new_project_id,
            new_owner_id=new_owner_id,
            is_certified_flag=is_certified_flag,
            certification_note=certification_note,
        ).get_request()
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, datasource_id=datasource_id, update_datasource=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_data_source_connection(
        self,
        datasource_id: str,
        connection_id: str,
        server_address: Optional[str] = None,
        port: Optional[str] = None,
        connection_username: Optional[str] = None,
        connection_password: Optional[str] = None,
        embed_password_flag: Optional[bool] = None,
    ) -> requests.Response:
        """Updates details for the specified connection in the specified datasource.

        Note that you must set the connection_password='' if changing the embed_password_flag from True to False
        """
        self.active_request = api_requests.UpdateDatasourceConnectionRequest(
            ts_connection=self,
            server_address=server_address,
            port=port,
            connection_username=connection_username,
            connection_password=connection_password,
            embed_password_flag=embed_password_flag,
        ).get_request()
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            connection_id=connection_id,
            update_datasource_connection=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.8")
    def update_data_source_now(self, datasource_id: str) -> requests.Response:
        """Immediately executes an extract refresh for the specified datasource."""
        self.active_request = api_requests.EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, datasource_id=datasource_id, refresh_datasource=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_data_source(self, datasource_id: str) -> requests.Response:
        """Deletes the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, datasource_id=datasource_id, delete_datasource=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def remove_data_source_revision(self, datasource_id: str, revision_number: str) -> requests.Response:
        """Deletes the specified revision number for the specified datasource."""
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            revision_number=revision_number,
            remove_datasource_revision=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # users and groups

    @decorators.verify_api_method_exists("2.3")
    def create_group(
        self,
        new_group_name: str,
        active_directory_group_name: Optional[str] = None,
        active_directory_domain_name: Optional[str] = None,
        minimum_site_role: Optional[str] = None,
        license_mode: Optional[str] = None,
        parameter_dict: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Creates a group on the active site.

        For descriptions of all input parameters, see Tableau's official REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_users_and_groups.htm#create_group
        """
        local_vars = self._set_local_vars(local_vars=locals())
        self.active_request = api_requests.CreateGroupRequest(ts_connection=self, **local_vars).get_request()
        self.active_endpoint = api_endpoints.GroupEndpoint(
            ts_connection=self, create_group=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_user_to_group(self, group_id: str, user_id: str) -> requests.Response:
        """Adds the specified user to the specified group."""
        self.active_request = api_requests.AddUserToGroupRequest(ts_connection=self, user_id=user_id).get_request()
        self.active_endpoint = api_endpoints.GroupEndpoint(
            ts_connection=self, group_id=group_id, add_user=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_user_to_site(self, user_name: str, site_role: str, auth_setting: Optional[str] = None) -> requests.Response:
        """Adds a user to the active site."""
        self.active_request = api_requests.AddUserToSiteRequest(
            ts_connection=self,
            user_name=user_name,
            site_role=site_role,
            auth_setting=auth_setting,
        ).get_request()
        self.active_endpoint = api_endpoints.UserEndpoint(ts_connection=self, add_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.7")
    def get_groups_for_a_user(self, user_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Gets a list of groups of which the specified user is a member."""
        self.active_endpoint = api_endpoints.UserEndpoint(
            ts_connection=self,
            user_id=user_id,
            query_groups_for_user=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def get_users_in_group(self, group_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all users within the specified group."""
        self.active_endpoint = api_endpoints.GroupEndpoint(
            ts_connection=self,
            group_id=group_id,
            get_users=True,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def get_users_on_site(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all users on the active site."""
        self.active_endpoint = api_endpoints.UserEndpoint(
            ts_connection=self, query_users=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_groups(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries details for all groups on the active site."""
        self.active_endpoint = api_endpoints.GroupEndpoint(
            ts_connection=self, query_groups=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_user_on_site(self, user_id: str) -> requests.Response:
        """Queries details for the specified user on the active site."""
        self.active_endpoint = api_endpoints.UserEndpoint(
            ts_connection=self, user_id=user_id, query_user=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_group(
        self,
        group_id: str,
        new_group_name: str,
        active_directory_group_name: Optional[str] = None,
        active_directory_domain_name: Optional[str] = None,
        minimum_site_role: Optional[str] = None,
        license_mode: Optional[str] = None,
        parameter_dict: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Updates details for the specified group."""
        local_vars = self._set_local_vars(local_vars=locals())
        self.active_request = api_requests.CreateGroupRequest(ts_connection=self, **local_vars).get_request()
        self.active_endpoint = api_endpoints.GroupEndpoint(
            ts_connection=self, group_id=group_id, update_group=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_user(
        self,
        user_id: str,
        new_full_name: Optional[str] = None,
        new_email: Optional[str] = None,
        new_password: Optional[str] = None,
        new_site_role: Optional[str] = None,
        new_auth_setting: Optional[str] = None,
    ) -> requests.Response:
        """Updates details for the specified user."""
        self.active_request = api_requests.UpdateUserRequest(
            ts_connection=self,
            new_full_name=new_full_name,
            new_email=new_email,
            new_password=new_password,
            new_site_role=new_site_role,
            new_auth_setting=new_auth_setting,
        ).get_request()
        self.active_endpoint = api_endpoints.UserEndpoint(
            ts_connection=self, user_id=user_id, update_user=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.default_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def remove_user_from_group(self, group_id: str, user_id: str) -> requests.Response:
        """Removes the specified user from the specified group."""
        self.active_endpoint = api_endpoints.GroupEndpoint(
            ts_connection=self, group_id=group_id, user_id=user_id, remove_user=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def remove_user_from_site(self, user_id: str) -> requests.Response:
        """Removes the specified user from the active site."""
        # TODO(elliott): add support for the mapAssetsTo optional parameter
        self.active_endpoint = api_endpoints.UserEndpoint(
            ts_connection=self, user_id=user_id, remove_user=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_group(self, group_id: str) -> requests.Response:
        """Deletes the specified group from the active site."""
        self.active_endpoint = api_endpoints.GroupEndpoint(
            ts_connection=self, group_id=group_id, delete_group=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # permissions

    @decorators.verify_api_method_exists("2.3")
    def add_data_source_permissions(
        self,
        datasource_id: str,
        user_capability_dict: Optional[Dict[str, Any]],
        group_capability_dict: Optional[Dict[str, Any]],
        user_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> requests.Response:
        """Adds permissions rules for the specified datasource."""
        self.active_request = api_requests.AddDatasourcePermissionsRequest(
            ts_connection=self,
            datasource_id=datasource_id,
            user_id=user_id,
            group_id=group_id,
            user_capability_dict=user_capability_dict,
            group_capability_dict=group_capability_dict,
        ).get_request()
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="datasource",
            object_id=datasource_id,
            add_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def add_flow_permissions(
        self,
        flow_id: str,
        user_capability_dict: Optional[Dict[str, Any]],
        group_capability_dict: Optional[Dict[str, Any]],
        user_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> requests.Response:
        """Adds permissions rules for the specified flow."""
        self.active_request = api_requests.AddFlowPermissionsRequest(
            ts_connection=self,
            user_id=user_id,
            group_id=group_id,
            user_capability_dict=user_capability_dict,
            group_capability_dict=group_capability_dict,
        ).get_request()
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="flow",
            object_id=flow_id,
            add_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_project_permissions(
        self,
        project_id,
        user_capability_dict=None,
        group_capability_dict=None,
        user_id=None,
        group_id=None,
    ):
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
        self.active_request = api_requests.AddProjectPermissionsRequest(
            ts_connection=self,
            user_id=user_id,
            group_id=group_id,
            user_capability_dict=user_capability_dict,
            group_capability_dict=group_capability_dict,
        ).get_request()
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="project",
            object_id=project_id,
            add_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_default_permissions(
        self,
        project_id,
        project_permissions_object,
        group_id=None,
        user_id=None,
        user_capability_dict=None,
        group_capability_dict=None,
    ):
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
        self.active_request = api_requests.AddDefaultPermissionsRequest(
            ts_connection=self,
            group_id=group_id,
            user_id=user_id,
            group_capability_dict=group_capability_dict,
            user_capability_dict=user_capability_dict,
        ).get_request()
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            project_id=project_id,
            project_permissions_object=project_permissions_object,
            add_default_project_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def add_view_permissions(
        self,
        view_id,
        user_capability_dict=None,
        group_capability_dict=None,
        user_id=None,
        group_id=None,
    ):
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
        self.active_request = api_requests.AddViewPermissionsRequest(
            ts_connection=self,
            view_id=view_id,
            user_id=user_id,
            group_id=group_id,
            user_capability_dict=user_capability_dict,
            group_capability_dict=group_capability_dict,
        ).get_request()
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="view",
            object_id=view_id,
            add_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_workbook_permissions(
        self,
        workbook_id,
        user_capability_dict=None,
        group_capability_dict=None,
        user_id=None,
        group_id=None,
    ):
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
        self.active_request = api_requests.AddWorkbookPermissionsRequest(
            ts_connection=self,
            workbook_id=workbook_id,
            user_id=user_id,
            group_id=group_id,
            user_capability_dict=user_capability_dict,
            group_capability_dict=group_capability_dict,
        ).get_request()
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="workbook",
            object_id=workbook_id,
            add_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers.copy()
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_data_source_permissions(self, datasource_id):
        """
        Queries permissions details for the specified datasource.
        :param string datasource_id: the datasource ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="datasource",
            object_id=datasource_id,
            query_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_flow_permissions(self, flow_id):
        """
        Queries permissions details for the specified flow.
        :param string flow_id: the flow ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="flow",
            object_id=flow_id,
            query_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_project_permissions(self, project_id):
        """
        Queries permissions details for the specified project.
        :param string project_id: the project ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="project",
            object_id=project_id,
            query_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_default_permissions(self, project_id: str, project_permissions_object: str) -> requests.Response:
        """Queries permissions details for the specified object variety within the specified project.

        Args:
            project_id: The Tableau project ID.
            project_permissions_object: The permissions object variety [workbook, datasource, flow, etc].
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            project_id=project_id,
            project_permissions_object=project_permissions_object,
            query_default_project_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def query_view_permissions(self, view_id):
        """
        Queries permissions details for the specified view.
        :param string view_id: the view ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="view",
            object_id=view_id,
            query_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_workbook_permissions(self, workbook_id):
        """
        Query permissions details for the specified workbook.
        :param string workbook_id: the workbook ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="workbook",
            object_id=workbook_id,
            query_object_permissions=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_data_source_permission(
        self,
        datasource_id,
        delete_permissions_object,
        delete_permissions_object_id,
        capability_name,
        capability_mode,
    ):
        """
        Deletes the specified permission for the specified datasource.
        :param string datasource_id: the datasource ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="datasource",
            object_id=datasource_id,
            delete_object_permissions=True,
            delete_permissions_object=delete_permissions_object,
            delete_permissions_object_id=delete_permissions_object_id,
            capability_name=capability_name,
            capability_mode=capability_mode,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def delete_flow_permission(
        self,
        flow_id,
        delete_permissions_object,
        delete_permissions_object_id,
        capability_name,
        capability_mode,
    ):
        """
        Deletes the specified permission for the specified flow.
        :param string flow_id: the flow ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="flow",
            object_id=flow_id,
            delete_object_permissions=True,
            delete_permissions_object=delete_permissions_object,
            delete_permissions_object_id=delete_permissions_object_id,
            capability_name=capability_name,
            capability_mode=capability_mode,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_project_permission(
        self,
        project_id,
        delete_permissions_object,
        delete_permissions_object_id,
        capability_name,
        capability_mode,
    ):
        """
        Deletes the specified permission for the specified project.
        :param string project_id: the project ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="project",
            object_id=project_id,
            delete_object_permissions=True,
            delete_permissions_object=delete_permissions_object,
            delete_permissions_object_id=delete_permissions_object_id,
            capability_name=capability_name,
            capability_mode=capability_mode,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_default_permission(
        self,
        project_id,
        project_permissions_object,
        delete_permissions_object,
        delete_permissions_object_id,
        capability_name,
        capability_mode,
    ):
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
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            project_id=project_id,
            project_permissions_object=project_permissions_object,
            delete_default_project_permissions=True,
            delete_permissions_object=delete_permissions_object,
            delete_permissions_object_id=delete_permissions_object_id,
            capability_name=capability_name,
            capability_mode=capability_mode,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.2")
    def delete_view_permission(
        self,
        view_id,
        delete_permissions_object,
        delete_permissions_object_id,
        capability_name,
        capability_mode,
    ):
        """
        Deletes the specified permission for the specified view.
        :param string view_id: the view ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="view",
            object_id=view_id,
            delete_object_permissions=True,
            delete_permissions_object=delete_permissions_object,
            delete_permissions_object_id=delete_permissions_object_id,
            capability_name=capability_name,
            capability_mode=capability_mode,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_workbook_permission(
        self,
        workbook_id,
        delete_permissions_object,
        delete_permissions_object_id,
        capability_name,
        capability_mode,
    ):
        """
        Deletes the specified permission for the specified workbook.
        :param string workbook_id: the workbook ID
        :param string delete_permissions_object: the object type [users or groups]
        :param string delete_permissions_object_id: the object ID [user ID or group ID]
        :param string capability_name: the capability to remove permissions for
        :param string capability_mode: the capability mode to remove permissions for
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.PermissionsEndpoint(
            ts_connection=self,
            object_type="workbook",
            object_id=workbook_id,
            delete_object_permissions=True,
            delete_permissions_object=delete_permissions_object,
            delete_permissions_object_id=delete_permissions_object_id,
            capability_name=capability_name,
            capability_mode=capability_mode,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # jobs, tasks, and schedules

    @decorators.verify_api_method_exists("2.8")
    def add_data_source_to_schedule(self, datasource_id: str, schedule_id: str) -> requests.Response:
        """Adds the specified datasource to the specified schedule.

        The official Tableau REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#add_data_source_to_schedule

        Args:
            datasource_id: The ID (luid) of the datasource being added to the extract refresh schedule.
            schedule_id: The ID (luid) of the extract refresh schedule the datasource is being added to.
        """
        self.active_request = api_requests.AddDatasourceToScheduleRequest(
            ts_connection=self, datasource_id=datasource_id
        ).get_request()
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, schedule_id=schedule_id, add_datasource=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def add_flow_task_to_schedule(self, flow_id, schedule_id):
        """
        Adds the specified flow task to the specified schedule.
        :param string flow_id: the flow ID
        :param string schedule_id: the schedule ID
        :return: HTTP response
        """
        self.active_request = api_requests.AddFlowToScheduleRequest(ts_connection=self, flow_id=flow_id).get_request()
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, schedule_id=schedule_id, add_flow=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.8")
    def add_workbook_to_schedule(self, workbook_id: str, schedule_id: str) -> requests.Response:
        """Adds the specified workbook to the specified schedule.

        The official Tableau REST API documentation:
        https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref_jobs_tasks_and_schedules.htm#add_workbook_to_schedule

        Args:
            workbook_id: The ID (luid) of the workbook being added to the extract refresh schedule.
            schedule_id: The ID (luid) of the extract refresh schedule the workbook is being added to.
        """
        self.active_request = api_requests.AddWorkbookToScheduleRequest(
            ts_connection=self, workbook_id=workbook_id
        ).get_request()
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, schedule_id=schedule_id, add_workbook=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.1")
    def cancel_job(self, job_id):
        """
        Cancels the specified job.
        :param string job_id: the job ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.JobsEndpoint(
            ts_connection=self, job_id=job_id, cancel_job=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_job(self, job_id):
        """
        Queries the specified job.
        :param string job_id: the job ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.JobsEndpoint(
            ts_connection=self, job_id=job_id, query_job=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.1")
    def query_jobs(self, parameter_dict: Optional[Dict[str, Any]] = None):
        """
        Queries details for all jobs on the active site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.JobsEndpoint(
            ts_connection=self, query_jobs=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.6")
    def get_extract_refresh_task(self, task_id: str) -> requests.Response:
        """Queries details for the specified extract refresh task.

        Args:
            task_id: The ID (luid) for the extract refresh task being queried.
        """
        self.active_endpoint = api_endpoints.TasksEndpoint(
            ts_connection=self, task_id=task_id, get_refresh_task=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def get_extract_refresh_tasks_for_site(self) -> requests.Response:
        """Queries details for all extract refresh tasks on the active site."""
        self.active_endpoint = api_endpoints.TasksEndpoint(ts_connection=self, get_refresh_tasks=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.8")
    def get_schedule(self, schedule_id: str) -> requests.Response:
        """Queries details for the specified schedule on the active server environment."""
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, query_schedule=True, schedule_id=schedule_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    def get_extract_refresh_tasks_for_schedule(self, schedule_id: str) -> requests.Response:
        """Queries details for all extract refresh tasks belonging to the specified schedule.

        Args:
            schedule_id: The ID (luid) for the extract refresh schedule being queried.
        """
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, schedule_id=schedule_id, query_extract_schedules=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def get_flow_run_task(self, task_id: str) -> requests.Response:
        """Queries details for the specified flow run task.

        Args:
            task_id: The ID (luid) for the flow run task being queried.
        """
        self.active_endpoint = api_endpoints.TasksEndpoint(
            ts_connection=self, task_id=task_id, get_flow_run_task=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def get_flow_run_tasks(self):
        """
        Queries details for all flow run tasks on the active site.
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.TasksEndpoint(ts_connection=self, get_flow_run_tasks=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def create_schedule(
        self,
        schedule_name,
        schedule_priority=50,
        schedule_type="Extract",
        schedule_execution_order="Parallel",
        schedule_frequency="Weekly",
        start_time="07:00:00",
        end_time="23:00:00",
        interval_expression_list: Optional[List[Dict[str, str]]] = None,
    ):
        """
        Creates a new schedule for the server.
        :param string schedule_name: the new schedule's name
        :param string schedule_priority: the new schedule's execution priority value [1-100]
        :param string schedule_type: the new schedule type [Flow, Extract, or Subscription]
        :param string schedule_execution_order: the new schedule execution order [Parallel or Serial]
        :param string schedule_frequency: the new schedule's frequency [Hourly, Daily, Weekly, or Monthly]
        :param string start_time: the new schedule's start time [HH:MM:SS]
        :param string end_time: the new schedule's end time [HH:MM:SS]
        :param list interval_expression_list: schedule interval details, please see Tableau's REST API documentation.
        :return: HTTP response
        """
        interval_expression_list = interval_expression_list or [{"weekDay": "Monday"}]
        self.active_request = api_requests.CreateScheduleRequest(
            ts_connection=self,
            schedule_name=schedule_name,
            schedule_priority=schedule_priority,
            schedule_type=schedule_type,
            schedule_execution_order=schedule_execution_order,
            schedule_frequency=schedule_frequency,
            start_time=start_time,
            end_time=end_time,
            interval_expression_list=interval_expression_list,
        ).get_request()
        self.active_endpoint = api_endpoints.SchedulesEndpoint(ts_connection=self, create_schedule=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def query_extract_refresh_tasks_for_schedule(self, schedule_id, parameter_dict: Optional[Dict[str, Any]] = None):
        """
        Queries details for all extract refresh tasks belonging to the specified schedule.
        Requires API version 3.6 or higher.
        :param string schedule_id: the schedule ID
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.TasksEndpoint(
            ts_connection=self,
            query_schedule_refresh_tasks=True,
            schedule_id=schedule_id,
            parameter_dict=parameter_dict,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.6")
    def delete_extract_refresh_task(self, task_id: str):
        """Deletes the extract refresh task associated with the specified `task_id`."""
        self.active_endpoint = api_endpoints.TasksEndpoint(
            ts_connection=self, delete_refresh_task=True, task_id=task_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers, verify=self.ssl_verify)
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_schedules(self, parameter_dict: Optional[Dict[str, Any]] = None):
        """
        Queries details for all schedules on the server.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, query_schedules=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.6")
    def run_extract_refresh_task(self, task_id):
        """
        Runs the specified extract refresh task.
        Note that this task must belong to a schedule, and this will execute with the task's default priority value.
        :param string task_id: the extract refresh task ID
        :return: HTTP response
        """
        self.active_request = api_requests.EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = api_endpoints.TasksEndpoint(
            ts_connection=self, task_id=task_id, run_refresh_task=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def run_flow_now(self, flow_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Runs the specified flow, to be executed immediately."""
        self.active_request = api_requests.EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, flow_id=flow_id, run_flow_now=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.1")
    def get_flow_runs(self, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Queries site to obtain information for flow runs"""
        self.active_endpoint = api_endpoints.FlowRunEndpoint(
            ts_connection=self, get_flow_runs=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.1")
    def get_flow_run(self, flow_run_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Gets information about specified flow run"""
        self.active_endpoint = api_endpoints.FlowRunEndpoint(
            ts_connection=self, flow_run_id=flow_run_id, get_flow_run=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.1")
    def cancel_flow_run(self, flow_run_id: str, parameter_dict: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Cancels specified flow run"""
        self.active_endpoint = api_endpoints.FlowRunEndpoint(
            ts_connection=self, flow_run_id=flow_run_id, cancel_flow_run=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def run_flow_task(self, task_id):
        """
        Runs the specified flow run task.
        Note that this task must belong to a schedule.
        :param string task_id: the flow run task ID
        :return: HTTP response
        """
        self.active_request = api_requests.EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = api_endpoints.TasksEndpoint(
            ts_connection=self, task_id=task_id, run_flow_task=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_schedule(
        self,
        schedule_id,
        schedule_name=None,
        schedule_priority=None,
        schedule_type=None,
        schedule_state=None,
        schedule_execution_order=None,
        schedule_frequency=None,
        start_time=None,
        end_time=None,
        interval_expression_list=None,
    ):
        """
        Updates details for the specified schedule.
        :param string schedule_id: the schedule ID
        :param string schedule_name: the new schedule's name
        :param string schedule_priority: the new schedule's execution priority value [1-100]
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
        self.active_request = api_requests.UpdateScheduleRequest(
            ts_connection=self,
            schedule_name=schedule_name,
            schedule_priority=schedule_priority,
            schedule_type=schedule_type,
            schedule_state=schedule_state,
            schedule_execution_order=schedule_execution_order,
            schedule_frequency=schedule_frequency,
            start_time=start_time,
            end_time=end_time,
            interval_expression_list=interval_expression_list,
        ).get_request()
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, schedule_id=schedule_id, update_schedule=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_schedule(self, schedule_id):
        """
        Deletes the specified schedule.
        :param string schedule_id: the schedule ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.SchedulesEndpoint(
            ts_connection=self, schedule_id=schedule_id, delete_schedule=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # subscriptions

    @decorators.verify_api_method_exists("2.3")
    def create_subscription(
        self,
        subscription_subject,
        content_type,
        content_id,
        schedule_id,
        user_id,
        message=None,
        attach_image_flag=False,
        attach_pdf_flag=False,
        pdf_page_orientation="Portrait",
        pdf_page_size="Letter",
        send_view_if_empty_flag=True,
    ):
        """
        Creates a new subscription for the specified user to receive the specified content on the specified schedule.
        :param string subscription_subject: the subject for the new subscription.
        :param string content_type: the content type for the new subscription [Workbook or View]
        :param string content_id: the content ID [workbook ID or view ID]
        :param string schedule_id: the schedule ID the subscription will be executed on
        :param string user_id: the user ID for the user being subscribed to the content
        :param str message: a message body to accompany the subscription email
        :param bool attach_image_flag: True if an image will be attached to the subscription email, False otherwise
        :param bool attach_pdf_flag: True if a PDF will be attached to the subscription email, False otherwise
        :param str pdf_page_orientation: page orientation for the attached PDF ['Portrait', 'Landscape']
        :param str pdf_page_size: page size for the attached PDF
        :param bool send_view_if_empty_flag: True if the subscription is to be sent even if empty (no data); False otherwise.
        :return: HTTP response
        """
        self.active_request = api_requests.CreateSubscriptionRequest(
            ts_connection=self,
            subscription_subject=subscription_subject,
            content_type=content_type,
            content_id=content_id,
            schedule_id=schedule_id,
            user_id=user_id,
            message=message,
            attach_image_flag=attach_image_flag,
            attach_pdf_flag=attach_pdf_flag,
            pdf_page_orientation=pdf_page_orientation,
            pdf_page_size=pdf_page_size,
            send_view_if_empty_flag=send_view_if_empty_flag,
        ).get_request()
        self.active_endpoint = api_endpoints.SubscriptionsEndpoint(
            ts_connection=self, create_subscription=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_subscription(self, subscription_id):
        """
        Queries details for the specified subscription.
        :param string subscription_id: the subscription ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.SubscriptionsEndpoint(
            ts_connection=self, subscription_id=subscription_id, query_subscription=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def query_subscriptions(self, parameter_dict: Optional[Dict[str, Any]] = None):
        """
        Queries details for all subscriptions on the site.
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.SubscriptionsEndpoint(
            ts_connection=self, query_subscriptions=True, parameter_dict=parameter_dict
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def update_subscription(self, subscription_id, new_subscription_subject=None, new_schedule_id=None):
        """
        Updates details for the specified subscription.
        :param string subscription_id: the subscription ID
        :param string new_subscription_subject: (optional) the new subscription subject
        :param string new_schedule_id: (optional) the new schedule ID for the subscription
        :return: HTTP response
        """
        self.active_request = api_requests.UpdateSubscriptionRequest(
            ts_connection=self,
            new_schedule_id=new_schedule_id,
            new_subscription_subject=new_subscription_subject,
        ).get_request()
        self.active_endpoint = api_endpoints.SubscriptionsEndpoint(
            ts_connection=self,
            subscription_id=subscription_id,
            update_subscription=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_subscription(self, subscription_id):
        """
        Deletes the specified subscription.
        :param string subscription_id: the subscription ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.SubscriptionsEndpoint(
            ts_connection=self,
            subscription_id=subscription_id,
            delete_subscription=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # favorites

    @decorators.verify_api_method_exists("2.3")
    def add_data_source_to_favorites(self, datasource_id, user_id, favorite_label):
        """
        Adds the specified datasource to the favorites for the specified user.
        :param string datasource_id: the datasource ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the datasource being added as a favorite
        :return: HTTP response
        """
        self.active_request = api_requests.AddDatasourceToFavoritesRequest(
            ts_connection=self,
            datasource_id=datasource_id,
            favorite_label=favorite_label,
        ).get_request()
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self, add_to_favorites=True, user_id=user_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.1")
    def add_project_to_favorites(self, project_id, user_id, favorite_label):
        """
        Adds the specified project to the favorites for the specified user.
        :param string project_id: the project ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the project being added as a favorite
        :return: HTTP response
        """
        self.active_request = api_requests.AddProjectToFavoritesRequest(
            ts_connection=self, project_id=project_id, favorite_label=favorite_label
        ).get_request()
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self, add_to_favorites=True, user_id=user_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_view_to_favorites(self, view_id, user_id, favorite_label):
        """
        Adds the specified view to the favorites for the specified user.
        :param string view_id: the view ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the view being added as a favorite
        :return: HTTP response
        """
        self.active_request = api_requests.AddViewToFavoritesRequest(
            ts_connection=self, view_id=view_id, favorite_label=favorite_label
        ).get_request()
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self, add_to_favorites=True, user_id=user_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def add_workbook_to_favorites(self, workbook_id, user_id, favorite_label):
        """
        Adds the specified workbook to the favorites for the specified user.
        :param string workbook_id: the workbook ID
        :param string user_id: the user ID
        :param string favorite_label: the text label for the workbook being added as a favorite
        :return: HTTP response
        """
        self.active_request = api_requests.AddWorkbookToFavoritesRequest(
            ts_connection=self, workbook_id=workbook_id, favorite_label=favorite_label
        ).get_request()
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self, add_to_favorites=True, user_id=user_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_data_source_from_favorites(self, datasource_id, user_id):
        """
        Deletes the specified datasource from the specified user's favorites list.
        :param string datasource_id: the datasource ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self,
            object_type="datasource",
            object_id=datasource_id,
            user_id=user_id,
            delete_from_favorites=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.1")
    def delete_project_from_favorites(self, project_id, user_id):
        """
        Deletes the specified project from the specified user's favorites list.
        :param string project_id: the project ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self,
            object_type="project",
            object_id=project_id,
            user_id=user_id,
            delete_from_favorites=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_view_from_favorites(self, view_id, user_id):
        """
        Deletes the specified view from the specified user's favorites list.
        :param string view_id: the view ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self,
            object_type="view",
            object_id=view_id,
            user_id=user_id,
            delete_from_favorites=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def delete_workbook_from_favorites(self, workbook_id, user_id):
        """
        Deletes the specified workbook from the specified user's favorites list.
        :param string workbook_id: the workbook ID
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self,
            object_type="workbook",
            object_id=workbook_id,
            user_id=user_id,
            delete_from_favorites=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.5")
    def get_favorites_for_user(self, user_id):
        """
        Queries the favorite items for a specified user.
        :param string user_id: the user ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.FavoritesEndpoint(
            ts_connection=self, get_user_favorites=True, user_id=user_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # publishing

    @decorators.verify_api_method_exists("2.3")
    def initiate_file_upload(self):
        """
        Initiates a file upload session with Tableau Server.
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.FileUploadEndpoint(
            ts_connection=self, initiate_file_upload=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def append_to_file_upload(self, upload_session_id, payload, content_type):
        """
        Appends file data to an existing file upload session.
        :param string upload_session_id: the upload session ID
        :param payload: the payload
        :param string content_type: the content type header
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.FileUploadEndpoint(
            ts_connection=self,
            append_to_file_upload=True,
            upload_session_id=upload_session_id,
        ).get_endpoint()
        self.active_headers = self.default_headers.copy()
        self.active_headers.update({"content-type": content_type})
        response = requests.put(
            url=self.active_endpoint,
            data=payload,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def publish_data_source(
        self,
        datasource_file_path,
        datasource_name,
        project_id,
        datasource_description=None,
        connection_username=None,
        connection_password=None,
        embed_credentials_flag=False,
        oauth_flag=False,
        parameter_dict: Optional[Dict[str, Any]] = None,
    ):
        """
        Publishes a datasource file to Tableau Server.
        :param string datasource_file_path: the path to the datasource file
        :param string datasource_name: the desired name for the datasource
        :param string project_id: the project ID where the file will be published
        :param string datasource_description: the description for the datasource
        :param string connection_username: the username for the datasource's connection
        :param string connection_password: the password for the datasource's connection
        :param boolean embed_credentials_flag: enables or disables embedding the connection's password
        :param boolean oauth_flag: enables or disables OAuth authentication
        :param dict parameter_dict: dict defining url parameters for API endpoint
        :return: HTTP response
        """
        publish_request = api_requests.PublishDatasourceRequest(
            ts_connection=self,
            datasource_name=datasource_name,
            datasource_file_path=datasource_file_path,
            project_id=project_id,
            datasource_description=datasource_description,
            connection_username=connection_username,
            connection_password=connection_password,
            embed_credentials_flag=embed_credentials_flag,
            oauth_flag=oauth_flag,
        )
        self.active_request, content_type = publish_request.get_request()
        self.active_headers, parameter_dict = publish_request.publish_prep(content_type, parameter_dict=parameter_dict)
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, publish_datasource=True, parameter_dict=parameter_dict
        ).get_endpoint()
        response = requests.post(
            url=self.active_endpoint,
            data=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("2.3")
    def publish_workbook(
        self,
        workbook_file_path: str,
        workbook_name: str,
        project_id: str,
        show_tabs_flag: Optional[bool] = False,
        user_id: Optional[str] = None,
        server_address: Optional[str] = None,
        port_number: Optional[str] = None,
        connection_username: Optional[str] = None,
        connection_password: Optional[str] = None,
        embed_credentials_flag: Optional[bool] = False,
        oauth_flag: Optional[bool] = False,
        workbook_views_to_hide: Optional[List[str]] = None,
        hide_view_flag: Optional[bool] = None,
        parameter_dict: Optional[Dict[str, Any]] = None,
    ) -> requests.Response:
        """Publishes a workbook file to Tableau Server."""
        local_vars = self._set_local_vars(local_vars=locals())
        publish_request = api_requests.PublishWorkbookRequest(ts_connection=self, **local_vars)
        self.active_request, content_type = publish_request.get_request()
        self.active_headers, parameter_dict = publish_request.publish_prep(content_type, parameter_dict=parameter_dict)
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self, publish_workbook=True, parameter_dict=parameter_dict
        ).get_endpoint()
        response = requests.post(
            url=self.active_endpoint,
            data=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.3")
    def publish_flow(
        self,
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
        parameter_dict: Optional[Dict[str, Any]] = None,
    ):
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
        publish_request = api_requests.PublishFlowRequest(
            ts_connection=self,
            flow_file_path=flow_file_path,
            flow_name=flow_name,
            project_id=project_id,
            flow_description=flow_description,
            server_address=server_address,
            port_number=port_number,
            connection_username=connection_username,
            connection_password=connection_password,
            embed_credentials_flag=embed_credentials_flag,
            oauth_flag=oauth_flag,
        )
        self.active_request, content_type = publish_request.get_request()
        self.active_headers, parameter_dict = publish_request.publish_prep(content_type, parameter_dict=parameter_dict)
        self.active_endpoint = api_endpoints.FlowEndpoint(
            ts_connection=self, publish_flow=True, parameter_dict=parameter_dict
        ).get_endpoint()
        response = requests.post(
            url=self.active_endpoint,
            data=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # metadata methods

    @decorators.verify_api_method_exists("3.5")
    def query_database(self, database_id):
        """
        Query details for the specified database.
        :param str database_id: the database ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DatabaseEndpoint(
            self, query_database=True, database_id=database_id
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def query_databases(self):
        """
        Queries details for databases stored on Tableau Server.
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DatabaseEndpoint(self, query_databases=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def update_database(
        self,
        database_id,
        certification_status=None,
        certification_note=None,
        new_description_value=None,
        new_contact_id=None,
    ):
        """
        Updates the details for the specified database.
        :param str database_id: the database ID
        :param bool certification_status: certifies (True) or removes certification (False) for the specified database
        :param str certification_note: custom text to accompany the certification status
        :param str new_description_value: custom text describing the database
        :param str new_contact_id: the ID for the Tableau Server user who is the contact for the specified database
        :return: HTTP response
        """
        self.active_request = api_requests.UpdateDatabaseRequest(
            self,
            certification_status=certification_status,
            certification_note=certification_note,
            new_description_value=new_description_value,
            new_contact_id=new_contact_id,
        ).get_request()
        self.active_endpoint = api_endpoints.DatabaseEndpoint(
            self, database_id=database_id, update_database=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def remove_database(self, database_id):
        """
        Removes the database asset.
        :param str database_id: the database ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DatabaseEndpoint(
            self, database_id=database_id, remove_database=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def query_table(self, table_id):
        """
        Queries details for the specified database table.
        :param str table_id: the table ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.TableEndpoint(self, table_id=table_id, query_table=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def query_tables(self):
        """
        Queries details for all tables on the active site.
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.TableEndpoint(self, query_tables=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def update_table(
        self,
        table_id,
        certification_status=None,
        certification_note=None,
        new_description_value=None,
        new_contact_id=None,
    ):
        """
        Updates details for the specified database table.
        :param str table_id: the table ID
        :param bool certification_status: certifies (True) or removes certification (False) for the specified table
        :param str certification_note: custom text to accompany the certification status
        :param str new_description_value: custom text describing the table
        :param str new_contact_id: the ID for the Tableau Server user who is the contact for the specified database
        :return: HTTP response
        """
        self.active_request = api_requests.UpdateTableRequest(
            self,
            certification_status=certification_status,
            certification_note=certification_note,
            new_description_value=new_description_value,
            new_contact_id=new_contact_id,
        ).get_request()
        self.active_endpoint = api_endpoints.TableEndpoint(self, table_id=table_id, update_table=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def remove_table(self, table_id):
        """
        Removes the database table asset.
        :param str table_id:
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.TableEndpoint(self, table_id=table_id, remove_table=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def query_table_column(self, table_id, column_id):
        """
        Queries details for the specified column in the specified database table.
        :param str table_id: the database table ID
        :param str column_id: the column ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.ColumnEndpoint(
            self, table_id=table_id, column_id=column_id, query_column=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def query_table_columns(self, table_id):
        """
        Queries details for all columns in the specified database table.
        :param str table_id: the database table ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.ColumnEndpoint(self, table_id=table_id, query_columns=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def update_column(self, table_id, column_id, new_description_value=None):
        """
        Updates details for the specified column in the specified database table.
        :param str table_id: the database table ID
        :param str column_id: the column ID
        :param str new_description_value: custom text describing the column
        :return: HTTP response
        """
        self.active_request = api_requests.UpdateColumnRequest(
            self, new_description_value=new_description_value
        ).get_request()
        self.active_endpoint = api_endpoints.ColumnEndpoint(
            self, table_id=table_id, column_id=column_id, update_column=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def remove_column(self, table_id, column_id):
        """
        Removes the specified column asset.
        :param str table_id: the database table ID
        :param str column_id: the column ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.ColumnEndpoint(
            self, table_id=table_id, column_id=column_id, remove_column=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def add_data_quality_warning(self, content_type, content_id, warning_type, message, status=None):
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
        self.active_request = api_requests.AddDQWarningRequest(
            self, warning_type=warning_type, message=message, status=status
        ).get_request()
        self.active_endpoint = api_endpoints.DQWarningEndpoint(
            self, content_type=content_type, content_id=content_id, add_warning=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def query_data_quality_warning_by_id(self, warning_id):
        """
        Queries details for the specified data quality warning, identified by its ID
        :param str warning_id: the data quality warning ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DQWarningEndpoint(
            self, warning_id=warning_id, query_by_id=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    def query_data_quality_warning_by_asset(self, content_type, content_id):
        """
        Queries details for the data quality warning belonging to a specific piece of content on Tableau Server.
        :param str content_type: the content type receiving the data quality warning
        [datasource, table, flow, or database]
        :param str content_id: the content ID for the specific content receiving the data quality warning
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DQWarningEndpoint(
            self,
            content_type=content_type,
            content_id=content_id,
            query_by_content=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def update_data_quality_warning(self, warning_id, warning_type=None, message=None, status=None):
        """
        Updates details for the specified data quality warning.
        :param str warning_id: the data quality warning ID
        :param str warning_type: the type of data quality warning
        [Deprecated, Warning, Stale data, or Under maintenance]
        :param str message: (optional) custom text accompanying the data quality warning
        :param bool status: toggles the data quality warning on (True) or off (False)
        :return: HTTP response
        """
        self.active_request = api_requests.UpdateDQWarningRequest(
            self, warning_type=warning_type, message=message, status=status
        ).get_request()
        self.active_endpoint = api_endpoints.DQWarningEndpoint(
            self, warning_id=warning_id, update_warning=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def delete_data_quality_warning_by_id(self, warning_id):
        """
        Removes the data quality warning from Tableau Server.
        :param str warning_id: the data quality warning ID
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DQWarningEndpoint(
            self, warning_id=warning_id, delete_by_id=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def delete_data_quality_warning_by_content(self, content_type, content_id):
        """
        Removes the data quality warning from the specified piece of content on Tableau Server.
        :param str content_type: the content type receiving the data quality warning
        [datasource, table, flow, or database]
        :param str content_id: the content ID for the specific content receiving the data quality warning
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DQWarningEndpoint(
            self,
            content_type=content_type,
            content_id=content_id,
            delete_by_content=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def metadata_graphql_query(self, query):
        """
        Builds a GraphQL query to run against the Metadata API.
        :param str query: the GraphQL query body (raw text)
        :return: HTTP response
        """
        self.active_request = api_requests.GraphqlRequest(self, query).get_request()
        self.active_endpoint = api_endpoints.GraphqlEndpoint(self).get_endpoint()
        self.active_headers = self.graphql_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # encryption methods

    @decorators.verify_api_method_exists("3.5")
    def encrypt_extracts(self):
        """
        Encrypts all extracts on the active site (encrypts .hyper extracts at rest).
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.EncryptionEndpoint(self, encrypt_extracts=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def decrypt_extracts(self):
        """
        Decrypts all extracts on the active site (decrypts .hyper extracts).
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.EncryptionEndpoint(self, decrypt_extracts=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def reencrypt_extracts(self):
        """
        Re-encrypts all .hyper extracts on the active site with new encryption keys.
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.EncryptionEndpoint(self, reencrypt_extracts=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    # extract methods

    @decorators.verify_api_method_exists("3.5")
    def create_extract_for_datasource(self, datasource_id, encryption_flag=False):
        """
        Creates an extract for the specified published datasource.
        :param str datasource_id: the ID of the datasource being converted into an extract
        :param bool encryption_flag: True if encrypting the new extract, False otherwise
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self,
            datasource_id=datasource_id,
            encryption_flag=encryption_flag,
            create_extract=True,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def delete_extract_from_datasource(self, datasource_id):
        """
        Deletes an extract for the specified published datasource.
        :param str datasource_id: the ID of the datasource being converted from an extract to a live connection
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.DatasourceEndpoint(
            ts_connection=self, datasource_id=datasource_id, delete_extract=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def create_extracts_for_workbook(
        self,
        workbook_id,
        encryption_flag=False,
        extract_all_datasources_flag=True,
        datasource_ids=None,
    ):
        """
        Creates extracts for all embedded datasources or a subset of specified embedded datasources.
        :param str workbook_id: the ID of the workbook whose embedded datasources are being converted
        :param bool encryption_flag: True if the new extracts will be encrypted, False otherwise
        :param bool extract_all_datasources_flag: True if extracting all datasources, False otherwise
        :param list datasource_ids: a list of datasource IDs if only converting a subset of datasources to extracts
        :return: HTTP response
        """
        self.active_request = api_requests.CreateExtractsForWorkbookRequest(
            ts_connection=self,
            extract_all_datasources_flag=extract_all_datasources_flag,
            datasource_ids=datasource_ids,
        ).get_request()
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self,
            workbook_id=workbook_id,
            create_extracts=True,
            encryption_flag=encryption_flag,
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.5")
    def delete_extracts_from_workbook(self, workbook_id):
        """
        Deletes all extracts from the workbook; the connections are converted from extract to live.
        :param str workbook_id: the ID of the workbook whose extracts will be deleted
        :return: HTTP response
        """
        self.active_request = {"datasources": {"includeAll": True}}
        self.active_endpoint = api_endpoints.WorkbookEndpoint(
            ts_connection=self, workbook_id=workbook_id, delete_extracts=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    #  webhook methods

    @decorators.verify_api_method_exists("3.6")
    def create_webhook(self, webhook_name=None, webhook_source_api_event_name=None, url=None):
        """
        Creates a new webhook for a site.
        :param str webhook_name: the name of the new webhook
        :param str webhook_source_api_event_name: the API event name for the source event
        :param str url: the destination URL for the webhook; must be https and have a valid certificate
        :return: HTTP response
        """
        self.active_request = api_requests.CreateWebhookRequest(
            self,
            webhook_name=webhook_name,
            webhook_source_api_event_name=webhook_source_api_event_name,
            http_request_method="POST",
            url=url,
        ).get_request()
        self.active_endpoint = api_endpoints.WebhookEndpoint(self, create_webhook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(
            url=self.active_endpoint,
            json=self.active_request,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.6")
    def query_webhook(self, webhook_id):
        """
        Queries information for the specified webhook.
        :param str webhook_id: the ID of the webhook being queried
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.WebhookEndpoint(
            self, webhook_id=webhook_id, query_webhook=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.6")
    def query_webhooks(self):
        """
        Queries all webhooks for the active site.
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.WebhookEndpoint(self, query_webhook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.6")
    def test_webhook(self, webhook_id):
        """
        Tests the specified webhook, sending a payload to the webhook's destination URL.
        :param str webhook_id: the ID of the webhook being tested
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.WebhookEndpoint(
            self, webhook_id=webhook_id, test_webhook=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response

    @decorators.verify_api_method_exists("3.6")
    def delete_webhook(self, webhook_id):
        """
        Deletes the specified webhook.
        :param str webhook_id: the ID of the webhook being deleted
        :return: HTTP response
        """
        self.active_endpoint = api_endpoints.WebhookEndpoint(
            self, webhook_id=webhook_id, delete_webhook=True
        ).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(
            url=self.active_endpoint,
            headers=self.active_headers,
            verify=self.ssl_verify,
        )
        response = self._set_response_encoding(response=response)
        return response
