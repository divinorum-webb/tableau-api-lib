import requests

from .endpoints import AuthEndpoint, DataAlertEndpoint, DatasourceEndpoint, \
    FavoritesEndpoint, FileUploadEndpoint, FlowEndpoint, GroupEndpoint, JobsEndpoint, PermissionsEndpoint, \
    ProjectEndpoint, SchedulesEndpoint, SiteEndpoint, SubscriptionsEndpoint, UserEndpoint, TasksEndpoint, \
    ViewEndpoint, WorkbookEndpoint
from .requests import AddDatasourcePermissionsRequest, AddDatasourceToFavoritesRequest, \
    AddDatasourceToScheduleRequest, AddDefaultPermissionsRequest, AddFlowPermissionsRequest, \
    AddFlowToScheduleRequest, AddProjectPermissionsRequest, AddProjectToFavoritesRequest, \
    AddTagsRequest, AddUserToAlertRequest, AddUserToGroupRequest, AddUserToSiteRequest, \
    AddViewPermissionsRequest, AddViewToFavoritesRequest, AddWorkbookPermissionsRequest, \
    AddWorkbookToFavoritesRequest, AddWorkbookToScheduleRequest, CreateGroupRequest, \
    CreateProjectRequest, CreateScheduleRequest, CreateSiteRequest, CreateSubscriptionRequest, \
    EmptyRequest, PublishDatasourceRequest, PublishFlowRequest, PublishWorkbookRequest, \
    SignInRequest, SwitchSiteRequest, UpdateDataAlertRequest, UpdateDatasourceConnectionRequest, \
    UpdateDatasourceRequest, UpdateFlowConnectionRequest, UpdateFlowRequest, UpdateGroupRequest, \
    UpdateProjectRequest, UpdateScheduleRequest, UpdateSiteRequest, UpdateSubscriptionRequest, \
    UpdateUserRequest, UpdateWorkbookConnectionRequest, UpdateWorkbookRequest
from .decorators import verify_response, verify_signed_in


class TableauServerConnection:
    def __init__(self,
                 config_json,
                 env='tableau_prod'):
        """
        :param config_json:     A configuration dict or JSON file. This is typically a JSON file that defines the
                                Tableau Server configuration details.
        :type config_json:      JSON or dict
        :param env:             The environment from the configuration file to use. This allows for multiple
                                configurations to exist within the configuration file / dict, which could hold details
                                for other servers (dev, test, prod, other Tableau Server instance, etc.)
        :type env:              string
        """
        self._config = config_json
        self._env = env
        self.__auth_token = None
        self.__site_id = None
        self.__site_url = None
        self.__user_id = None
        self.active_endpoint = None
        self.active_request = None
        self.active_headers = None

    @property
    def server(self):
        return self._config[self._env]['server']

    @property
    def api_version(self):
        return self._config[self._env]['api_version']

    @property
    def username(self):
        return self._config[self._env]['username']

    @property
    def password(self):
        return self._config[self._env]['password']

    @property
    def site_name(self):
        return self._config[self._env]['site_name']

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
    def auth_token(self):
        return self.__auth_token

    @auth_token.setter
    def auth_token(self, token_value):
        if token_value != self.__auth_token or token_value is None:
            self.__auth_token = token_value
        else:
            raise Exception('You are already signed in with a valid auth token.')

    @property
    def site_id(self):
        return self.__site_id

    @site_id.setter
    def site_id(self, site_id_value):
        if self.site_id != site_id_value:
            self.__site_id = site_id_value
        else:
            raise Exception('This Tableau Server connection is already connected the specified site.')

    @property
    def site_url(self):
        return self._config[self._env]['site_url']

    @site_url.setter
    def site_url(self, content_url):
        if self.site_url != content_url:
            self.__site_url = content_url
        else:
            raise Exception('The current Tableau Server site already uses this content URL ({}).'
                            .format(content_url))

    @property
    def user_id(self):
        return self.__user_id

    @user_id.setter
    def user_id(self, user_id_value):
        self.__user_id = user_id_value

    # authentication

    def sign_in(self):
        request = SignInRequest(ts_connection=self, username=self.username, password=self.password).get_request()
        endpoint = AuthEndpoint(ts_connection=self, sign_in=True).get_endpoint()
        response = requests.post(url=endpoint, json=request, headers=self.sign_in_headers)
        if response.status_code == 200:
            self.auth_token = response.json()['credentials']['token']
            self.site_id = response.json()['credentials']['site']['id']
            self.user_id = response.json()['credentials']['user']['id']
        return response

    @verify_signed_in
    def sign_out(self):
        endpoint = AuthEndpoint(ts_connection=self, sign_out=True).get_endpoint()
        response = requests.post(url=endpoint, headers=self.x_auth_header)
        if response.status_code == 204:
            self.auth_token = None
            self.site_id = None
            self.user_id = None
        return response

    @verify_signed_in
    def switch_site(self, content_url):
        self.active_request = SwitchSiteRequest(ts_connection=self, site_name=content_url).get_request()
        self.active_endpoint = AuthEndpoint(ts_connection=self, switch_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        if response.status_code == 200:
            self.auth_token = response.json()['credentials']['token']
            self.site_id = response.json()['credentials']['site']['id']
            self.site_url = response.json()['credentials']['site']['contentUrl']
            self.user_id = response.json()['credentials']['user']['id']
        return response

    def server_info(self):
        self.active_endpoint = AuthEndpoint(ts_connection=self, get_server_info=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    # sites

    def create_site(self, site_name, content_url, admin_mode='ContentAndUsers', user_quota=None, storage_quota=None,
                    disable_subscriptions_flag=False, flows_enabled_flag=None, guest_access_enabled_flag=False,
                    cache_warmup_enabled_flag=False, commenting_enabled_flag=False, revision_history_enabled_flag=False,
                    revision_limit=None, subscribe_others_enabled_flag=False):
        # This method can only be called by server administrators.
        self.active_request = CreateSiteRequest(ts_connection=self,
                                                site_name=site_name, content_url=content_url, admin_mode=admin_mode,
                                                user_quota=user_quota, storage_quota=storage_quota,
                                                disable_subscriptions_flag=disable_subscriptions_flag,
                                                flows_enabled_flag=flows_enabled_flag,
                                                guest_access_enabled_flag=guest_access_enabled_flag,
                                                cache_warmup_enabled_flag=cache_warmup_enabled_flag,
                                                commenting_enabled_flag=commenting_enabled_flag,
                                                revision_history_enabled_flag=revision_history_enabled_flag,
                                                revision_limit=revision_limit,
                                                subscribe_others_enabled_flag=subscribe_others_enabled_flag)\
                                                .get_request()
        self.active_endpoint = SiteEndpoint(ts_connection=self, create_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def query_site(self, parameter_dict=None):
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            query_site=True,
                                            site_id=self.site_id,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_sites(self, parameter_dict=None):
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            query_sites=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_views_for_site(self, parameter_dict=None):
        self.active_endpoint = SiteEndpoint(ts_connection=self,
                                            query_views=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_site(self, site_id, site_name=None, content_url=None, admin_mode=None, user_quota=None,
                    state=None, storage_quota=None, disable_subscriptions_flag=False, flows_enabled_flag=None,
                    guest_access_enabled_flag=False, cache_warmup_enabled_flag=False, commenting_enabled_flag=False,
                    revision_history_enabled_flag=False, revision_limit=None, subscribe_others_enabled_flag=False):
        # This method can only be called by server administrators.
        self.active_request = UpdateSiteRequest(ts_connection=self,
                                                site_name=site_name, content_url=content_url, admin_mode=admin_mode,
                                                user_quota=user_quota, state=state, storage_quota=storage_quota,
                                                disable_subscriptions_flag=disable_subscriptions_flag,
                                                flows_enabled_flag=flows_enabled_flag,
                                                guest_access_enabled_flag=guest_access_enabled_flag,
                                                cache_warmup_enabled_flag=cache_warmup_enabled_flag,
                                                commenting_enabled_flag=commenting_enabled_flag,
                                                revision_history_enabled_flag=revision_history_enabled_flag,
                                                revision_limit=revision_limit,
                                                subscribe_others_enabled_flag=subscribe_others_enabled_flag) \
                                                .get_request()
        self.active_endpoint = SiteEndpoint(ts_connection=self, site_id=site_id, update_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_site(self, site_id=None, site_name=None, content_url=None):
        # This method can only be called by server administrators.
        self.active_endpoint = SiteEndpoint(ts_connection=self, delete_site=True, site_id=site_id, site_name=site_name,
                                            content_url=content_url).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # data driven alerts

    def delete_data_driven_alert(self, data_alert_id):
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_data_driven_alert_details(self, data_alert_id):
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 query_data_alert=True,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_data_driven_alerts(self, parameter_dict=None):
        self.active_endpoint = DataAlertEndpoint(ts_connection=self,
                                                 query_data_alerts=True,
                                                 parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def add_user_to_data_driven_alert(self, user_id, data_alert_id):
        # this appears to be broken on Tableau's side, always returning an internal server error
        self.active_request = AddUserToAlertRequest(ts_connection=self, user_id=user_id).get_request()
        self.active_endpoint = DataAlertEndpoint(ts_connection=self, add_user=True, user_id=user_id,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_user_from_data_driven_alert(self, user_id, data_alert_id):
        self.active_endpoint = DataAlertEndpoint(ts_connection=self, remove_user=True, user_id=user_id,
                                                 data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_data_driven_alert(self, data_alert_id, data_alert_subject=None, data_alert_frequency=None,
                                 data_alert_owner_id=None, is_public_flag=None):
        self.active_request = UpdateDataAlertRequest(ts_connection=self,
                                                     data_alert_subject=data_alert_subject,
                                                     data_alert_frequency=data_alert_frequency,
                                                     data_alert_owner_id=data_alert_owner_id,
                                                     is_public_flag=is_public_flag).get_request()
        self.active_endpoint = DataAlertEndpoint(ts_connection=self, data_alert_id=data_alert_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    # flows

    def query_flow(self, flow_id):
        self.active_endpoint = FlowEndpoint(ts_connection=self, flow_id=flow_id, query_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_flow(self, flow_id):
        self.active_endpoint = FlowEndpoint(ts_connection=self, flow_id=flow_id, delete_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def download_flow(self, flow_id):
        self.active_endpoint = FlowEndpoint(ts_connection=self, flow_id=flow_id, download_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_flow_connections(self, flow_id):
        self.active_endpoint = FlowEndpoint(ts_connection=self, flow_id=flow_id,
                                            query_flow_connections=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_flows_for_site(self):
        self.active_endpoint = FlowEndpoint(ts_connection=self, query_flows_for_site=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_flows_for_user(self, user_id, parameter_dict=None):
        self.active_endpoint = FlowEndpoint(ts_connection=self, user_id=user_id, query_flows_for_user=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_flow(self, flow_id, new_project_id=None, new_owner_id=None):
        self.active_request = UpdateFlowRequest(ts_connection=self,
                                                new_project_id=new_project_id,
                                                new_owner_id=new_owner_id).get_request()
        self.active_endpoint = FlowEndpoint(ts_connection=self,
                                            flow_id=flow_id,
                                            update_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def update_flow_connection(self, flow_id, connection_id, server_address=None, port=None, connection_username=None,
                               connection_password=None, embed_password_flag=None):
        """Note that you must set the connection_password='' if changing the embed_password_flag from True to False"""
        self.active_request = UpdateFlowConnectionRequest(ts_connection=self, server_address=server_address, port=port,
                                                          connection_username=connection_username,
                                                          connection_password=connection_password,
                                                          embed_password_flag=embed_password_flag).get_request()
        self.active_endpoint = FlowEndpoint(ts_connection=self, flow_id=flow_id, connection_id=connection_id,
                                            update_flow_connection=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    # projects

    def create_project(self, project_name, project_description=None, content_permissions='ManagedByOwner',
                       parent_project_id=None, parameter_dict=None):
        self.active_request = CreateProjectRequest(ts_connection=self, project_name=project_name,
                                                   project_description=project_description,
                                                   content_permissions=content_permissions,
                                                   parent_project_id=parent_project_id).get_request()
        self.active_endpoint = ProjectEndpoint(ts_connection=self, create_project=True,
                                               parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def query_projects(self, parameter_dict=None):
        self.active_endpoint = ProjectEndpoint(ts_connection=self, query_projects=True,
                                               parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_project(self, project_id, project_name=None, project_description=None, content_permissions=None,
                       parent_project_id=None):
        self.active_request = UpdateProjectRequest(ts_connection=self, project_name=project_name,
                                                   project_description=project_description,
                                                   content_permissions=content_permissions,
                                                   parent_project_id=parent_project_id).get_request()
        self.active_endpoint = ProjectEndpoint(ts_connection=self, update_project=True,
                                               project_id=project_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_project(self, project_id):
        self.active_endpoint = ProjectEndpoint(ts_connection=self, project_id=project_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # workbooks and views

    def add_tags_to_view(self, view_id, tags):
        self.active_request = AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, add_tags=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_tags_to_workbook(self, workbook_id, tags):
        self.active_request = AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                add_tags=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def query_views_for_workbook(self, workbook_id, parameter_dict=None):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, query_views=True, workbook_id=workbook_id,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_view_data(self, view_id, parameter_dict=None):
        # the CSV returned is in the response body as response.content
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, query_view_data=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_view_image(self, view_id, parameter_dict=None):
        # the image returned is in the response body as response.content
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, query_view_image=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_view_pdf(self, view_id, parameter_dict=None):
        # the PDF returned is in the response body as response.content
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, query_view_pdf=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_view_preview_image(self, workbook_id, view_id, parameter_dict=None):
        # the preview thumbnail image returned is in the response body as response.content
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id, view_id=view_id,
                                                query_workbook_view_preview_img=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_view(self, view_id):
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, query_view=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_view(self, view_id):
        """This extra method exists because the official method 'get_view' appears to break naming conventions"""
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, query_view=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_workbook(self, workbook_id, parameter_dict=None):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id, query_workbook=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_workbook_connections(self, workbook_id, parameter_dict=None):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id, query_connections=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_workbook_revisions(self, workbook_id, parameter_dict=None):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                get_workbook_revisions=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def remove_workbook_revision(self, workbook_id, revision_number):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                revision_number=revision_number,
                                                remove_workbook_revision=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_workbook_preview_image(self, workbook_id, parameter_dict=None):
        # the preview image returned is in the response body as response.content
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                query_workbook_preview_img=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_workbooks_for_site(self, parameter_dict=None):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, query_workbooks=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_workbooks_for_user(self, user_id, parameter_dict=None):
        self.active_endpoint = UserEndpoint(ts_connection=self, user_id=user_id, query_workbooks_for_user=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def download_workbook(self, workbook_id, parameter_dict=None):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id, download_workbook=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def download_workbook_pdf(self, workbook_id, parameter_dict=None):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id, download_workbook_pdf=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def download_workbook_revision(self, workbook_id, revision_number, parameter_dict=None):
        # this method only works for workbook versions that are NOT the current version
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                revision_number=revision_number,
                                                download_workbook_revision=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_workbook(self, workbook_id, show_tabs_flag=None, project_id=None, owner_id=None):
        self.active_request = UpdateWorkbookRequest(ts_connection=self, show_tabs_flag=show_tabs_flag,
                                                    project_id=project_id, owner_id=owner_id).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                update_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def update_workbook_connection(self, workbook_id, connection_id, server_address=None, port=None,
                                   connection_username=None,
                                   connection_password=None, embed_password_flag=None, parameter_dict=None):
        # fails to execute correctly on Tableau Server's side
        self.active_request = UpdateWorkbookConnectionRequest(ts_connection=self, server_address=server_address,
                                                              port=port,
                                                              connection_username=connection_username,
                                                              connection_password=connection_password,
                                                              embed_password_flag=embed_password_flag).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                connection_id=connection_id,
                                                update_workbook_connection=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def update_workbook_now(self, workbook_id, ):
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                refresh_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_workbook(self, workbook_id):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id,
                                                delete_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_tag_from_view(self, view_id, tag_name):
        self.active_endpoint = ViewEndpoint(ts_connection=self, view_id=view_id, tag_name=tag_name,
                                            delete_tag=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_tag_from_workbook(self, workbook_id, tag_name):
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, workbook_id=workbook_id, tag_name=tag_name,
                                                delete_tag=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # data sources

    def add_tags_to_data_source(self, datasource_id, tags):
        self.active_request = AddTagsRequest(ts_connection=self, tags=tags).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  add_tags=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_tag_from_data_source(self, datasource_id, tag_name):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id, tag_name=tag_name,
                                                  delete_tag=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_data_source(self, datasource_id):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  query_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_data_sources(self, parameter_dict=None):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, query_datasources=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_data_source_connections(self, datasource_id):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  query_datasource_connections=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_data_source_revisions(self, datasource_id, parameter_dict=None):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  get_datasource_revisions=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def download_data_source(self, datasource_id, parameter_dict=None):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self,
                                                  datasource_id=datasource_id,
                                                  download_datasource=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def download_data_source_revision(self, datasource_id, revision_number, parameter_dict=None):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  revision_number=revision_number,
                                                  download_datasource_revision=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_data_source(self, datasource_id, new_project_id=None, new_owner_id=None, is_certified_flag=None,
                           certification_note=None):
        """
        Note that assigning an embedded extract will remain in the same project as its workbook,
        even if the response indicates it has moved
        """
        self.active_request = UpdateDatasourceRequest(ts_connection=self, new_project_id=new_project_id,
                                                      new_owner_id=new_owner_id,
                                                      is_certified_flag=is_certified_flag,
                                                      certification_note=certification_note).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  update_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def update_data_source_connection(self, datasource_id, connection_id, server_address=None, port=None,
                                      connection_username=None,
                                      connection_password=None, embed_password_flag=None):
        """Note that you must set the connection_password='' if changing the embed_password_flag from True to False"""
        self.active_request = UpdateDatasourceConnectionRequest(ts_connection=self, server_address=server_address,
                                                                port=port,
                                                                connection_username=connection_username,
                                                                connection_password=connection_password,
                                                                embed_password_flag=embed_password_flag).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  connection_id=connection_id,
                                                  update_datasource_connection=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def update_data_source_now(self, datasource_id):
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  refresh_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_data_source(self, datasource_id):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  delete_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def remove_data_source_revision(self, datasource_id, revision_number):
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, datasource_id=datasource_id,
                                                  revision_number=revision_number,
                                                  remove_datasource_revision=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # users and groups

    def create_group(self, new_group_name, active_directory_group_name=None, active_directory_domain_name=None,
                     default_site_role=None, parameter_dict=None):
        self.active_request = CreateGroupRequest(ts_connection=self, new_group_name=new_group_name,
                                                 active_directory_group_name=active_directory_group_name,
                                                 active_directory_domain_name=active_directory_domain_name,
                                                 default_site_role=default_site_role).get_request()
        self.active_endpoint = GroupEndpoint(ts_connection=self, create_group=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_user_to_group(self, group_id, user_id):
        self.active_request = AddUserToGroupRequest(ts_connection=self, user_id=user_id).get_request()
        self.active_endpoint = GroupEndpoint(ts_connection=self, group_id=group_id, add_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_user_to_site(self, user_name, site_role, auth_setting=None):
        self.active_request = AddUserToSiteRequest(ts_connection=self, user_name=user_name,
                                                   site_role=site_role, auth_setting=auth_setting).get_request()
        self.active_endpoint = UserEndpoint(ts_connection=self, add_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def get_users_in_group(self, group_id, parameter_dict=None):
        self.active_endpoint = GroupEndpoint(ts_connection=self, group_id=group_id, get_users=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_users_on_site(self, parameter_dict=None):
        self.active_endpoint = UserEndpoint(ts_connection=self, query_users=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_groups(self, parameter_dict=None):
        self.active_endpoint = GroupEndpoint(ts_connection=self, query_groups=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_user_on_site(self, user_id):
        self.active_endpoint = UserEndpoint(ts_connection=self, user_id=user_id, query_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_group(self, group_id, new_group_name=None, active_directory_group_name=None,
                     active_directory_domain_name=None,
                     default_site_role=None, parameter_dict=None):
        self.active_request = UpdateGroupRequest(ts_connection=self, new_group_name=new_group_name,
                                                 active_directory_group_name=active_directory_group_name,
                                                 active_directory_domain_name=active_directory_domain_name,
                                                 default_site_role=default_site_role).get_request()
        self.active_endpoint = GroupEndpoint(ts_connection=self, group_id=group_id, update_group=True,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def update_user(self, user_id, new_full_name=None, new_email=None, new_password=None, new_site_role=None,
                    new_auth_setting=None):
        self.active_request = UpdateUserRequest(ts_connection=self, new_full_name=new_full_name, new_email=new_email,
                                                new_password=new_password, new_site_role=new_site_role,
                                                new_auth_setting=new_auth_setting).get_request()
        self.active_endpoint = UserEndpoint(ts_connection=self, user_id=user_id, update_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.default_headers)
        return response

    def remove_user_from_group(self, group_id, user_id):
        self.active_endpoint = GroupEndpoint(ts_connection=self, group_id=group_id, user_id=user_id,
                                             remove_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def remove_user_from_site(self, user_id):
        self.active_endpoint = UserEndpoint(ts_connection=self, user_id=user_id, remove_user=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_group(self, group_id):
        self.active_endpoint = GroupEndpoint(ts_connection=self, group_id=group_id, delete_group=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # permissions

    def add_data_source_permissions(self, datasource_id, user_capability_dict=None, group_capability_dict=None,
                                    user_id=None, group_id=None):
        self.active_request = AddDatasourcePermissionsRequest(ts_connection=self, datasource_id=datasource_id,
                                                              user_id=user_id, group_id=group_id,
                                                              user_capability_dict=user_capability_dict,
                                                              group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='datasource',
                                                   object_id=datasource_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_flow_permissions(self, flow_id, user_capability_dict=None, group_capability_dict=None, user_id=None,
                             group_id=None):
        self.active_request = AddFlowPermissionsRequest(ts_connection=self, user_id=user_id, group_id=group_id,
                                                        user_capability_dict=user_capability_dict,
                                                        group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='flow', object_id=flow_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_project_permissions(self, project_id, user_capability_dict=None, group_capability_dict=None, user_id=None,
                                group_id=None):
        self.active_request = AddProjectPermissionsRequest(ts_connection=self, user_id=user_id, group_id=group_id,
                                                           user_capability_dict=user_capability_dict,
                                                           group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='project', object_id=project_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_default_permissions(self, project_id, project_permissions_object, group_id=None,
                                user_id=None, user_capability_dict=None, group_capability_dict=None):
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
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_view_permissions(self, view_id, user_capability_dict=None, group_capability_dict=None, user_id=None,
                             group_id=None):
        self.active_request = AddViewPermissionsRequest(ts_connection=self, view_id=view_id, user_id=user_id,
                                                        group_id=group_id,
                                                        user_capability_dict=user_capability_dict,
                                                        group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='view', object_id=view_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_workbook_permissions(self, workbook_id, user_capability_dict=None, group_capability_dict=None, user_id=None,
                                 group_id=None):
        self.active_request = AddWorkbookPermissionsRequest(ts_connection=self, workbook_id=workbook_id,
                                                            user_id=user_id, group_id=group_id,
                                                            user_capability_dict=user_capability_dict,
                                                            group_capability_dict=group_capability_dict).get_request()
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='workbook', object_id=workbook_id,
                                                   add_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers.copy()
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def query_data_source_permissions(self, datasource_id):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='datasource',
                                                   object_id=datasource_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_flow_permissions(self, flow_id):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='flow', object_id=flow_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_project_permissions(self, project_id):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='project', object_id=project_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_default_permissions(self, project_id, project_permissions_object):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, project_id=project_id,
                                                   project_permissions_object=project_permissions_object,
                                                   query_default_project_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_view_permissions(self, view_id):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='view', object_id=view_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_workbook_permissions(self, workbook_id):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='workbook', object_id=workbook_id,
                                                   query_object_permissions=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_data_source_permission(self, datasource_id, delete_permissions_object, delete_permissions_object_id,
                                      capability_name, capability_mode):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='datasource',
                                                   object_id=datasource_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_flow_permission(self, flow_id, delete_permissions_object, delete_permissions_object_id, capability_name,
                               capability_mode):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='flow', object_id=flow_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_project_permission(self, project_id, delete_permissions_object, delete_permissions_object_id,
                                  capability_name, capability_mode):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self, object_type='project', object_id=project_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_default_permission(self, project_id,
                                  project_permissions_object,
                                  delete_permissions_object,
                                  delete_permissions_object_id,
                                  capability_name,
                                  capability_mode):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   project_id=project_id,
                                                   project_permissions_object=project_permissions_object,
                                                   delete_default_project_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        print('endpoint: ', self.active_endpoint)
        print('request: ', self.active_request)
        print('headers: ', self.active_headers)
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_view_permission(self,
                               view_id,
                               delete_permissions_object,
                               delete_permissions_object_id,
                               capability_name,
                               capability_mode):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='view',
                                                   object_id=view_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_workbook_permission(self, workbook_id, delete_permissions_object, delete_permissions_object_id,
                                   capability_name, capability_mode):
        self.active_endpoint = PermissionsEndpoint(ts_connection=self,
                                                   object_type='workbook',
                                                   object_id=workbook_id,
                                                   delete_object_permissions=True,
                                                   delete_permissions_object=delete_permissions_object,
                                                   delete_permissions_object_id=delete_permissions_object_id,
                                                   capability_name=capability_name,
                                                   capability_mode=capability_mode).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # jobs, tasks, and schedules

    def add_data_source_to_schedule(self, datasource_id, schedule_id):
        self.active_request = AddDatasourceToScheduleRequest(ts_connection=self,
                                                             datasource_id=datasource_id).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, schedule_id=schedule_id,
                                                 add_datasource=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_flow_task_to_schedule(self, flow_id, schedule_id):
        self.active_request = AddFlowToScheduleRequest(ts_connection=self, flow_id=flow_id).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, schedule_id=schedule_id,
                                                 add_flow=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_workbook_to_schedule(self, workbook_id, schedule_id):
        self.active_request = AddWorkbookToScheduleRequest(ts_connection=self, workbook_id=workbook_id).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, schedule_id=schedule_id,
                                                 add_workbook=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def cancel_job(self, job_id):
        self.active_endpoint = JobsEndpoint(ts_connection=self, job_id=job_id, cancel_job=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_job(self, job_id):
        self.active_endpoint = JobsEndpoint(ts_connection=self, job_id=job_id, query_job=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_jobs(self, parameter_dict=None):
        self.active_endpoint = JobsEndpoint(ts_connection=self, query_jobs=True,
                                            parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_extract_refresh_task(self, task_id):
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, get_refresh_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_extract_refresh_tasks(self):
        self.active_endpoint = TasksEndpoint(ts_connection=self, get_refresh_tasks=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_flow_run_task(self, task_id):
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, get_flow_run_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_flow_run_tasks(self):
        self.active_endpoint = TasksEndpoint(ts_connection=self, get_flow_run_tasks=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def create_schedule(self, schedule_name, schedule_priority=50, schedule_type='Extract',
                        schedule_execution_order='Parallel', schedule_frequency='Weekly',
                        start_time='07:00:00', end_time='23:00:00', interval_expression_dict={'weekDay': 'Monday'}):
        self.active_request = CreateScheduleRequest(ts_connection=self, schedule_name=schedule_name,
                                                    schedule_priority=schedule_priority, schedule_type=schedule_type,
                                                    schedule_execution_order=schedule_execution_order,
                                                    schedule_frequency=schedule_frequency,
                                                    start_time=start_time, end_time=end_time,
                                                    interval_expression_dict=interval_expression_dict).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, create_schedule=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def query_extract_refresh_tasks(self, schedule_id, parameter_dict=None):
        self.active_endpoint = TasksEndpoint(ts_connection=self, query_schedule_refresh_tasks=True,
                                             schedule_id=schedule_id,
                                             parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_schedules(self, parameter_dict=None):
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, query_schedules=True,
                                                 parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def run_extract_refresh_task(self, task_id):
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, run_refresh_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def run_flow_task(self, task_id):
        self.active_request = EmptyRequest(ts_connection=self).get_request()
        self.active_endpoint = TasksEndpoint(ts_connection=self, task_id=task_id, run_flow_task=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def update_schedule(self, schedule_id, schedule_name=None, schedule_priority=None, schedule_type=None,
                        schedule_execution_order=None,
                        schedule_frequency=None, start_time=None, end_time=None, interval_expression_dict=None):
        self.active_request = UpdateScheduleRequest(ts_connection=self, schedule_name=schedule_name,
                                                    schedule_priority=schedule_priority, schedule_type=schedule_type,
                                                    schedule_execution_order=schedule_execution_order,
                                                    schedule_frequency=schedule_frequency,
                                                    start_time=start_time, end_time=end_time,
                                                    interval_expression_dict=interval_expression_dict).get_request()
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, schedule_id=schedule_id,
                                                 update_schedule=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_schedule(self, schedule_id):
        self.active_endpoint = SchedulesEndpoint(ts_connection=self, schedule_id=schedule_id,
                                                 delete_schedule=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # subscriptions

    def create_subscription(self, subscription_subject, content_type, content_id, schedule_id, user_id):
        self.active_request = CreateSubscriptionRequest(ts_connection=self, subscription_subject=subscription_subject,
                                                        content_type=content_type,
                                                        content_id=content_id, schedule_id=schedule_id,
                                                        user_id=user_id).get_request()
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self, create_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def query_subscription(self, subscription_id):
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self, subscription_id=subscription_id,
                                                     query_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def query_subscriptions(self, parameter_dict=None):
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self, query_subscriptions=True,
                                                     parameter_dict=parameter_dict).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    def update_subscription(self, subscription_id, new_subscription_subject=None, new_schedule_id=None):
        self.active_request = UpdateSubscriptionRequest(ts_connection=self, new_schedule_id=new_schedule_id,
                                                        new_subscription_subject=new_subscription_subject).get_request()
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self, subscription_id=subscription_id,
                                                     update_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_subscription(self, subscription_id):
        self.active_endpoint = SubscriptionsEndpoint(ts_connection=self, subscription_id=subscription_id,
                                                     delete_subscription=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    # favorites

    def add_data_source_to_favorites(self, datasource_id, user_id, favorite_label):
        self.active_request = AddDatasourceToFavoritesRequest(ts_connection=self, datasource_id=datasource_id,
                                                              favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_project_to_favorites(self, project_id, user_id, favorite_label):
        self.active_request = AddProjectToFavoritesRequest(ts_connection=self, project_id=project_id,
                                                           favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_view_to_favorites(self, view_id, user_id, favorite_label):
        self.active_request = AddViewToFavoritesRequest(ts_connection=self, view_id=view_id,
                                                        favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def add_workbook_to_favorites(self, workbook_id, user_id, favorite_label):
        self.active_request = AddWorkbookToFavoritesRequest(ts_connection=self, workbook_id=workbook_id,
                                                            favorite_label=favorite_label).get_request()
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, add_to_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.put(url=self.active_endpoint, json=self.active_request, headers=self.active_headers)
        return response

    def delete_data_source_from_favorites(self, datasource_id, user_id):
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, object_type='datasource', object_id=datasource_id,
                                                 user_id=user_id, delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_project_from_favorites(self, project_id, user_id):
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, object_type='project', object_id=project_id,
                                                 user_id=user_id, delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_view_from_favorites(self, view_id, user_id):
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, object_type='view', object_id=view_id,
                                                 user_id=user_id, delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def delete_workbook_from_favorites(self, workbook_id, user_id):
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, object_type='workbook', object_id=workbook_id,
                                                 user_id=user_id, delete_from_favorites=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.delete(url=self.active_endpoint, headers=self.active_headers)
        return response

    def get_favorites_for_user(self, user_id):
        self.active_endpoint = FavoritesEndpoint(ts_connection=self, get_user_favorites=True,
                                                 user_id=user_id).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.get(url=self.active_endpoint, headers=self.active_headers)
        return response

    # publishing

    def initiate_file_upload(self):
        self.active_endpoint = FileUploadEndpoint(ts_connection=self, initiate_file_upload=True).get_endpoint()
        self.active_headers = self.default_headers
        response = requests.post(url=self.active_endpoint, headers=self.active_headers)
        return response

    def append_to_file_upload(self, upload_session_id, payload, content_type):
        self.active_endpoint = FileUploadEndpoint(ts_connection=self, append_to_file_upload=True,
                                                  upload_session_id=upload_session_id).get_endpoint()
        self.active_headers = self.default_headers.copy()
        self.active_headers.update({'content-type': content_type})
        response = requests.put(url=self.active_endpoint, data=payload, headers=self.active_headers)
        return response

    def publish_data_source(self, datasource_file_path, datasource_name, project_id, connection_username=None,
                            connection_password=None,
                            embed_credentials_flag=False, oauth_flag=False, parameter_dict=None):
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
        self.active_endpoint = DatasourceEndpoint(ts_connection=self, publish_datasource=True,
                                                  parameter_dict=parameter_dict).get_endpoint()
        response = requests.post(url=self.active_endpoint, data=self.active_request, headers=self.active_headers)
        return response

    def publish_workbook(self, workbook_file_path, workbook_name, project_id, show_tabs_flag=False,
                         user_id=None, server_address=None, port_number=None, connection_username=None,
                         connection_password=None,
                         embed_credentials_flag=False, oauth_flag=False, workbook_views_to_hide=None,
                         hide_view_flag=False, parameter_dict=None):
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
        self.active_endpoint = WorkbookEndpoint(ts_connection=self, publish_workbook=True,
                                                parameter_dict=parameter_dict).get_endpoint()
        response = requests.post(url=self.active_endpoint, data=self.active_request, headers=self.active_headers)
        return response

    def publish_flow(self, flow_file_path, flow_name, project_id, flow_description=None, server_address=None,
                     port_number=None,
                     connection_username=None, connection_password=None, embed_credentials_flag=False, oauth_flag=False,
                     parameter_dict=None):
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
        response = requests.post(url=self.active_endpoint, data=self.active_request, headers=self.active_headers)
        return response
