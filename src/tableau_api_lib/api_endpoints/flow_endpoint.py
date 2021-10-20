from tableau_api_lib.api_endpoints import BaseEndpoint


class FlowEndpoint(BaseEndpoint):
    def __init__(
        self,
        ts_connection,
        flow_id=None,
        user_id=None,
        connection_id=None,
        schedule_id=None,
        query_flows_for_site=False,
        query_flows_for_user=False,
        query_flow=False,
        download_flow=False,
        publish_flow=False,
        query_flow_connections=False,
        query_flow_permissions=False,
        update_flow=False,
        update_flow_connection=False,
        add_flow_task_to_schedule=False,
        delete_flow=False,
        run_flow_now=False,
        parameter_dict=None,
    ):
        """
        Builds API endpoints for REST API flow methods.
        :param class ts_connection: the Tableau Server connection object
        :param str flow_id: the flow ID relevant to queries
        :param str user_id: the user ID relevant to queries
        :param str connection_id: the connection ID relevant to queries
        :param str schedule_id: the schedule ID relevant to queries
        :param bool query_flows_for_site: True if querying all flows on the site, False otherwise
        :param bool query_flows_for_user: True if querying all flows for a user, False otherwise
        :param bool query_flow: True if querying a specific flow, False otherwise
        :param bool query_flows_for_site: True if querying all flows on the site, False otherwise
        :param bool download_flow: True if downloading a specific flow, False otherwise
        :param bool publish_flow: True if publishing a specific flow, False otherwise
        :param bool query_flow_connections: True if querying a flow's connections, False otherwise
        :param bool query_flow_permissions: True if querying flow permissions, False otherwise
        :param bool update_flow: True if updating a specific flow, False otherwise
        :param bool update_flow_connection: True if updating a flow's connection, False otherwise
        :param bool add_flow_task_to_schedule: True if adding a flow task to an existing schedule, False otherwise
        :param bool delete_flow: True if deleting a specific flow, False otherwise
        :param bool run_flow_now: True if running the flow now, False otherwise
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the literal
        text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._flow_id = flow_id
        self._user_id = user_id
        self._connection_id = connection_id
        self._schedule_id = schedule_id
        self._query_flows_for_site = query_flows_for_site
        self._query_flows_for_user = query_flows_for_user
        self._query_flow = query_flow
        self._download_flow = download_flow
        self._publish_flow = publish_flow
        self._query_flow_connections = query_flow_connections
        self._query_flow_permissions = query_flow_permissions
        self._update_flow = update_flow
        self._update_flow_connection = update_flow_connection
        self._add_flow_task_to_schedule = add_flow_task_to_schedule
        self._delete_flow = delete_flow
        self._run_flow_now = run_flow_now
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_flows_for_site,
            self._query_flows_for_user,
            self._query_flow,
            self._download_flow,
            self._publish_flow,
            self._query_flow_connections,
            self._query_flow_permissions,
            self._update_flow,
            self._update_flow_connection,
            self._add_flow_task_to_schedule,
            self._delete_flow,
            self._run_flow_now,
        ]

    def _validate_inputs(self) -> None:
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_flow_url(self):
        return "{0}/api/{1}/sites/{2}/flows".format(
            self._connection.server, self._connection.api_version, self._connection.site_id
        )

    @property
    def base_flow_id_url(self):
        return "{0}/{1}".format(self.base_flow_url, self._flow_id)

    @property
    def base_flow_content_url(self):
        return "{0}/content".format(self.base_flow_id_url)

    @property
    def base_flow_connections_url(self):
        return "{0}/connections".format(self.base_flow_id_url)

    @property
    def base_flow_connection_id_url(self):
        return "{0}/{1}".format(self.base_flow_connections_url, self._connection_id)

    @property
    def base_flow_permissions_url(self):
        return "{0}/permissions".format(self.base_flow_id_url)

    @property
    def base_flow_user_url(self):
        return "{0}/api/{1}/sites/{2}/users/{3}/flows".format(
            self._connection.server, self._connection.api_version, self._connection.site_id, self._user_id
        )

    @property
    def base_run_flow_now_url(self):
        return "{0}/run".format(self.base_flow_id_url)

    def get_endpoint(self):
        url = None
        if self._flow_id:
            if self._query_flow and not self._delete_flow:
                url = self.base_flow_id_url
            elif self._delete_flow and not self._query_flow:
                url = self.base_flow_id_url
            elif self._download_flow:
                url = self.base_flow_content_url
            elif self._query_flow_connections:
                url = self.base_flow_connections_url
            elif self._query_flow_permissions:
                url = self.base_flow_permissions_url
            elif self._update_flow:
                url = self.base_flow_id_url
            elif self._update_flow_connection:
                url = self.base_flow_connection_id_url
            elif self._run_flow_now:
                url = self.base_run_flow_now_url
            else:
                self._invalid_parameter_exception()
        elif self._user_id:
            url = self.base_flow_user_url
        else:
            url = self.base_flow_url

        return self._append_url_parameters(url)
