from tableau.client.endpoints import BaseEndpoint


class FlowEndpoint(BaseEndpoint):
    """
    Flow endpoint for Tableau Server API requests.

    :param ts_connection:                   The Tableau Server connection object.
    :type ts_connection:                    class
    :param flow_id:                         The flow ID relevant to queries.
    :type flow_id:                          string
    :param user_id:                         The user ID relevant to queries.
    :type user_id:                          string
    :param connection_id:                   The connection ID relevant to queries.
    :type connection_id:                    string
    :param schedule_id:                     The schedule ID relevant to queries.
    :type schedule_id:                      string
    :param query_flows_for_site:            Boolean flag; True if querying all flows on the site, False otherwise.
    :type query_flows_for_site:             boolean
    :param query_flows_for_user:            Boolean flag; True if querying all flows for a user, False otherwise.
    :type query_flows_for_user:             boolean
    :param query_flow:                      Boolean flag; True if querying a specific flow, False otherwise.
    :type query_flow:                       boolean
    :param query_flows_for_site:            Boolean flag; True if querying all flows on the site, False otherwise.
    :type query_flows_for_site:             boolean
    :param download_flow:                   Boolean flag; True if downloading a specific flow, False otherwise.
    :type download_flow:                    boolean
    :param publish_flow:                    Boolean flag; True if publishing a specific flow, False otherwise.
    :type publish_flow:                     boolean
    :param query_flow_connections:          Boolean flag; True if querying a flow's connections, False otherwise.
    :type query_flow_connections:           boolean
    :param query_flow_permissions           Boolean flag; True if querying flow permissions, False otherwise.
    :type query_flow_permissions:           boolean
    :param update_flow:                     Boolean flag; True if updating a specific flow, False otherwise.
    :type update_flow:                      boolean
    :param update_flow_connection:          Boolean flag; True if updating a flow's connection, False otherwise.
    :type update_flow_connection:           boolean
    :param add_flow_task_to_schedule:       Boolean flag; True if adding a flow task to an existing schedule,
                                            False otherwise.
    :type add_flow_task_to_schedule:        boolean
    :param delete_flow:                     Boolean flag; True if deleting a specific flow, False otherwise.
    :type delete_flow:                      boolean
    :param parameter_dict:                  Dictionary of URL parameters to append. The value in each key-value pair
                                            is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:                   dict
    """
    def __init__(self,
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
                 parameter_dict=None):

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
        self._parameter_dict = parameter_dict

    @property
    def base_flow_url(self):
        return "{0}/api/{1}/sites/{2}/flows".format(self._connection.server,
                                                    self._connection.api_version,
                                                    self._connection.site_id)

    @property
    def base_flow_id_url(self):
        return "{0}/{1}".format(self.base_flow_url,
                                self._flow_id)

    @property
    def base_flow_content_url(self):
        return "{0}/content".format(self.base_flow_id_url)

    @property
    def base_flow_connections_url(self):
        return "{0}/connections".format(self.base_flow_id_url)

    @property
    def base_flow_connection_id_url(self):
        return "{0}/{1}".format(self.base_flow_connections_url,
                                self._connection_id)

    @property
    def base_flow_permissions_url(self):
        return "{0}/permissions".format(self.base_flow_id_url)

    @property
    def base_flow_user_url(self):
        return "{0}/api/{1}/sites/{2}/users/{3}/flows".format(self._connection.server,
                                                              self._connection.api_version,
                                                              self._connection.site_id,
                                                              self._user_id)

    def get_endpoint(self):
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
            else:
                self._invalid_parameter_exception()
        elif self._user_id:
            url = self.base_flow_user_url
        else:
            url = self.base_flow_url

        return self._append_url_parameters(url)
