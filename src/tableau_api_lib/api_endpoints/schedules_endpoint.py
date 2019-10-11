from tableau_api_lib.api_endpoints import BaseEndpoint


class SchedulesEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 schedule_id=None,
                 create_schedule=False,
                 query_schedules=False,
                 query_extract_schedules=False,
                 update_schedule=False,
                 delete_schedule=False,
                 add_datasource=False,
                 add_workbook=False,
                 add_flow=False,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API schedules methods.
        :param class ts_connection: the Tableau Server connection object
        :param str schedule_id: the schedule ID.
        :param bool create_schedule: True if creating a schedule, False otherwise
        :param bool query_schedules: True if querying all schedules, False otherwise
        :param bool query_extract_schedules: True if querying all extract schedules, False otherwise
        :param bool update_schedule: True if updating a specific schedule, False otherwise
        :param bool delete_schedule: True if deleting a specific schedule, False otherwise
        :param bool add_datasource: True if adding a datasource, False otherwise
        :param bool add_workbook: True if adding a workbook, False otherwise
        :param bool add_flow: True if adding a flow, False otherwise
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._schedule_id = schedule_id
        self._create_schedule = create_schedule
        self._query_schedules = query_schedules
        self._query_extract_schedules = query_extract_schedules
        self._update_schedule = update_schedule
        self._delete_schedule = delete_schedule
        self._add_datasource = add_datasource
        self._add_workbook = add_workbook
        self._add_flow = add_flow
        self._parameter_dict = parameter_dict

    @property
    def mutually_exclusive_params(self):
        return [
            self._create_schedule,
            self._query_schedules,
            self._query_extract_schedules,
            self._update_schedule,
            self._delete_schedule,
            self._add_datasource,
            self._add_workbook,
            self._add_flow
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_schedule_url(self):
        return "{0}/api/{1}/schedules".format(self._connection.server,
                                              self._connection.api_version)

    @property
    def base_site_schedule_id_url(self):
        return "{0}/api/{1}/sites/{2}/schedules/{3}".format(self._connection.server,
                                                            self._connection.api_version,
                                                            self._connection.site_id,
                                                            self._schedule_id)

    @property
    def base_schedule_id_url(self):
        return "{0}/{1}".format(self.base_schedule_url,
                                self._schedule_id)

    @property
    def base_schedule_datasource_url(self):
        return "{0}/datasources".format(self.base_site_schedule_id_url)

    @property
    def base_schedule_workbook_url(self):
        return "{0}/workbooks".format(self.base_site_schedule_id_url)

    @property
    def base_schedule_flow_url(self):
        return "{0}/flows".format(self.base_site_schedule_id_url)

    @property
    def base_schedule_extracts_url(self):
        return "{0}/extracts".format(self.base_schedule_id_url)

    def get_endpoint(self):
        url = None
        if self._schedule_id:
            if self._add_datasource:
                url = self.base_schedule_datasource_url
            elif self._add_workbook:
                url = self.base_schedule_workbook_url
            elif self._add_flow:
                url = self.base_schedule_flow_url
            elif self._update_schedule or self._delete_schedule:
                url = self.base_schedule_id_url
            elif self._query_extract_schedules:
                url = self.base_schedule_extracts_url
            else:
                self._invalid_parameter_exception()
        else:
            url = self.base_schedule_url

        return self._append_url_parameters(url)
