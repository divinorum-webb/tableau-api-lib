from tableau.client.endpoints import BaseEndpoint


class SchedulesEndpoint(BaseEndpoint):
    """
    Schedules endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param schedule_id:         The schedule ID.
    :type schedule_id:          string
    :param create_schedule:     Boolean flag; True if creating a schedule, False otherwise.
    :type create_schedule:      boolean
    :param query_schedules:     Boolean flag; True if querying all schedules, False otherwise.
    :type query_schedules:      boolean
    :param update_schedule:     Boolean flag; True if updating a specific schedule, False otherwise.
    :type update_schedule:      boolean
    :param delete_schedule:     Boolean flag; True if deleting a specific schedule, False otherwise.
    :type delete_schedule:      boolean
    :param add_datasource:      Boolean flag; True if adding a datasource, False otherwise.
    :type add_datasource:       boolean
    :param add_workbook:        Boolean flag; True if adding a workbook, False otherwise.
    :type add_workbook:         boolean
    :param add_flow:            Boolean flag; True if adding a flow, False otherwise.
    :type add_flow:             boolean
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self,
                 ts_connection,
                 schedule_id=None,
                 create_schedule=False,
                 query_schedules=False,
                 update_schedule=False,
                 delete_schedule=False,
                 add_datasource=False,
                 add_workbook=False,
                 add_flow=False,
                 parameter_dict=None):

        super().__init__(ts_connection)
        self._schedule_id = schedule_id
        self._create_schedule = create_schedule
        self._query_schedules = query_schedules
        self._update_schedule = update_schedule
        self._delete_schedule = delete_schedule
        self._add_datasource = add_datasource
        self._add_workbook = add_workbook
        self._add_flow = add_flow
        self._parameter_dict = parameter_dict

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

    def get_endpoint(self):
        if self._schedule_id:
            if self._add_datasource and not (self._add_workbook or self._add_flow):
                url = self.base_schedule_datasource_url
            elif self._add_workbook and not (self._add_datasource or self._add_flow):
                url = self.base_schedule_workbook_url
            elif self._add_flow and not (self._add_datasource or self._add_workbook):
                url = self.base_schedule_flow_url
            elif self._update_schedule or self._delete_schedule:
                url = self.base_schedule_id_url
            else:
                self._invalid_parameter_exception()
        else:
            url = self.base_schedule_url

        return self._append_url_parameters(url)
