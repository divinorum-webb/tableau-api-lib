from tableau_api_lib.api_requests import BaseRequest


class AddDatasourceToScheduleRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding datasources to schedules.
    :param class ts_connection: the Tableau Server connection object
    :param str datasource_id: the datasource ID
    """
    def __init__(self,
                 ts_connection,
                 datasource_id):
        super().__init__(ts_connection)
        self._datasource_id = datasource_id
        self.base_add_datasource_request()

    @property
    def required_datasource_param_keys(self):
        return ['id']

    @property
    def required_datasource_param_values(self):
        return [self._datasource_id]

    def base_add_datasource_request(self):
        self._request_body.update({'task': {'extractRefresh': {}}})
        return self._request_body

    def modified_add_datasource_request(self):
        self._request_body['task']['extractRefresh'].update({'datasource': {}})
        self._request_body['task']['extractRefresh']['datasource'].update(
            self._get_parameters_dict(self.required_datasource_param_keys,
                                      self.required_datasource_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_add_datasource_request()
