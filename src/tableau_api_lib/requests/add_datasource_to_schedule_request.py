from tableau.client.requests import BaseRequest


class AddDatasourceToScheduleRequest(BaseRequest):
    """
    Add datasource to schedule request for generating API requests to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param datasource_id:       The datasource ID.
    :type datasource_id:        string
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
