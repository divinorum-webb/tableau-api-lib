from tableau_api_lib.api_requests import BaseRequest


class AddWorkbookToScheduleRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding workbooks to schedules.
    :param class ts_connection: the Tableau Server connection object
    :param str workbook_id: the workbook ID
    """
    def __init__(self,
                 ts_connection,
                 workbook_id):
        super().__init__(ts_connection)
        self._workbook_id = workbook_id
        self.base_add_workbook_request()

    @property
    def required_workbook_param_keys(self):
        return ['id']

    @property
    def required_workbook_param_values(self):
        return [self._workbook_id]

    def base_add_workbook_request(self):
        self._request_body.update({'task': {'extractRefresh': {}}})
        return self._request_body

    def modified_add_workbook_request(self):
        self._request_body['task']['extractRefresh'].update({'workbook': {}})
        self._request_body['task']['extractRefresh']['workbook'].update(
            self._get_parameters_dict(self.required_workbook_param_keys,
                                      self.required_workbook_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_add_workbook_request()
