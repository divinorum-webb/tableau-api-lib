from tableau_api_lib.api_requests import BaseRequest


class AddFlowToScheduleRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding flows to schedules.
    :param class ts_connection: the Tableau Server connection object
    :param str flow_id: the flow ID for the flow being added to a schedule
    """
    def __init__(self,
                 ts_connection,
                 flow_id):
        super().__init__(ts_connection)
        self._flow_id = flow_id
        self.base_add_flow_request()

    @property
    def required_flow_param_keys(self):
        return ['id']

    @property
    def required_flow_param_values(self):
        return [self._flow_id]

    def base_add_flow_request(self):
        self._request_body.update({'task': {'flowRun': {}}})
        return self._request_body

    def modified_add_flow_request(self):
        self._request_body['task']['flowRun'].update({'flow': {}})
        self._request_body['task']['flowRun']['flow'].update(
            self._get_parameters_dict(self.required_flow_param_keys,
                                      self.required_flow_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_add_flow_request()
