from tableau_api_lib.api_requests import BaseRequest


class UpdateFlowRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating flows.
    :param class ts_connection: the Tableau Server connection object
    :param str new_project_id: (optional) the ID of the project to add the data source to
    :param str new_owner_id: (optional) the ID of the user who will own the flow
    """
    def __init__(self,
                 ts_connection,
                 new_project_id=None,
                 new_owner_id=None,
                 is_certified_flag=None,
                 certification_note=None):

        super().__init__(ts_connection)
        self._new_project_id = new_project_id
        self._new_owner_id = new_owner_id
        self._is_certified_flag = is_certified_flag
        self._certification_note = certification_note
        self.base_update_flow_request()

    @property
    def optional_project_param_keys(self):
        return ['id']

    @property
    def optional_owner_param_keys(self):
        return ['id']

    @property
    def optional_project_param_values(self):
        return [self._new_project_id]

    @property
    def optional_owner_param_values(self):
        return [self._new_owner_id]

    def base_update_flow_request(self):
        self._request_body.update({'flow': {}})
        return self._request_body

    def modified_update_flow_request(self):

        if any(self.optional_project_param_keys):
            self._request_body['flow'].update({'project': {}})
            self._request_body['flow']['project'].update(
                self._get_parameters_dict(self.optional_project_param_keys,
                                          self.optional_project_param_values))

        if any(self.optional_owner_param_keys):
            self._request_body['flow'].update({'owner': {}})
            self._request_body['flow']['owner'].update(
                self._get_parameters_dict(self.optional_owner_param_keys,
                                          self.optional_owner_param_values))

        return self._request_body

    def get_request(self):
        return self.modified_update_flow_request()
