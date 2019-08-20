from tableau.client.requests import BaseRequest


class UpdateWorkbookRequest(BaseRequest):
    """
    Update workbook request for sending API requests to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param show_tabs_flag:      (Optional) Boolean flag; True if the workbook will show views as tabs, False otherwise.
                                Default value is False.
    :type show_tabs_flag:       boolean
    :param project_id:          (Optional) The ID of a project to assign the workbook to.
    :type project_id:           string
    :param owner_id:            (Optional) The ID of the user who will own the workbook.
    :type owner_id:             string
    """
    def __init__(self,
                 ts_connection,
                 show_tabs_flag=None,
                 project_id=None,
                 owner_id=None):

        super().__init__(ts_connection)
        self._show_tabs_flag = show_tabs_flag
        self._project_id = project_id
        self._owner_id = owner_id
        self.base_update_workbook_request()

    @property
    def optional_workbook_param_keys(self):
        return ['showTabs']

    @property
    def optional_project_param_keys(self):
        return ['id']

    @property
    def optional_owner_param_keys(self):
        return ['id']

    @property
    def optional_workbook_param_values(self):
        return ['true' if self._show_tabs_flag is True else 'false' if self._show_tabs_flag is False else None]

    @property
    def optional_project_param_values(self):
        return [self._project_id]

    @property
    def optional_owner_param_values(self):
        return [self._owner_id]

    def base_update_workbook_request(self):
        self._request_body.update({'workbook': {}})
        return self._request_body

    def modified_update_workbook_request(self):
        if any(self.optional_workbook_param_values):
            self._request_body['workbook'].update(
                self._get_parameters_dict(self.optional_workbook_param_keys,
                                          self.optional_workbook_param_values))

        if any(self.optional_project_param_values):
            self._request_body['workbook'].update({'project': {}})
            self._request_body['workbook']['project'].update(
                self._get_parameters_dict(self.optional_project_param_keys,
                                          self.optional_project_param_values))

        if any(self.optional_owner_param_values):
            self._request_body['workbook'].update({'owner': {}})
            self._request_body['workbook']['owner'].update(
                self._get_parameters_dict(self.optional_owner_param_keys,
                                          self.optional_owner_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_update_workbook_request()
