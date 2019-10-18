from tableau_api_lib.api_requests import BaseRequest


class UpdateDataAlertRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating database columns.
    :param class ts_connection: the Tableau Server connection object
    :param str data_alert_subject: (optional) the string to set as the new subject of the alert
    :param str data_alert_frequency: (optional) the frequency of the data-driven alert: once, frequently, hourly, daily,
    or weekly
    :param str data_alert_owner_id: (optional) the ID of the user to assign as owner of the data-driven alert
    :param bool is_public_flag: (optional) Determines the visibility of the data-driven alert. If the flag is
    True, users with access to the view containing the alert can see the alert and add themselves as recipients. If the
    flag is False, then the alert is only visible to the owner, site or server administrators, and specific users they
    add as recipients
    """
    def __init__(self,
                 ts_connection,
                 data_alert_subject=None,
                 data_alert_frequency=None,
                 data_alert_owner_id=None,
                 is_public_flag=None):
        super().__init__(ts_connection)
        self._data_alert_subject = data_alert_subject
        self._data_alert_frequency = data_alert_frequency
        self._data_alert_owner_id = data_alert_owner_id
        self._is_public_flag = is_public_flag
        self.base_update_alert_request()

    @property
    def optional_alert_param_keys(self):
        return [
            'subject',
            'frequency',
            'public'
        ]

    @property
    def optional_owner_param_keys(self):
        return ['id']

    @property
    def optional_alert_param_values(self):
        return [
            self._data_alert_subject,
            self._data_alert_frequency,
            self._is_public_flag
        ]

    @property
    def optional_owner_param_values(self):
        return [self._data_alert_owner_id]

    def base_update_alert_request(self):
        self._request_body.update({'dataAlert': {}})
        return self._request_body

    def modified_update_alert_request(self):
        self._request_body['dataAlert'].update(
            self._get_parameters_dict(
                self.optional_alert_param_keys,
                self.optional_alert_param_values))
        if self._data_alert_owner_id:
            self._request_body['dataAlert'].update({'owner': {}})
            self._request_body['dataAlert']['owner'].update(
                self._get_parameters_dict(
                    self.optional_owner_param_keys,
                    self.optional_owner_param_values))
        return self._request_body

    def get_request(self):
        return self.modified_update_alert_request()
