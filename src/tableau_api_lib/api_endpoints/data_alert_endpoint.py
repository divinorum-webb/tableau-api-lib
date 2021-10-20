from tableau_api_lib.api_endpoints import BaseEndpoint


class DataAlertEndpoint(BaseEndpoint):
    def __init__(self, 
                 ts_connection,
                 query_data_alerts=False,
                 query_data_alert=False,
                 data_alert_id=None, 
                 user_id=None, 
                 add_user=False, 
                 remove_user=False,
                 delete_data_alert=False,
                 parameter_dict=None):
        """
        Data Alert endpoint for Tableau Server API api_requests.
        :param class ts_connection: the Tableau Server connection object
        :param bool query_data_alerts: True if querying all data alerts, False otherwise
        :param bool query_data_alert: True if querying a specific data alert, False otherwise
        :param str data_alert_id: the data alert ID
        :param str user_id: the user ID
        :param bool add_user: True if adding a user to the alert, False otherwise
        :param bool remove_user: True if removing a user from the alert; False otherwise
        :param bool delete_data_alert: True if deleting a data-driven alert; False otherwise
        :param dict parameter_dict: dictionary of URL parameters to append; the value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """
        
        super().__init__(ts_connection)
        self._query_data_alerts = query_data_alerts
        self._query_data_alert = query_data_alert
        self._data_alert_id = data_alert_id
        self._user_id = user_id
        self._add_user = add_user
        self._remove_user = remove_user
        self._delete_data_alert = delete_data_alert
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_data_alerts,
            self._query_data_alert,
            self._add_user,
            self._remove_user,
            self._delete_data_alert,
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_data_alert_url(self):
        return "{0}/api/{1}/sites/{2}/dataAlerts".format(self._connection.server,
                                                         self._connection.api_version,
                                                         self._connection.site_id)
    
    @property
    def base_data_alert_id_url(self):
        return "{0}/{1}".format(self.base_data_alert_url, 
                                self._data_alert_id)
    
    @property
    def base_data_alert_user_url(self):
        return "{0}/users".format(self.base_data_alert_id_url)
    
    @property
    def base_data_alert_user_id_url(self):
        return "{0}/{1}".format(self.base_data_alert_user_url, 
                                self._user_id)
    
    def get_endpoint(self):
        url = None
        if not (self._data_alert_id or self._user_id or self._add_user or self._remove_user):
            url = self.base_data_alert_url
        elif self._data_alert_id and not (self._user_id or self._add_user or self._remove_user):
            url = self.base_data_alert_id_url
        elif self._data_alert_id and self._add_user and self._user_id and not self._remove_user:
            url = self.base_data_alert_user_url
        elif self._data_alert_id and self._remove_user and self._user_id and not self._add_user:
            url = self.base_data_alert_user_id_url
        else:
            self._invalid_parameter_exception()
            
        return self._append_url_parameters(url)
