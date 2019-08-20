from tableau.client.endpoints import BaseEndpoint


class DataAlertEndpoint(BaseEndpoint):
    """
    Data Alert endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param query_data_alerts:   Boolean flag; True if querying all data alerts, False otherwise.
    :type query_data_alerts:    boolean
    :param query_data_alert:    Boolean flag; True if querying a specific data alert, False otherwise.
    :type query_data_alert:     boolean
    :param data_alert_id:       The data alert ID.
    :type data_alert_id:        string
    :param user_id:             The user ID.
    :type user_id:              string
    :param add_user:            Boolean flag; True if adding a user to the alert, False otherwise.
    :type add_user:             boolean
    :param remove_user:         Boolean flag; True if removing a user from the alert; False otherwise.
    :type remove_user:          boolean
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self, 
                 ts_connection,
                 query_data_alerts=False,
                 query_data_alert=False,
                 data_alert_id=None, 
                 user_id=None, 
                 add_user=False, 
                 remove_user=False,
                 parameter_dict=None):
        
        super().__init__(ts_connection)
        self._query_data_alerts = query_data_alerts
        self._query_data_alert = query_data_alert
        self._data_alert_id = data_alert_id
        self._user_id = user_id
        self._add_user = add_user
        self._remove_user = remove_user
        self._parameter_dict = parameter_dict
        
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
