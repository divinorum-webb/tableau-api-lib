from tableau_api_lib.api_endpoints import BaseEndpoint


class SubscriptionsEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 create_subscription=False,
                 query_subscriptions=False,
                 query_subscription=False,
                 update_subscription=False,
                 delete_subscription=False,
                 subscription_id=None,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API subscription methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool create_subscription: True if creating a subscription, False otherwise
        :param bool query_subscriptions: True if querying all subscriptions, False otherwise
        :param bool query_subscription: True if querying a specific subscription, False otherwise
        :param bool update_subscription: True if updating a specific subscription, False otherwise
        :param bool delete_subscription: True if deleting a specific subscription, False otherwise
        :param str subscription_id: the subscription ID
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._create_subscription = create_subscription
        self._query_subscriptions = query_subscriptions
        self._query_subscription = query_subscription
        self._update_subscription = update_subscription
        self._delete_subscription = delete_subscription
        self._subscription_id = subscription_id
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._create_subscription,
            self._query_subscriptions,
            self._query_subscription,
            self._update_subscription,
            self._delete_subscription
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_subscription_url(self):
        return "{0}/api/{1}/sites/{2}/subscriptions".format(self._connection.server,
                                                            self._connection.api_version,
                                                            self._connection.site_id)

    @property
    def base_subscription_id_url(self):
        return "{0}/{1}".format(self.base_subscription_url,
                                self._subscription_id)

    def get_endpoint(self):
        url = None
        if self._create_subscription:
            url = self.base_subscription_url
        elif self._query_subscriptions and not self._subscription_id:
            url = self.base_subscription_url
        elif self._subscription_id:
            if self._query_subscription or self._update_subscription or self._delete_subscription:
                url = self.base_subscription_id_url
            else:
                self._invalid_parameter_exception()
        else:
            self._invalid_parameter_exception()

        return self._append_url_parameters(url)
