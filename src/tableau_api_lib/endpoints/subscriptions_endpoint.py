from tableau.client.endpoints import BaseEndpoint


class SubscriptionsEndpoint(BaseEndpoint):
    """
    Subscriptions endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param create_subscription: Boolean flag; True if creating a subscription, False otherwise.
    :type create_subscription:  boolean
    :param query_subscriptions: Boolean flag; True if querying all subscriptions, False otherwise.
    :type query_subscriptions:  boolean
    :param query_subscription:  Boolean flag; True if querying a specific subscription, False otherwise.
    :type query_subscription:   boolean
    :param update_subscription: Boolean flag; True if updating a specific subscription, False otherwise.
    :type update_subscription:  boolean
    :param delete_subscription: Boolean flag; True if deleting a specific subscription, False otherwise.
    :type delete_subscription:  boolean
    :param subscription_id:     The subscription ID.
    :type subscription_id:      string
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self,
                 ts_connection,
                 create_subscription=False,
                 query_subscriptions=False,
                 query_subscription=False,
                 update_subscription=False,
                 delete_subscription=False,
                 subscription_id=None,
                 parameter_dict=None):

        super().__init__(ts_connection)
        self._create_subscription = create_subscription
        self._query_subscriptions = query_subscriptions
        self._query_subscription = query_subscription
        self._update_subscription = update_subscription
        self._delete_subscription = delete_subscription
        self._subscription_id = subscription_id
        self._parameter_dict = parameter_dict

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
