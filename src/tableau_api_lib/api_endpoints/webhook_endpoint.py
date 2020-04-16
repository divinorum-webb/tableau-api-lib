from tableau_api_lib.api_endpoints import BaseEndpoint


class WebhookEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 create_webhook=False,
                 query_webhook=False,
                 query_webhooks=False,
                 test_webhook=False,
                 delete_webhook=False,
                 webhook_id=None,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API webhook methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool create_webhook: True if creating a webhook, False otherwise
        :param bool query_webhook: True if querying a specific webhook, False otherwise
        :param bool query_webhooks: True if querying all webhooks, False otherwise
        :param bool test_webhook: True if updating a specific webhook, False otherwise
        :param bool delete_webhook: True if deleting a specific webhook, False otherwise
        :param str webhook_id: the webhook ID
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._create_webhook = create_webhook
        self._query_webhook = query_webhook
        self._query_webhooks = query_webhooks
        self._test_webhook = test_webhook
        self._delete_webhook = delete_webhook
        self._webhook_id = webhook_id
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._create_webhook,
            self._query_webhook,
            self._query_webhooks,
            self._test_webhook,
            self._delete_webhook
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_webhook_url(self):
        return "{0}/api/{1}/sites/{2}/webhooks".format(self._connection.server,
                                                       self._connection.api_version,
                                                       self._connection.site_id)

    @property
    def base_webhook_id_url(self):
        return "{0}/{1}".format(self.base_webhook_url,
                                self._webhook_id)

    @property
    def base_webhook_test_url(self):
        return "{0}/test".format(self.base_webhook_id_url)

    def get_endpoint(self):
        if self._webhook_id:
            if self._test_webhook:
                url = self.base_webhook_test_url
            else:
                url = self.base_webhook_id_url
        else:
            url = self.base_webhook_url
        if url:
            return self._append_url_parameters(url)
        else:
            self._invalid_parameter_exception()
