from tableau_api_lib.api_requests import BaseRequest


class CreateWebhookRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests creating webhooks.
    :param class ts_connection: the Tableau Server connection object
    :param str webhook_name: the name of the webhook being created
    :param str webhook_source_api_event_name: one of the valid documented source api event names
    :param str http_request_method: the http request method [GET, POST, DELETE]
    :param str url: the destination URL for the webhook; must be https and have a valid certificate
    """
    def __init__(self,
                 ts_connection,
                 webhook_name=None,
                 webhook_source_api_event_name=None,
                 http_request_method=None,
                 url=None):

        super().__init__(ts_connection)
        self._webhook_name = webhook_name
        self._webhook_source_api_event_name = webhook_source_api_event_name
        self._http_request_method = str(http_request_method).upper()
        self._url = url
        self._validate_inputs()

    def _validate_inputs(self):
        valid = True
        if self._webhook_source_api_event_name not in self.valid_webhook_source_api_event_names:
            valid = False
        if self._http_request_method not in self.valid_http_request_methods:
            valid = False
        if not valid:
            raise ValueError(f'The webhook source API event name "{self._webhook_source_api_event_name} is invalid."')

    @property
    def valid_webhook_source_api_event_names(self):
        return [
            'webhook-source-event-datasource-refresh-started',
            'webhook-source-event-datasource-refresh-succeeded',
            'webhook-source-event-datasource-refresh-failed',
            'webhook-source-event-datasource-updated',
            'webhook-source-event-datasource-created',
            'webhook-source-event-datasource-deleted',
            'webhook-source-event-workbook-updated',
            'webhook-source-event-workbook-created',
            'webhook-source-event-workbook-deleted',
            'webhook-source-event-workbook-refresh-started',
            'webhook-source-event-workbook-refresh-succeeded',
            'webhook-source-event-workbook-refresh-failed'
        ]

    @property
    def valid_http_request_methods(self):
        return ['GET', 'POST', 'DELETE']

    @property
    def required_webhook_param_keys(self):
        return ['name']

    @property
    def required_webhook_destination_param_keys(self):
        return ['method', 'url']

    @property
    def required_webhook_param_values(self):
        return [self._webhook_name]

    @property
    def required_webhook_destination_param_values(self):
        return [self._http_request_method, self._url]

    def base_create_webhook_request(self):
        if all(self.required_webhook_param_values) and all(self.required_webhook_destination_param_values):
            self._request_body.update({'webhook': {}})
            self._request_body['webhook'].update(
                self._get_parameters_dict(self.required_webhook_param_keys,
                                          self.required_webhook_param_values))
            self._request_body['webhook'].update({'webhook-source': {}})
            self._request_body['webhook']['webhook-source'].update({self._webhook_source_api_event_name: {}})
            self._request_body['webhook'].update({'webhook-destination': {}})
            self._request_body['webhook']['webhook-destination'].update({'webhook-destination-http': {}})
            self._request_body['webhook']['webhook-destination']['webhook-destination-http'].update(
                self._get_parameters_dict(self.required_webhook_destination_param_keys,
                                          self.required_webhook_destination_param_values))
        else:
            self._invalid_parameter_exception()

        return self._request_body

    def get_request(self):
        return self.base_create_webhook_request()
