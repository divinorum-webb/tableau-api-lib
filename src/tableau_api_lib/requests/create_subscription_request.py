from tableau.client.requests.base_request import BaseRequest


class CreateSubscriptionRequest(BaseRequest):
    """
    Create subscription request for generating API request URLs to Tableau Server.

    :param ts_connection:           The Tableau Server connection object.
    :type ts_connection:            class
    :param subscription_subject:    The subject to display to users receiving the subscription.
    :type subscription_subject:     string
    :param content_type:            Set this value to 'Workbook' if the subscription is for a workbook;
                                    set this value to 'View' if the subscription is for a view.
    :type content_type:             string
    :param content_id:              The ID of the workbook or view the subscription is sourced from.
    :type content_id:               string
    :param schedule_id:             The ID of the schedule the subscription runs on.
    :type schedule_id:              string
    :param user_id:                 The user ID for the user who is being subscribed to the view or workbook.
                                    This user must have an email address defined on Tableau Server.
    :type user_id:                  string
    """
    def __init__(self,
                 ts_connection,
                 subscription_subject,
                 content_type,
                 content_id,
                 schedule_id,
                 user_id):

        super().__init__(ts_connection)
        self._subscription_subject = subscription_subject
        self._content_type = content_type.lower()
        self._content_id = content_id
        self._schedule_id = schedule_id
        self._user_id = user_id
        self._validate_content_type()

    @property
    def valid_content_types(self):
        return [
            'Workbook',
            'View'
        ]

    def _validate_content_type(self):
        if self._content_type.capitalize() in self.valid_content_types:
            pass
        else:
            self._invalid_parameter_exception()

    def base_create_subscription_request(self):
        self._request_body.update({
            'subscription': {
                'subject': self._subscription_subject,
                'content': {
                    'type': self._content_type,
                    'id': self._content_id
                },
                'schedule': {'id': self._schedule_id},
                'user': {'id': self._user_id}
            },
        })
        return self._request_body

    def get_request(self):
        return self.base_create_subscription_request()
