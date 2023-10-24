from tableau_api_lib.api_requests import BaseRequest


class UpdateSubscriptionRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating subscriptions.
    :param class ts_connection: the Tableau Server connection object
    :param str new_subscription_subject: (optional) a new subject for the subscription
    :param str new_schedule_id: (optional) the ID of a schedule to associate this subscription with
    :param bool attach_image_flag: True if attaching a .png image to the subscription, defaults to False
    :param bool attach_pdf_flag: True if attaching a .pdf file to the subscription, defaults to False
    """
    def __init__(self,
                 ts_connection,
                 new_subscription_subject=None,
                 new_schedule_id=None,
                 attach_image_flag=None,
                 attach_pdf_flag=None):

        super().__init__(ts_connection)
        self._new_subscription_subject = new_subscription_subject
        self._new_schedule_id = new_schedule_id
        self._attach_image_flag = attach_image_flag
        self._attach_pdf_flag = attach_pdf_flag

    @property
    def optional_param_keys(self):
        return [
            'attachImage',
            'attachPdf'
        ]

    @property
    def optional_param_values(self):
        return [
            self._attach_image_flag,
            self._attach_pdf_flag
        ]

    def base_update_subscription_request(self):
        if self._new_subscription_subject and self._new_schedule_id:
            self._request_body.update({
                'subscription': {
                    'subject': self._new_subscription_subject,
                    'schedule': {'id': self._new_schedule_id}
                }
            })
        elif self._new_subscription_subject and not self._new_schedule_id:
            self._request_body.update({
                'subscription': {
                    'subject': self._new_subscription_subject
                }
            })
        else:
            self._request_body.update({
                'subscription': {
                    'schedule': {'id': self._new_schedule_id}
                }
            })
        if any(self.optional_param_values):
            self._request_body['subscription'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                                self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.base_update_subscription_request()
