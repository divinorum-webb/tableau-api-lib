from tableau_api_lib.api_requests import BaseRequest


class CreateSubscriptionRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests creating subscriptions.
    :param class ts_connection: the Tableau Server connection object
    :param str subscription_subject: the subject to display to users receiving the subscription
    :param str content_type: set this value to 'Workbook' if the subscription is for a workbook; set this value to
    'View' if the subscription is for a view
    :param str content_id: the ID of the workbook or view the subscription is sourced from.
    :param str schedule_id: the ID of the schedule the subscription runs on.
    :param str user_id: the user ID for the user who is being subscribed to the view or workbook; this user must have
    an email address defined on Tableau Server.
    :param str message: a message body to accompany the subscription email
    :param bool attach_image_flag: True if attaching a .png image to the subscription, defaults to False
    :param bool attach_pdf_flag: True if attaching a .pdf file to the subscription, defaults to False
    :param str pdf_page_orientation: page orientation for the attached PDF ['Portrait', 'Landscape']
    :param str pdf_page_size: page size for the attached PDF
    :param bool send_view_if_empty_flag: True if the subscription is to be sent even if empty (no data);
    False otherwise.
    """
    def __init__(self,
                 ts_connection,
                 subscription_subject,
                 content_type,
                 content_id,
                 schedule_id,
                 user_id,
                 message=None,
                 attach_image_flag=None,
                 attach_pdf_flag=None,
                 pdf_page_orientation="Portrait",
                 pdf_page_size="Letter",
                 send_view_if_empty_flag=True,
                 ):

        super().__init__(ts_connection)
        self._subscription_subject = subscription_subject
        self._content_type = content_type.lower()
        self._content_id = content_id
        self._schedule_id = schedule_id
        self._user_id = user_id
        self._message = message or ""
        self._attach_image_flag = attach_image_flag
        self._attach_pdf_flag = attach_pdf_flag
        self._pdf_page_orientation = pdf_page_orientation
        self._pdf_page_size = pdf_page_size
        self._send_view_if_empty_flag = send_view_if_empty_flag
        self._validate_content_type()
        self._validate_pdf_page_orientation()
        self._validate_pdf_page_size()

    @property
    def valid_content_types(self):
        return [
            'Workbook',
            'View',
        ]

    @property
    def valid_pdf_page_sizes(self):
        return [
            'A3',
            'A4',
            'A5',
            'B5',
            'Executive',
            'Folio',
            'Ledger',
            'Legal',
            'Letter',
            'Note',
            'Quarto',
            'Tabloid',
        ]

    @property
    def valid_pdf_page_orientations(self):
        return [
            'Portrait',
            'Landscape',
        ]

    def _validate_content_type(self):
        valid = True
        if not(self._content_type.lower().capitalize() in self.valid_content_types):
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    def _validate_pdf_page_orientation(self):
        if self._pdf_page_orientation.lower().capitalize() not in self.valid_pdf_page_orientations:
            self._invalid_parameter_exception()

    def _validate_pdf_page_size(self):
        if self._pdf_page_size.lower().capitalize() not in self.valid_pdf_page_sizes:
            self._invalid_parameter_exception()

    @property
    def optional_param_keys(self):
        return [
            'attachImage',
            'attachPdf',
            'message',
            'pageOrientation',
            'pageSizeOption',
        ]

    @property
    def optional_param_values(self):
        return [
            self._attach_image_flag,
            self._attach_pdf_flag,
            self._message,
            self._pdf_page_orientation,
            self._pdf_page_size,
        ]

    def base_create_subscription_request(self):
        self._request_body.update({
            'subscription': {
                'subject': self._subscription_subject,
                'content': {
                    'type': self._content_type,
                    'id': self._content_id,
                    'sendIfViewEmpty': self._send_view_if_empty_flag
                },
                'schedule': {'id': self._schedule_id},
                'user': {'id': self._user_id}
            },
        })
        if any(self.optional_param_values):
            self._request_body['subscription'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                                self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.base_create_subscription_request()
