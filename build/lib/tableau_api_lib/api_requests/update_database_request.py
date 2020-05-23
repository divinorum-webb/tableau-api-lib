from tableau_api_lib.api_requests import BaseRequest


class UpdateDatabaseRequest(BaseRequest):
    def __init__(self,
                 ts_connection,
                 certification_status=None,
                 certification_note=None,
                 new_description_value=None,
                 new_contact_id=None
                 ):
        """
        Builds the request body for Tableau Server REST API requests updating databases.
        :param class ts_connection: the Tableau Server connection object
        :param bool certification_status: (optional) True if the database is certified, False otherwise
        :param str certification_note: (optional) custom text accompanying the certification status
        :param str new_description_value: (optional) custom text to describe the database asset
        :param str new_contact_id: (optional) the user ID for the contact associated with the database
        """

        super().__init__(ts_connection)
        self._certification_status = certification_status
        self._certification_note = certification_note
        self._new_description_value = new_description_value
        self._new_contact_id = new_contact_id
        self._request_body = {'database': {}}

    @property
    def optional_param_keys(self):
        return [
            'isCertified',
            'certificationNote',
            'description'
        ]

    @property
    def optional_param_values(self):
        return [
            self._certification_status,
            self._certification_note,
            self._new_description_value
        ]

    def base_update_database_request(self):
        if self._new_contact_id:
            self._request_body['database'].update({'contact': self._new_contact_id})
        self._request_body['database'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                        self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.base_update_database_request()
