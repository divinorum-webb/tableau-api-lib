from tableau_api_lib.api_requests import BaseRequest


class UpdateTableRequest(BaseRequest):
    def __init__(self,
                 ts_connection,
                 certification_status=None,
                 certification_note=None,
                 new_description_value=None,
                 new_contact_id=None
                 ):

        super().__init__(ts_connection)
        self._certification_status = certification_status
        self._certification_note = certification_note
        self._new_description_value = new_description_value
        self._new_contact_id = new_contact_id
        self._request_body = {'table': {}}

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

    def base_update_table_request(self):
        if self._new_contact_id:
            self._request_body['table'].update({'contact': self._new_contact_id})
        self._request_body['table'].update(self._get_parameters_dict(self.optional_param_keys,
                                                                     self.optional_param_values))
        return self._request_body

    def get_request(self):
        return self.base_update_table_request()
