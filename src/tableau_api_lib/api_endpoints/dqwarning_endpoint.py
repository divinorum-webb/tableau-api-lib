from tableau_api_lib.api_endpoints import BaseEndpoint


class DQWarningEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 warning_id=None,
                 content_type=None,
                 content_id=None,
                 add_warning=False,
                 query_by_id=False,
                 query_by_content=False,
                 update_warning=False,
                 delete_by_id=False,
                 delete_by_content=False):

        super().__init__(ts_connection)
        self._warning_id = warning_id
        self._content_type = str(content_type).lower() if content_type else None
        self._content_id = content_id
        self._add_warning = add_warning
        self._query_by_id = query_by_id
        self._query_by_content = query_by_content
        self._update_warning = update_warning
        self._delete_by_id = delete_by_id
        self._delete_by_content = delete_by_content
        self._validate_inputs()

    @property
    def valid_content_types(self):
        return [
            'database',
            'table',
            'datasource',
            'flow'
        ]

    @property
    def mutually_exclusive_params(self):
        return [
            self._add_warning,
            self._query_by_id,
            self._query_by_content,
            self._update_warning,
            self._delete_by_id,
            self._delete_by_content
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if self._content_type not in self.valid_content_types:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_warning_url(self):
        return "{0}/api/{1}/sites/{2}/dataQualityWarnings".format(self._connection.server,
                                                                  self._connection.api_version,
                                                                  self._connection.site_id)

    @property
    def base_warning_id_url(self):
        return "{0}/{1}".format(self.base_warning_url,
                                self._warning_id)

    @property
    def base_content_type_url(self):
        return "{0}/contentType/{1}/contentLuid/{2}".format(self.base_warning_url,
                                                            self._content_type,
                                                            self._content_id)

    def get_endpoint(self):
        url = None
        if any([self._add_warning, self._query_by_content, self._delete_by_content]):
            url = self.base_content_type_url
        elif any([self._query_by_id, self._delete_by_id, self._update_warning]):
            url = self.base_warning_id_url
        if url:
            return url
        else:
            self._invalid_parameter_exception()
