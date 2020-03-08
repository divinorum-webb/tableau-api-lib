from tableau_api_lib.api_requests import BaseRequest


class CreateExtractsForWorkbookRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests creating extracts for a workbook.
    :param class ts_connection: the Tableau Server connection object
    :param bool extract_all_datasources_flag: True if extracting all datasources, False otherwise
    :param list datasource_ids: a list of datasources to create extracts from, if extract_all_datasources_flag is False
    """
    def __init__(self,
                 ts_connection,
                 extract_all_datasources_flag=True,
                 datasource_ids=None):
        super().__init__(ts_connection)
        self._extract_all_datasources_flag = extract_all_datasources_flag
        self._datasource_ids = datasource_ids or []
        self._validate_inputs()
        self.base_create_extracts_for_workbook_request()

    def _validate_inputs(self):
        valid = True
        if self._extract_all_datasources_flag is False and not any(self._datasource_ids):
            valid = False
        if self._extract_all_datasources_flag is True and self._datasource_ids:
            valid = False
        if not valid:
            raise self._invalid_parameter_exception()

    @property
    def optional_extract_param_keys(self):
        return ['datasource_ids']

    @property
    def optional_extract_param_values(self):
        return [self._datasource_ids]

    def base_create_extracts_for_workbook_request(self):
        self._request_body.update({'datasources': {'includeAll': self._extract_all_datasources_flag}})
        return self._request_body

    def modified_create_extracts_for_workbook_request(self):
        if not self._extract_all_datasources_flag and any(self._datasource_ids):
            self._request_body['datasources'].update({'datasource': []})
            for datasource_id in self._datasource_ids:
                self._request_body['datasources']['datasource'].append({
                    'id': datasource_id
                })
        return self._request_body

    def get_request(self):
        return self.modified_create_extracts_for_workbook_request()
