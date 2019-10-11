from tableau_api_lib.api_endpoints import BaseEndpoint


class ColumnEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 query_column=False,
                 query_columns=False,
                 update_column=False,
                 remove_column=False,
                 table_id=None,
                 column_id=None):
        """
        Builds the API endpoint for interacting with database table columns.
        :param class ts_connection: the Tableau Server connection object
        :param bool query_column: True if querying a specific column by ID, False by default
        :param bool query_columns: True if querying all columns for the active site, False by default
        :param bool update_column: True if updating a specific column, False by default
        :param bool remove_column: True if removing a specific column, False by default
        :param str table_id: the database table ID
        :param str column_id: the column ID
        """

        super().__init__(ts_connection)
        self._query_column = query_column
        self._query_columns = query_columns
        self._update_column = update_column
        self._remove_column = remove_column
        self._table_id = table_id
        self._column_id = column_id
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_column,
            self._query_columns,
            self._update_column,
            self._remove_column
        ]

    def _validate_inputs(self):
        if sum(self.mutually_exclusive_params) == 1:
            pass
        else:
            self._invalid_parameter_exception()

    @property
    def base_column_url(self):
        return "{0}/api/{1}/sites/{2}/tables/{3}/columns".format(self._connection.server,
                                                                 self._connection.api_version,
                                                                 self._connection.site_id,
                                                                 self._table_id)

    @property
    def base_column_id_url(self):
        return "{0}/{1}".format(self.base_column_url,
                                self._column_id)

    def get_endpoint(self):
        if self._column_id and self._table_id:
            url = self.base_column_id_url
        else:
            url = self.base_column_url
        if url:
            return url
        else:
            self._invalid_parameter_exception()
