from tableau_api_lib.api_endpoints import BaseEndpoint


class TableEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 query_table=False,
                 query_tables=False,
                 update_table=False,
                 remove_table=False,
                 table_id=None):
        """
        Builds API endpoints for REST API table methods.
        :param class ts_connection: the Tableau Server connection object
        :param query_table: True if querying a specific database table, False otherwise
        :param query_tables: True if querying all database tables, False otherwise
        :param update_table: True if updating a specific database table, False otherwise
        :param remove_table: True if removing a specific database table, False otherwise
        :param table_id: the database table ID
        """

        super().__init__(ts_connection)
        self._query_table = query_table
        self._query_tables = query_tables
        self._update_table = update_table
        self._remove_table = remove_table
        self._table_id = table_id
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_table,
            self._query_tables,
            self._update_table,
            self._remove_table
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_table_url(self):
        return "{0}/api/{1}/sites/{2}/tables".format(self._connection.server,
                                                     self._connection.api_version,
                                                     self._connection.site_id)

    @property
    def base_table_id_url(self):
        return "{0}/{1}".format(self.base_table_url,
                                self._table_id)

    def get_endpoint(self):
        if self._table_id:
            url = self.base_table_id_url
        else:
            url = self.base_table_url
        if url:
            return url
        else:
            self._invalid_parameter_exception()
