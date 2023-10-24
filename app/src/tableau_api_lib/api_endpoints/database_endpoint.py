from tableau_api_lib.api_endpoints import BaseEndpoint


class DatabaseEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 query_database=False,
                 query_databases=False,
                 update_database=False,
                 remove_database=False,
                 database_id=None):
        """
        Builds the API endpoints for interacting with database assets on Tableau Server.
        :param ts_connection: the Tableau Server connection object
        :param query_database: True if querying a specific database, False otherwise
        :param query_databases: True if querying all databases on the active site, False otherwise
        :param update_database: True if updating details for a specific database, False otherwise
        :param remove_database: True if removing a specific database asset, False otherwise
        :param database_id: the database ID
        """

        super().__init__(ts_connection)
        self._query_database = query_database
        self._query_databases = query_databases
        self._update_database = update_database
        self._remove_database = remove_database
        self._database_id = database_id
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_database,
            self._query_databases,
            self._update_database,
            self._remove_database
        ]

    def _validate_inputs(self):
        if sum(self.mutually_exclusive_params) == 1:
            pass
        else:
            self._invalid_parameter_exception()

    @property
    def base_database_url(self):
        return "{0}/api/{1}/sites/{2}/databases".format(self._connection.server,
                                                        self._connection.api_version,
                                                        self._connection.site_id)

    @property
    def base_database_id_url(self):
        return "{0}/{1}".format(self.base_database_url,
                                self._database_id)

    def get_endpoint(self):
        if self._database_id:
            url = self.base_database_id_url
        else:
            url = self.base_database_url
        if url:
            return url
        else:
            self._invalid_parameter_exception()
