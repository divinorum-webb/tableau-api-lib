from tableau_api_lib.api_endpoints import BaseEndpoint


class GraphqlEndpoint(BaseEndpoint):
    def __init__(self, ts_connection):
        """
        Builds the API endpoint for issuing GraphQL queries to the Metadata API.
        :param class ts_connection: the Tableau Server connection object
        """

        super().__init__(ts_connection)

    @property
    def base_graphql_url(self):
        return "{0}/api/metadata/graphql".format(self._connection.server,
                                                 self._connection.api_version,
                                                 self._connection.site_id)

    def get_endpoint(self):
        return self.base_graphql_url
