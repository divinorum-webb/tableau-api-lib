from tableau_api_lib.api_requests import BaseRequest


class GraphqlRequest(BaseRequest):
    """
    Builds the request body for Tableau Server Metadata API GraphQL queries.
    :param class ts_connection: the Tableau Server connection object
    :param str query: the GraphQL query for the Metadata API (raw text)
    """
    def __init__(self,
                 ts_connection,
                 query):

        super().__init__(ts_connection)
        self._query = query
        self._validate_inputs()
        self.base_create_group_request()

    def _validate_inputs(self):
        if isinstance(self._query, str):
            pass
        else:
            raise ValueError("The GraphQL query argument must be of type 'str', not '{}'.".format(type(self._query)))

    def base_create_group_request(self):
        self._request_body.update({'query': self._query})
        return self._request_body

    def get_request(self):
        return self.base_create_group_request()
