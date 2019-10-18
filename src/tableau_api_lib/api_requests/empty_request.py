from tableau_api_lib.api_requests import BaseRequest


class EmptyRequest(BaseRequest):
    """
    Builds the request body for empty Tableau Server REST API requests.
    :param class ts_connection: the Tableau Server connection object
    """
    def __init__(self,
                 ts_connection):

        super().__init__(ts_connection)

    def get_request(self):
        return self._request_body
