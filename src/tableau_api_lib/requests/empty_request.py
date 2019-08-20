from tableau.client.requests import BaseRequest


class EmptyRequest(BaseRequest):
    """
    Empty request for generating API request URLs to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    """
    def __init__(self,
                 ts_connection):

        super().__init__(ts_connection)

    def get_request(self):
        return self._request_body
