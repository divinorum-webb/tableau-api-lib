from src.tableau_api_lib.api_requests import BaseRequest


class UpdateWorkbookNowRequest(BaseRequest):
    """
    Update workbook now request for sending API api_requests to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    """
    def __init__(self,
                 ts_connection):
        super().__init__(ts_connection)

    def base_update_workbook_now_request(self):
        return self._request_body

    def get_request(self):
        return self.base_update_workbook_now_request()
