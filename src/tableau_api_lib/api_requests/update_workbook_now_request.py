from tableau_api_lib.api_requests import BaseRequest


class UpdateWorkbookNowRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating workbook extracts immediately.
    :param class ts_connection: the Tableau Server connection object
    """
    def __init__(self,
                 ts_connection):
        super().__init__(ts_connection)

    def base_update_workbook_now_request(self):
        return self._request_body

    def get_request(self):
        return self.base_update_workbook_now_request()
