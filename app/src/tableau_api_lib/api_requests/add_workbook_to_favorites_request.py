from tableau_api_lib.api_requests import BaseRequest


class AddWorkbookToFavoritesRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding workbooks to favorites.
    :param class ts_connection: the Tableau Server connection object
    :param str favorite_label: the text label to assign to the favorite item
    :param str workbook_id: the workbook ID
    """
    def __init__(self,
                 ts_connection,
                 favorite_label,
                 workbook_id):
        super().__init__(ts_connection)
        self._favorite_label = favorite_label
        self._workbook_id = workbook_id

    def base_add_favorites_request(self):
        self._request_body.update({
            'favorite': {
                'label': self._favorite_label,
                'workbook': {'id': self._workbook_id}
            }
        })
        return self._request_body

    def get_request(self):
        return self.base_add_favorites_request()
