from tableau_api_lib.api_requests import BaseRequest


class AddDatasourceToFavoritesRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding datasources to favorites.
    :param class ts_connection: the Tableau Server connection object
    :param str favorite_label: the text label to assign to the favorite item
    :param str datasource_id: the datasource ID
    """
    def __init__(self,
                 ts_connection,
                 favorite_label,
                 datasource_id):

        super().__init__(ts_connection)
        self._favorite_label = favorite_label
        self._datasource_id = datasource_id

    def base_add_favorites_request(self):
        self._request_body.update({
            'favorite': {
                'label': self._favorite_label,
                'datasource': {'id': self._datasource_id}
            }
        })
        return self._request_body

    def get_request(self):
        return self.base_add_favorites_request()
