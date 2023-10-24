from tableau_api_lib.api_requests import BaseRequest


class AddProjectToFavoritesRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding project to favorites.
    :param class ts_connection: the Tableau Server connection object
    :param str favorite_label: the text label to assign to the favorite item
    :param str project_id: the project ID
    """
    def __init__(self,
                 ts_connection,
                 favorite_label,
                 project_id):
        super().__init__(ts_connection)
        self._favorite_label = favorite_label
        self._project_id = project_id

    def base_add_favorites_request(self):
        self._request_body.update({
            'favorite': {
                'label': self._favorite_label,
                'project': {'id': self._project_id}
            }
        })
        return self._request_body

    def get_request(self):
        return self.base_add_favorites_request()
