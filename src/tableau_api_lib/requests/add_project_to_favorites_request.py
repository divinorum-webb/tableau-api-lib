from tableau.client.requests import BaseRequest


class AddProjectToFavoritesRequest(BaseRequest):
    """
    Add project to favorites request for generating API request URLs to Tableau Server.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param favorite_label:      The text label to assign to the favorite item.
    :type favorite_label:       string
    :param project_id:          The project ID.
    :type project_id:           string
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
