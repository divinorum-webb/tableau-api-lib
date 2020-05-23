from tableau_api_lib.api_requests import BaseRequest


class AddTagsRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests adding tags to content.
    :param class ts_connection: the Tableau Server connection object
    :param list tags: a list of tag names to add
    """
    def __init__(self,
                 ts_connection,
                 tags=None):
        super().__init__(ts_connection)
        self._tags = tags or []
        self.base_add_tags_request()

    def base_add_tags_request(self):
        self._request_body.update({'tags': {'tag': []}})
        return self._request_body

    def modified_add_tags_request(self):
        for tag in self._tags:
            self._request_body['tags']['tag'].append({
                'label': tag
            })
        return self._request_body

    def get_request(self):
        return self.modified_add_tags_request()
