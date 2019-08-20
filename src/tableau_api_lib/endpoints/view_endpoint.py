from tableau.client.endpoints import BaseEndpoint


class ViewEndpoint(BaseEndpoint):
    """
    View endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param query_views:         Boolean flag; True if querying all views, False otherwise.
    :type query_views:          boolean
    :param query_view:          Boolean flag; True if querying a specific view, False otherwise.
    :type query_view:           boolean
    :param view_id:             The view ID.
    :type view_id:              string
    :param add_tags:            Boolean flag; True if adding tags, False otherwise.
    :type add_tags:             boolean
    :param delete_tag:          Boolean flag; True if deleting a specific tag, False otherwise.
    :type delete_tag:           boolean
    :param tag_name:            The name of the tag.
    :type tag_name:             string
    :param query_view_pdf:      Boolean flag; True if querying a specific view's PDF, False otherwise.
    :type query_view_pdf:       boolean
    :param query_view_image:    Boolean flag; True if querying a specific view's image, False otherwise.
    :type query_view_image:     boolean
    :param query_view_data:     Boolean flag; True if querying a specific view's data, False otherwise.
    :type query_view_data:      boolean
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self,
                 ts_connection,
                 query_views=False,
                 query_view=False,
                 view_id=None,
                 add_tags=False,
                 delete_tag=False,
                 tag_name=None,
                 query_view_pdf=False,
                 query_view_image=False,
                 query_view_data=False,
                 parameter_dict=None):

        super().__init__(ts_connection)
        self._view_id = view_id
        self._add_tags = add_tags
        self._delete_tag = delete_tag
        self._tag_name = tag_name
        self._query_view = query_view
        self._query_views = query_views
        self._query_view_pdf = query_view_pdf
        self._query_view_image = query_view_image
        self._query_view_data = query_view_data
        self._parameter_dict = parameter_dict

    @property
    def base_view_url(self):
        return "{0}/api/{1}/sites/{2}/views".format(self._connection.server,
                                                    self._connection.api_version,
                                                    self._connection.site_id)

    @property
    def base_view_id_url(self):
        return "{0}/{1}".format(self.base_view_url,
                                self._view_id)

    @property
    def base_view_tags_url(self):
        return "{0}/tags".format(self.base_view_id_url)

    @property
    def base_delete_view_tag_url(self):
        return "{0}/{1}".format(self.base_view_tags_url,
                                self._tag_name)

    @property
    def base_query_view_pdf_url(self):
        return "{0}/{1}/pdf".format(self.base_view_url,
                                    self._view_id)

    @property
    def base_query_view_image_url(self):
        return "{0}/{1}/image".format(self.base_view_url,
                                      self._view_id)

    @property
    def base_query_view_data_url(self):
        return "{0}/data".format(self.base_view_id_url)

    def get_endpoint(self):
        if self._view_id:
            if self._query_view:
                url = self.base_view_id_url
            elif self._add_tags:
                url = self.base_view_tags_url
            elif self._delete_tag and self._tag_name:
                url = self.base_delete_view_tag_url
            elif self._query_view_pdf:
                url = self.base_query_view_pdf_url
            elif self._query_view_image:
                url = self.base_query_view_image_url
            elif self._query_view_data:
                url = self.base_query_view_data_url
            else:
                self._invalid_parameter_exception()
        else:
            url = self.base_view_url

        return self._append_url_parameters(url)
