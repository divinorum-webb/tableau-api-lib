from tableau_api_lib.api_endpoints import BaseEndpoint


class ViewEndpoint(BaseEndpoint):
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
        """
        Builds API endpoints for REST API view methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool query_views: True if querying all views, False otherwise
        :param bool query_view: True if querying a specific view, False otherwise
        :param str view_id: the view ID
        :param bool add_tags: True if adding tags, False otherwise
        :param bool delete_tag: True if deleting a specific tag, False otherwise
        :param str tag_name: the name of the tag
        :param bool query_view_pdf: True if querying a specific view's PDF, False otherwise
        :param bool query_view_image: True if querying a specific view's image, False otherwise
        :param bool query_view_data: True if querying a specific view's data, False otherwise
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

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
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._add_tags,
            self._delete_tag,
            self._query_view,
            self._query_views,
            self._query_view_pdf,
            self._query_view_image,
            self._query_view_data
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

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
        url = None
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
