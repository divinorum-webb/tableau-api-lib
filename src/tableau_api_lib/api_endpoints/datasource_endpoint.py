from tableau_api_lib.api_endpoints import BaseEndpoint


class DatasourceEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 query_datasources=False,
                 query_datasource=False,
                 publish_datasource=False,
                 datasource_id=None,
                 query_datasource_connections=False,
                 connection_id=None,
                 add_tags=False,
                 delete_tag=False,
                 create_extract=False,
                 delete_extract=False,
                 encryption_flag=False,
                 refresh_datasource=False,
                 update_datasource=False,
                 update_datasource_connection=False,
                 tag_name=None,
                 download_datasource=False,
                 delete_datasource=False,
                 get_datasource_revisions=False,
                 download_datasource_revision=False,
                 remove_datasource_revision=False,
                 revision_number=None,
                 parameter_dict=None):
        """
        Datasource endpoint for Tableau Server API api_requests.

        :param class ts_connection: the Tableau Server connection object
        :param bool query_datasources: True if querying all datasources, False otherwise
        :param bool query_datasource: True if querying a specific datasource, False otherwise
        :param bool publish_datasource: True if publishing a specific datasource, False otherwise
        :param str datasource_id: the datasource ID
        :param bool query_datasource_connections: True if querying a specific datasource's connections, False otherwise
        :param str connection_id: the datasource connection id
        :param bool add_tags: True if adding tags to the datasource, False otherwise
        :param bool delete_tag: True if deleting a datasource tag, False otherwise
        :param bool create_extract: True if creating an extract, False otherwise
        :param bool delete_extract: True if deleting an extract, False otherwise
        :param bool encryption_flag: True if encrypting a created extract, False otherwise
        :param bool refresh_datasource: True if refreshing the datasource, False otherwise
        :param bool update_datasource: True if updating a datasource, False otherwise
        :param bool update_datasource_connection: True if updating a datasource connection, False otherwise
        :param str tag_name: the name / label for the datasource tag being added
        :param bool download_datasource: True if downloading the datasource, False otherwise
        :param bool delete_datasource: True if deleting the datasource, False otherwise
        :param bool get_datasource_revisions: True if getting datasource revisions, False otherwise
        :param bool download_datasource_revision: True if downloading a specific datasource revision, False otherwise
        :param bool remove_datasource_revision: True if removing a specific datasource revision, False otherwise
        :param str revision_number: the datasource revision number
        :param dict parameter_dict: dictionary of URL parameters to append; the value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._query_datasource = query_datasource
        self._query_datasources = query_datasources
        self._publish_datasource = publish_datasource
        self._datasource_id = datasource_id
        self._connection_id = connection_id
        self._add_tags = add_tags
        self._delete_tag = delete_tag
        self._create_extract = create_extract
        self._delete_extract = delete_extract
        self._encryption_flag = encryption_flag
        self._refresh_datasource = refresh_datasource
        self._update_datasource = update_datasource
        self._update_datasource_connection = update_datasource_connection
        self._tag_name = tag_name
        self._query_datasource_connections = query_datasource_connections
        self._download_datasource = download_datasource
        self._delete_datasource = delete_datasource
        self._get_datasource_revisions = get_datasource_revisions
        self._download_datasource_revision = download_datasource_revision
        self._remove_datasource_revision = remove_datasource_revision
        self._revision_number = revision_number
        self._parameter_dict = parameter_dict
        self._modify_parameter_dict()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_datasource,
            self._query_datasources,
            self._publish_datasource,
            self._add_tags,
            self._delete_tag,
            self._create_extract,
            self._delete_extract,
            self._refresh_datasource,
            self._update_datasource,
            self._update_datasource_connection,
            self._query_datasource_connections,
            self._download_datasource,
            self._delete_datasource,
            self._get_datasource_revisions,
            self._download_datasource_revision,
            self._remove_datasource_revision
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    def _modify_parameter_dict(self):
        if self._encryption_flag and not self._parameter_dict:
            self._parameter_dict = {}
        if self._encryption_flag:
            self._parameter_dict.update({"encryption_flag": f"encrypt={self._encryption_flag}"})

    @property
    def base_datasource_url(self):
        return "{0}/api/{1}/sites/{2}/datasources".format(self._connection.server,
                                                          self._connection.api_version,
                                                          self._connection.site_id)

    @property
    def base_datasource_id_url(self):
        return "{0}/{1}".format(self.base_datasource_url,
                                self._datasource_id)

    @property
    def base_datasource_tags_url(self):
        return "{0}/tags".format(self.base_datasource_id_url)

    @property
    def base_create_extract_url(self):
        return "{0}/createExtract".format(self.base_datasource_id_url)

    @property
    def base_delete_extract_url(self):
        return "{0}/deleteExtract".format(self.base_datasource_id_url)

    @property
    def base_delete_datasource_tag_url(self):
        return "{0}/{1}".format(self.base_datasource_tags_url,
                                self._tag_name)

    @property
    def base_datasource_connections_url(self):
        return "{0}/connections".format(self.base_datasource_id_url)

    @property
    def base_datasource_revisions_url(self):
        return "{0}/revisions".format(self.base_datasource_id_url)

    @property
    def base_datasource_revision_number_url(self):
        return "{0}/{1}".format(self.base_datasource_revisions_url,
                                self._revision_number)

    @property
    def base_download_datasource_url(self):
        return "{0}/content".format(self.base_datasource_id_url)

    @property
    def base_download_datasource_revision_url(self):
        return "{0}/{1}/content".format(self.base_datasource_revisions_url,
                                        self._revision_number)

    @property
    def base_datasource_connection_id_url(self):
        return "{0}/{1}".format(self.base_datasource_connections_url,
                                self._connection_id)

    @property
    def base_refresh_datasource_url(self):
        return "{0}/refresh".format(self.base_datasource_id_url)

    def get_endpoint(self):
        url = None
        if self._datasource_id:
            if self._query_datasource:
                url = self.base_datasource_id_url
            elif self._publish_datasource and not (self._delete_datasource or self._update_datasource):
                url = self.base_datasource_url
            elif self._add_tags:
                url = self.base_datasource_tags_url
            elif self._delete_tag and self._tag_name:
                url = self.base_delete_datasource_tag_url
            elif self._create_extract:
                url = self.base_create_extract_url
            elif self._delete_extract:
                url = self.base_delete_extract_url
            elif self._query_datasource_connections:
                url = self.base_datasource_connections_url
            elif self._get_datasource_revisions:
                url = self.base_datasource_revisions_url
            elif self._remove_datasource_revision and self._revision_number:
                url = self.base_datasource_revision_number_url
            elif self._download_datasource:
                url = self.base_download_datasource_url
            elif self._delete_datasource:
                url = self.base_datasource_id_url
            elif self._download_datasource_revision:
                url = self.base_download_datasource_revision_url
            elif self._refresh_datasource:
                url = self.base_refresh_datasource_url
            elif self._update_datasource:
                url = self.base_datasource_id_url
            elif self._update_datasource_connection:
                url = self.base_datasource_connection_id_url
            else:
                self._invalid_parameter_exception()
        else:
            url = self.base_datasource_url

        return self._append_url_parameters(url)
