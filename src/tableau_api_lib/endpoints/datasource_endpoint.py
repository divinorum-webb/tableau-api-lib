from tableau.client.endpoints import BaseEndpoint


class DatasourceEndpoint(BaseEndpoint):
    """
    Datasource endpoint for Tableau Server API requests.

    :param ts_connection:                   The Tableau Server connection object.
    :type ts_connection:                    class
    :param query_datasources:               Boolean flag; True if querying all datasources, False otherwise.
    :type query_datasources:                boolean
    :param query_datasource:                Boolean flag; True if querying a specific datasource, False otherwise.
    :type query_datasource:                 boolean
    :param publish_datasource:              Boolean flag; True if publishing a specific datasource, False otherwise.
    :type publish_datasource:               boolean
    :param datasource_id:                   The datasource ID.
    :type datasource_id:                    string
    :param query_datasource_connections:    Boolean flag; True if querying a specific datasource's connections,
                                            False otherwise.
    :type query_datasource_connections:     boolean
    :param connection_id:                   The datasource connection id.
    :type connection_id:                    string
    :param add_tags:                        Boolean flag; True if adding tags to the datasource, False otherwise.
    :type add_tags:                         boolean
    :param delete_tag:                      Boolean flag; True if deleting a datasource tag, False otherwise.
    :type delete_tag:                       boolean
    :param refresh_datasource:              Boolean flag; True if refreshing the datasource, False otherwise.
    :type refresh_datasource:               boolean
    :param update_datasource:               Boolean flag; True if updating a datasource's information,
                                            False otherwise.
    :type update_datasource:                boolean
    :param update_datasource_connection:    Boolean flag; True if updating a datasource connection's information,
                                            False otherwise.
    :type update_datasource_connection:     boolean
    :type tag_name:                         The name / label for the datasource tag being added.
    :param tag_name:                        string
    :type download_datasource:              Boolean flag; True if downloading the datasource, False otherwise.
    :param download_datasource:             boolean
    :type delete_datasource:                Boolean flag; True if deleting the datasource, False otherwise.
    :param delete_datasource:               boolean
    :type get_datasource_revisions:         Boolean flag; True if getting datasource revisions, False otherwise.
    :param get_datasource_revisions:        boolean
    :type download_datasource_revision:     Boolean flag; True if downloading a specific datasource revision,
                                            False otherwise.
    :param download_datasource_revision:    boolean
    :type remove_datasource_revision:       Boolean flag; True if removing a specific datasource revision,
                                            False otherwise.
    :param remove_datasource_revision:      boolean
    :type revision_number:                  The datasource revision number.
    :param revision_number:                 string
    :type parameter_dict:                   Dictionary of URL parameters to append. The value in each key-value pair
                                            is the literal text that will be appended to the URL endpoint.
    :param parameter_dict:                  dict
    """
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

        super().__init__(ts_connection)
        self._query_datasource = query_datasource
        self._query_datasources = query_datasources
        self._publish_datasource = publish_datasource
        self._datasource_id = datasource_id
        self._connection_id = connection_id
        self._add_tags = add_tags
        self._delete_tag = delete_tag
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
        if self._datasource_id:
            if self._query_datasource:
                url = self.base_datasource_id_url
            elif self._publish_datasource and not (self._delete_datasource or self._update_datasource):
                url = self.base_datasource_url
            elif self._add_tags:
                url = self.base_datasource_tags_url
            elif self._delete_tag and self._tag_name:
                url = self.base_delete_datasource_tag_url
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
