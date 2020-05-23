from tableau_api_lib.api_endpoints import BaseEndpoint


class WorkbookEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 query_workbooks=False,
                 query_workbook=False,
                 publish_workbook=False,
                 update_workbook=False,
                 delete_workbook=False,
                 update_workbook_connection=False,
                 workbook_id=None,
                 view_id=None,
                 connection_id=None,
                 add_tags=False,
                 delete_tag=False,
                 tag_name=None,
                 create_extracts=False,
                 delete_extracts=False,
                 encryption_flag=False,
                 revision_number=None,
                 downgrade_target_version=None,
                 query_views=False,
                 query_connections=False,
                 query_workbook_preview_img=False,
                 query_workbook_view_preview_img=False,
                 get_workbook_revisions=False,
                 remove_workbook_revision=False,
                 download_workbook=False,
                 download_workbook_pdf=False,
                 download_workbook_revision=False,
                 refresh_workbook=False,
                 get_workbook_downgrade_info=False,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API workbook methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool query_workbooks: True if querying all workbooks; False otherwise
        :param bool query_workbook: True if querying a specific workbook, False otherwise
        :param bool update_workbook_connection: True if updating a specific workbook connection, False otherwise
        :param bool publish_workbook: True if publishing a specific workbook, False otherwise
        :param bool update_workbook: True if updating a specific workbook, False otherwise
        :param bool delete_workbook: True if deleting a specific workbook, False otherwise
        :param str workbook_id: the workbook ID
        :param str view_id: the view ID
        :param str connection_id: the workbook connection ID
        :param bool add_tags: True if adding tags, False otherwise
        :param bool delete_tag: True if deleting a specific tag, False otherwise
        :param str tag_name: the name of the tag
        :param bool create_extracts: True if creating extracts for the workbook, False otherwise
        :param bool delete_extracts: True if deleting extracts for the workbook, False otherwise
        :param bool encryption_flag: True if encrypting extracts for the workbook, False otherwise
        :param str revision_number: the revision number of the workbook revision to download
        :param str downgrade_target_version: the desired downgrade target version for the workbook
        :param bool query_views: True if querying all views, False otherwise
        :param bool query_connections: True if querying all connections, False otherwise
        :param bool query_workbook_preview_img: True if querying a specific preview image, False otherwise
        :param bool query_workbook_view_preview_img: True if querying a specific preview image, False otherwise
        :param bool get_workbook_revisions: True if getting all workbook revisions, False otherwise
        :param bool remove_workbook_revision: True if removing a workbook revision, False otherwise
        :param bool download_workbook: True if downloading workbook content, False otherwise
        :param bool download_workbook_pdf: True if downloading a specific workbook's PDF, False otherwise
        :param bool download_workbook_revision: True if downloading a specific workbook revision, False otherwise
        :param bool refresh_workbook: True if refreshing a specific workbook, False otherwise
        :param bool get_workbook_downgrade_info: True if getting downgrade info for a specific workbook, False otherwise
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._query_workbooks = query_workbooks
        self._query_workbook = query_workbook
        self._publish_workbook = publish_workbook
        self._update_workbook = update_workbook
        self._delete_workbook = delete_workbook
        self._update_workbook_connection = update_workbook_connection
        self._workbook_id = workbook_id
        self._view_id = view_id
        self._connection_id = connection_id
        self._add_tags = add_tags
        self._delete_tag = delete_tag
        self._tag_name = tag_name
        self._create_extracts = create_extracts
        self._delete_extracts = delete_extracts
        self._encryption_flag = encryption_flag
        self._revision_number = revision_number
        self._downgrade_target_version = downgrade_target_version
        self._query_views = query_views
        self._query_connections = query_connections
        self._query_workbook_preview_img = query_workbook_preview_img
        self._query_workbook_view_preview_img = query_workbook_view_preview_img
        self._get_workbook_revisions = get_workbook_revisions
        self._remove_workbook_revision = remove_workbook_revision
        self._download_workbook = download_workbook
        self._download_workbook_pdf = download_workbook_pdf
        self._download_workbook_revision = download_workbook_revision
        self._refresh_workbook = refresh_workbook
        self._get_workbook_downgrade_info = get_workbook_downgrade_info
        self._parameter_dict = parameter_dict
        self._modify_parameter_dict()
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_workbooks,
            self._query_workbook,
            self._publish_workbook,
            self._update_workbook,
            self._delete_workbook,
            self._update_workbook_connection,
            self._add_tags,
            self._delete_tag,
            self._create_extracts,
            self._delete_extracts,
            self._query_views,
            self._query_connections,
            self._query_workbook_preview_img,
            self._query_workbook_view_preview_img,
            self._get_workbook_revisions,
            self._remove_workbook_revision,
            self._download_workbook,
            self._download_workbook_pdf,
            self._download_workbook_revision,
            self._refresh_workbook,
            self._get_workbook_downgrade_info
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) > 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    def _modify_parameter_dict(self):
        if self._encryption_flag and not self._parameter_dict:
            self._parameter_dict = {}
        if self._encryption_flag:
            self._parameter_dict.update({"encryption_flag": f"encrypt={self._encryption_flag}"})

    @property
    def base_workbook_url(self):
        return "{0}/api/{1}/sites/{2}/workbooks".format(self._connection.server,
                                                        self._connection.api_version,
                                                        self._connection.site_id)

    @property
    def base_workbook_id_url(self):
        return "{0}/{1}".format(self.base_workbook_url,
                                self._workbook_id)

    @property
    def base_workbook_tags_url(self):
        return "{0}/tags".format(self.base_workbook_id_url)

    @property
    def base_delete_workbook_tag_url(self):
        return "{0}/{1}".format(self.base_workbook_tags_url,
                                self._tag_name)

    @property
    def base_create_extract_url(self):
        return "{0}/createExtract".format(self.base_workbook_id_url)

    @property
    def base_delete_extract_url(self):
        return "{0}/deleteExtract".format(self.base_workbook_id_url)

    @property
    def base_workbook_views_url(self):
        return "{0}/views".format(self.base_workbook_id_url)

    @property
    def base_workbook_connections_url(self):
        return "{0}/connections".format(self.base_workbook_id_url)

    @property
    def base_workbook_connection_id_url(self):
        return "{0}/{1}".format(self.base_workbook_connections_url,
                                self._connection_id)

    @property
    def base_workbook_preview_url(self):
        return "{0}/previewImage".format(self.base_workbook_id_url)

    @property
    def base_workbook_view_preview_url(self):
        return "{0}/{1}/previewImage".format(self.base_workbook_views_url,
                                             self._view_id)

    @property
    def base_workbook_revisions_url(self):
        return "{0}/revisions".format(self.base_workbook_id_url)

    @property
    def base_workbook_downgrade_version_url(self):
        return "{0}/downGradeInfo?productVersion={1}".format(self.base_workbook_id_url,
                                                             self._downgrade_target_version)

    @property
    def base_workbook_revision_removal_url(self):
        return "{0}/{1}".format(self.base_workbook_revisions_url,
                                self._revision_number)

    @property
    def base_workbook_content_url(self):
        return "{0}/content".format(self.base_workbook_id_url)

    @property
    def base_workbook_revision_download_url(self):
        return "{0}/{1}/content".format(self.base_workbook_revisions_url,
                                        self._revision_number)

    @property
    def base_workbook_download_pdf_url(self):
        return "{0}/pdf".format(self.base_workbook_id_url)

    @property
    def base_workbook_refresh_url(self):
        return "{0}/refresh".format(self.base_workbook_id_url)

    @property
    def valid_page_orientations(self):
        return [
            'Portrait',
            'Landscape'
        ]

    @property
    def valid_page_types(self):
        return [
            'A3',
            'A4',
            'A5',
            'B5',
            'Executive',
            'Folio',
            'Ledger',
            'Legal',
            'Letter',
            'Note',
            'Quarto',
            'Tabloid'
        ]

    def _validate_parameter_dict(self):
        if self._parameter_dict:
            parameter_keys = [key.lower() for key in self._parameter_dict.keys()]
            if 'orientation' in parameter_keys:
                if self._parameter_dict['orientation'] in self.valid_page_orientations:
                    pass
                else:
                    raise self._invalid_parameter_exception()
            if 'type' in parameter_keys:
                if self._parameter_dict['type'] in self.valid_page_types:
                    pass
                else:
                    raise self._invalid_parameter_exception()

    def get_endpoint(self):
        url = None
        if self._workbook_id:
            if self._query_workbook:
                url = self.base_workbook_id_url
            elif self._delete_workbook:
                url = self.base_workbook_id_url
            elif self._update_workbook_connection and self._connection_id:
                url = self.base_workbook_connection_id_url
            elif self._update_workbook and not (self._delete_workbook or self._publish_workbook):
                url = self.base_workbook_id_url
            elif self._add_tags:
                url = self.base_workbook_tags_url
            elif self._delete_tag:
                url = self.base_delete_workbook_tag_url
            elif self._create_extracts:
                url = self.base_create_extract_url
            elif self._delete_extracts:
                url = self.base_delete_extract_url
            elif self._query_views:
                url = self.base_workbook_views_url
            elif self._query_connections:
                url = self.base_workbook_connections_url
            elif self._query_workbook_preview_img:
                url = self.base_workbook_preview_url
            elif self._query_workbook_view_preview_img:
                url = self.base_workbook_view_preview_url
            elif self._get_workbook_revisions:
                url = self.base_workbook_revisions_url
            elif self._remove_workbook_revision:
                url = self.base_workbook_revision_removal_url
            elif self._download_workbook:
                url = self.base_workbook_content_url
            elif self._download_workbook_pdf:
                url = self.base_workbook_download_pdf_url
            elif self._download_workbook_revision and self._revision_number:
                url = self.base_workbook_revision_download_url
            elif self._refresh_workbook:
                url = self.base_workbook_refresh_url
            elif self._get_workbook_downgrade_info:
                url = self.base_workbook_downgrade_version_url
            else:
                self._invalid_parameter_exception()
        else:
            url = self.base_workbook_url

        return self._append_url_parameters(url)
