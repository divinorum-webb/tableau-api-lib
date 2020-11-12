from tableau_api_lib.api_endpoints import BaseEndpoint


class FileUploadEndpoint(BaseEndpoint):
    """
    FileUploadEndpoint endpoint for Tableau Server API api_requests.

    :param ts_connection:           The Tableau Server connection object.
    :type ts_connection:            class
    :param initiate_file_upload:    Boolean flag; True if initiating a file upload, False otherwise.
    :type initiate_file_upload:     boolean
    :param append_to_file_upload:   Boolean flag; True if appending to a file upload, False otherwise.
    :type append_to_file_upload:    boolean
    :param upload_session_id:       The upload session ID.
    :type upload_session_id:        string
    :param parameter_dict:          Dictionary of URL parameters to append. The value in each key-value pair
                                    is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:           dict
    """
    def __init__(self,
                 ts_connection,
                 initiate_file_upload=False,
                 append_to_file_upload=False,
                 upload_session_id=None,
                 parameter_dict=None):

        super().__init__(ts_connection)
        self._initiate_file_upload = initiate_file_upload
        self._append_to_file_upload = append_to_file_upload
        self._upload_session_id = upload_session_id
        self._parameter_dict = parameter_dict

    @property
    def mutually_exclusive_params(self):
        return [
            self._initiate_file_upload,
            self._append_to_file_upload
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_file_upload_url(self):
        return "{0}/api/{1}/sites/{2}/fileUploads".format(self._connection.server,
                                                          self._connection.api_version,
                                                          self._connection.site_id)

    @property
    def base_file_upload_id_url(self):
        return "{0}/{1}".format(self.base_file_upload_url,
                                self._upload_session_id)

    def get_endpoint(self):
        url = None
        if self._initiate_file_upload:
            url = self.base_file_upload_url
        elif self._append_to_file_upload and self._upload_session_id:
            url = self.base_file_upload_id_url
        else:
            self._invalid_parameter_exception()

        return self._append_url_parameters(url)
