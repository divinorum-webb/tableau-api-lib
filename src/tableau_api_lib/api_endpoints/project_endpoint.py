from tableau_api_lib.api_endpoints import BaseEndpoint


class ProjectEndpoint(BaseEndpoint):
    def __init__(self, 
                 ts_connection,
                 create_project=False,
                 query_projects=False,
                 update_project=False,
                 delete_project=False,
                 project_id=None, 
                 parameter_dict=None):
        """
        Builds API endpoints for REST API project methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool create_project: True if creating a project, False otherwise
        :param bool query_projects: True if querying all projects, False otherwise
        :param bool update_project: True if updating a specific project, False otherwise
        :param bool delete_project: True if deleting a specific project, False otherwise
        :param str project_id: the project ID
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """
        
        super().__init__(ts_connection)
        self._create_project = create_project
        self._query_projects = query_projects
        self._update_project = update_project
        self._delete_project = delete_project
        self._project_id = project_id
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._create_project,
            self._query_projects,
            self._update_project,
            self._delete_project
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()
        
    @property
    def base_project_url(self):
        return "{0}/api/{1}/sites/{2}/projects".format(self._connection.server, 
                                                       self._connection.api_version, 
                                                       self._connection.site_id)
    
    @property
    def base_project_id_url(self):
        return "{0}/{1}".format(self.base_project_url, 
                                self._project_id)
        
    def get_endpoint(self):
        if self._project_id:
            url = self.base_project_id_url
        else:
            url = self.base_project_url
        if url:
            return self._append_url_parameters(url)
        else:
            self._invalid_parameter_exception()
