from tableau.client.endpoints import BaseEndpoint


class ProjectEndpoint(BaseEndpoint):
    """
    Projects endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param create_project:      Boolean flag; True if creating a project, False otherwise.
    :type create_project:       boolean
    :param query_projects:      Boolean flag; True if querying all projects, False otherwise.
    :type query_projects:       boolean
    :param update_project:      Boolean flag; True if updating a specific project, False otherwise.
    :type update_project:       boolean
    :param delete_project:      Boolean flag; True if deleting a specific project, False otherwise.
    :type delete_project:       boolean
    :param project_id:          The project ID.
    :type project_id:           string
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self, 
                 ts_connection,
                 create_project=False,
                 query_projects=False,
                 update_project=False,
                 delete_project=False,
                 project_id=None, 
                 parameter_dict=None):
        
        super().__init__(ts_connection)
        self._create_project = create_project
        self._query_projects = query_projects
        self._update_project = update_project
        self._delete_project = delete_project
        self._project_id = project_id
        self._parameter_dict = parameter_dict
        
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
