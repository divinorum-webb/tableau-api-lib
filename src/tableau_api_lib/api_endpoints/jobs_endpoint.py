from tableau_api_lib.api_endpoints import BaseEndpoint


class JobsEndpoint(BaseEndpoint):
    def __init__(self,
                 ts_connection,
                 query_jobs=False,
                 query_job=False,
                 cancel_job=False,
                 job_id=None,
                 parameter_dict=None):
        """
        Builds API endpoints for REST API job methods.
        :param class ts_connection: the Tableau Server connection object
        :param bool query_jobs: True if querying all jobs, False otherwise
        :param bool query_job: True if querying a specific job, False otherwise
        :param bool cancel_job: True if canceling a specific job, False otherwise
        :param str job_id: the job ID
        :param dict parameter_dict: dictionary of URL parameters to append; the value in each key-value pair is the
        literal text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._query_jobs = query_jobs
        self._query_job = query_job
        self._cancel_job = cancel_job
        self._job_id = job_id
        self._parameter_dict = parameter_dict

    @property
    def mutually_exclusive_params(self):
        return [
            self._query_jobs,
            self._query_job,
            self._cancel_job
        ]

    def _validate_inputs(self):
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_job_url(self):
        return "{0}/api/{1}/sites/{2}/jobs".format(self._connection.server,
                                                   self._connection.api_version,
                                                   self._connection.site_id)

    @property
    def base_job_id_url(self):
        return "{0}/{1}".format(self.base_job_url,
                                self._job_id)

    def get_endpoint(self):
        if self._job_id:
            if self._query_job and not self._cancel_job:
                url = self.base_job_id_url
            elif self._cancel_job and not self._query_job:
                url = self.base_job_id_url
            else:
                url = self._invalid_parameter_exception()
        else:
            url = self.base_job_url

        return self._append_url_parameters(url)
