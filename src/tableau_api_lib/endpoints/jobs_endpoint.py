from tableau.client.endpoints import BaseEndpoint


class JobsEndpoint(BaseEndpoint):
    """
    Jobs endpoint for Tableau Server API requests.

    :param ts_connection:       The Tableau Server connection object.
    :type ts_connection:        class
    :param query_jobs:          Boolean flag; True if querying all jobs, False otherwise.
    :type query_jobs:           boolean
    :param query_job:           Boolean flag; True if querying a specific job, False otherwise.
    :type query_job:            boolean
    :param cancel_job:          Boolean flag; True if canceling a specific job, False otherwise.
    :type cancel_job:           boolean
    :param job_id:              The job ID.
    :type job_id:               string
    :param parameter_dict:      Dictionary of URL parameters to append. The value in each key-value pair
                                is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:       dict
    """
    def __init__(self,
                 ts_connection,
                 query_jobs=False,
                 query_job=False,
                 cancel_job=False,
                 job_id=None,
                 parameter_dict=None):

        super().__init__(ts_connection)
        self._query_jobs = query_jobs
        self._query_job = query_job
        self._cancel_job = cancel_job
        self._job_id = job_id
        self._parameter_dict = parameter_dict

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
