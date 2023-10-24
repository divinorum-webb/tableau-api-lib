from tableau_api_lib.api_endpoints import BaseEndpoint


class FlowRunEndpoint(BaseEndpoint):
    def __init__(
        self,
        ts_connection,
        flow_run_id=None,
        get_flow_runs=False,
        get_flow_run=False,
        cancel_flow_run=False,
        parameter_dict=None,
    ):
        """
        Builds API endpoints for REST API flow methods.
        :param class ts_connection: the Tableau Server connection object
        :param str flow_run_id: the flow run ID relevant to queries
        :param bool get_flow_runs: True if querying flow runs
        :param bool get_flow_run: True if getting info about specified flow run
        :param bool cancel_flow_run: True if canceling specified flow run
        :param dict parameter_dict: dictionary of URL parameters to append. The value in each key-value pair is the literal
        text that will be appended to the URL endpoint
        """

        super().__init__(ts_connection)
        self._flow_run_id = flow_run_id
        self._get_flow_runs = get_flow_runs
        self._get_flow_run = get_flow_run
        self._cancel_flow_run = cancel_flow_run
        self._parameter_dict = parameter_dict
        self._validate_inputs()

    @property
    def mutually_exclusive_params(self):
        return [
            self._get_flow_runs,
            self._get_flow_run,
            self._cancel_flow_run
        ]

    def _validate_inputs(self) -> None:
        valid = True
        if sum(self.mutually_exclusive_params) != 1:
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def base_flow_runs_url(self):
        return "{0}/api/{1}/sites/{2}/flows/runs".format(
            self._connection.server, self._connection.api_version, self._connection.site_id
        )

    @property
    def base_flow_run_url(self):
        return "{0}/{1}".format(self.base_flow_runs_url, self._flow_run_id)

    def get_endpoint(self):
        url = None
        if self._flow_run_id:
            url = self.base_flow_run_url
        else:
            url = self.base_flow_runs_url

        return self._append_url_parameters(url)
