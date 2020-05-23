from tableau_api_lib.api_requests import BaseRequest


class UpdateScheduleRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests updating schedules.
    :param class ts_connection: the Tableau Server connection object
    :param str schedule_name: the new name to give to the schedule
    :param str schedule_priority: an integer value (entered here as a string) between 1 and 100 that determines the
    default priority of the schedule if multiple tasks are pending in the queue; higher numbers have higher priority
    :param str schedule_state: the state for the schedule. Can be either 'Active' or 'Suspended'
    :param str schedule_type: this value (Extract or Subscription) indicates whether the schedule type is an extract or
    a subscription schedule
    :param str schedule_execution_order: parallel to allow jobs associated with this schedule to run at the same time,
    or Serial to require the jobs to run one after the other
    :param str schedule_frequency: the granularity of the schedule. Valid values are:
    [Hourly, Daily, Weekly, or Monthly]
    :param str start_time: the time of day on which scheduled jobs should run (or if the frequency is hourly, on which
    they should start being run), in the format HH:MM:SS (for example, 18:30:00); this value is required for all
    schedule frequencies
    :param str end_time: if the schedule frequency is Hourly, the time of day on which scheduled jobs should stop being
    run, in the format HH:MM:SS (for example, 23:30:00); hourly jobs will occur at the specified intervals between the
    start time and the end time; for other schedule frequencies, this value is not required and if the attribute is
    included, it is ignored
    :param list interval_expression_list: this list of dicts specifies the time interval(s) between jobs on the given
    schedule; the value required here depends on the 'schedule_frequency' value;
    if 'schedule_frequency' = 'Hourly', the interval expression should be either hours="interval" (where "interval" is
    a number [1, 2, 4, 6, 8, 12] in quotes);
    if 'schedule_frequency' = 'Daily', no interval needs to be specified;
    if 'schedule_frequency' = 'Weekly, the interval is weekDay="weekday", where weekday is one of
    ['Sunday', 'Monday', 'Tuesday', etc.] wrapped in quotes;
    if 'schedule_frequency' = 'Monthly', the interval expression is monthDay="day", where day is either the day of the
    month (1-31), or 'LastDay'; in both cases the value is wrapped in quotes.
    """

    def __init__(self,
                 ts_connection,
                 schedule_name=None,
                 schedule_priority=None,
                 schedule_type=None,
                 schedule_state=None,
                 schedule_execution_order=None,
                 schedule_frequency=None,
                 start_time=None,
                 end_time=None,
                 interval_expression_list=None):

        super().__init__(ts_connection)
        self._schedule_name = schedule_name
        self._schedule_priority = schedule_priority
        self._schedule_type = schedule_type
        self._schedule_state = schedule_state
        self._schedule_execution_order = schedule_execution_order
        self._schedule_frequency = schedule_frequency
        self._start_time = start_time
        self._end_time = end_time
        self._interval_expression_list = interval_expression_list
        self._interval_expression_keys = None
        self._interval_expression_values = None
        self._validate_inputs()
        self._validate_state()
        self.base_update_schedule_request()

    @property
    def optional_schedule_param_keys(self):
        return [
            'name',
            'priority',
            'type',
            'state',
            'frequency',
            'executionOrder'
        ]

    @property
    def optional_frequency_param_keys(self):
        return [
            'start',
            'end'
        ]

    @property
    def valid_interval_keys(self):
        return [
            'minutes',
            'hours',
            'weekDay',
            'monthDay'
        ]

    @property
    def valid_schedule_states(self):
        return [
            'active',
            'suspended',
            None
        ]

    def _validate_inputs(self):
        if self._interval_expression_list:
            self._set_interval_expressions()
        else:
            pass

    def _validate_state(self):
        if not self._schedule_state:
            pass
        elif str(self._schedule_state).lower() in self.valid_schedule_states:
            self._schedule_state = self._schedule_state.capitalize()
        else:
            self._invalid_parameter_exception()

    @property
    def optional_schedule_param_values(self):
        return [
            self._schedule_name,
            self._schedule_priority,
            self._schedule_type,
            self._schedule_state,
            self._schedule_frequency,
            self._schedule_execution_order
        ]

    @property
    def optional_frequency_param_values(self):
        return [
            self._start_time,
            self._end_time
        ]

    def _unpack_interval_expressions_list(self, interval_list):
        interval_keys = []
        interval_values = []
        for interval_dict in interval_list:
            for key in interval_dict.keys():
                if key in self.valid_interval_keys:
                    interval_keys.append(key)
                    interval_values.append(interval_dict[key])
                else:
                    self._invalid_parameter_exception()
        return interval_keys, interval_values

    def _set_interval_expressions(self):
        if self._interval_expression_list:
            if any(self._interval_expression_list):
                self._interval_expression_keys, self._interval_expression_values = \
                    self._unpack_interval_expressions_list(
                        self._interval_expression_list)

    @staticmethod
    def _get_parameters_list(param_keys, param_values):
        params_list = []
        for i, key in enumerate(param_keys):
            if param_values[i]:
                params_list.append({key: param_values[i]})
        return params_list

    def base_update_schedule_request(self):
        self._request_body.update({'schedule': {}})
        return self._request_body

    def modified_update_schedule_request(self):
        if any(self.optional_schedule_param_values):
            self._request_body['schedule'].update(
                self._get_parameters_dict(self.optional_schedule_param_keys,
                                          self.optional_schedule_param_values))

        if any(self.optional_frequency_param_values):
            self._request_body['schedule'].update({'frequencyDetails': {}})
            self._request_body['schedule']['frequencyDetails'].update(
                self._get_parameters_dict(self.optional_frequency_param_keys,
                                          self.optional_frequency_param_values))

        if self._interval_expression_list:
            self._request_body['schedule']['frequencyDetails'].update({'intervals': {}})
            self._request_body['schedule']['frequencyDetails']['intervals'].update({
                    'interval': self._get_parameters_list(self._interval_expression_keys,
                                                          self._interval_expression_values)
                })
        return self._request_body

    def get_request(self):
        return self.modified_update_schedule_request()
