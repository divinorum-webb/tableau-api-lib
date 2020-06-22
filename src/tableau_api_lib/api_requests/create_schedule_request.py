from tableau_api_lib.api_requests import BaseRequest


class CreateScheduleRequest(BaseRequest):
    """
    Builds the request body for Tableau Server REST API requests creating schedules.
    :param class ts_connection: the Tableau Server connection object
    :param str schedule_name: the name of the schedule being created
    :param str schedule_priority: the priority value (1-100) for the schedule
    :param str schedule_type: the schedule type (Flow, Extract, or Subscription)
    :param str schedule_execution_order: set this value to 'Parallel' to allow jobs associated with this schedule to run
    in parallel; set the value to 'Serial' to require the jobs to run one at a time
    :param str schedule_frequency: the granularity of the schedule (Hourly, Daily, Weekly, or Monthly)
    :param str start_time: the time of day when the schedule should run (HH:MM:SS). If the frequency is set to 'Hourly',
    this value indicates the hour the schedule starts running
    :param str end_time: only set this value if the schedule frequency has been set to 'Hourly'. This value indicates
    the hour the schedule will stop running (HH:MM:SS)
    :param list interval_expression_list: this list of dicts specifies the time interval(s) between jobs on the
    schedule. The value required here depends on the 'schedule_frequency' value. If 'schedule_frequency' = 'Hourly',
    the interval expression should be hours="interval" (where "interval" is a number [1, 2, 4, 6, 8, 12] in quotes).
    If 'schedule_frequency' = 'Daily', no interval needs to be specified.
    If 'schedule_frequency' = 'Weekly, the interval is weekDay="weekday", where weekday is one of
    ['Sunday', 'Monday', 'Tuesday', etc.] wrapped in quotes.
    If 'schedule_frequency' = 'Monthly', the interval expression is monthDay="day", where day is either the day of the
    month (1-31), or 'LastDay'. In both cases the value is wrapped in quotes.
    """
    def __init__(self,
                 ts_connection,
                 schedule_name,
                 schedule_priority=50,
                 schedule_type='Extract',
                 schedule_execution_order='Parallel',
                 schedule_frequency='Weekly',
                 start_time='07:00:00',
                 end_time=None,
                 interval_expression_list=[{'weekDay': 'Monday'}]
                 ):

        super().__init__(ts_connection)
        self._schedule_name = schedule_name
        self._schedule_priority = schedule_priority
        self._schedule_type = schedule_type
        self._schedule_execution_order = schedule_execution_order
        self._schedule_frequency = schedule_frequency
        self._start_time = start_time
        self._end_time = end_time
        self._interval_expression_list = interval_expression_list
        self._interval_expression_keys, self._interval_expression_values = None, None
        self._validate_inputs()
        self.base_create_schedule_request()

    @property
    def required_schedule_param_keys(self):
        return [
            'name',
            'priority',
            'type',
            'frequency',
            'executionOrder'
        ]

    @property
    def required_frequency_param_keys(self):
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

    def _validate_inputs(self):
        valid = True
        if self._interval_expression_list:
            self._set_interval_expressions()
        elif self._schedule_frequency != 'Daily':
            valid = False
        if not valid:
            self._invalid_parameter_exception()

    @property
    def required_schedule_param_values(self):
        return [
            self._schedule_name,
            self._schedule_priority,
            self._schedule_type,
            self._schedule_frequency,
            self._schedule_execution_order
        ]

    @property
    def required_frequency_param_values(self):
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

    def base_create_schedule_request(self):
        self._request_body.update({'schedule': {'frequencyDetails': {}}})
        return self._request_body

    def modified_create_schedule_request(self):
        self._request_body['schedule'].update(
            self._get_parameters_dict(self.required_schedule_param_keys,
                                      self.required_schedule_param_values))
        self._request_body['schedule']['frequencyDetails'].update(
            self._get_parameters_dict(self.required_frequency_param_keys,
                                      self.required_frequency_param_values))
        if self._interval_expression_list:
            self._request_body['schedule']['frequencyDetails'].update({'intervals': {}})
            self._request_body['schedule']['frequencyDetails']['intervals'].update(
                {'interval': self._get_parameters_list(self._interval_expression_keys,
                                                       self._interval_expression_values)})
        return self._request_body

    def get_request(self):
        return self.modified_create_schedule_request()
