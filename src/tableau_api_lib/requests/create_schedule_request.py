from tableau.client.requests import BaseRequest


class CreateScheduleRequest(BaseRequest):
    """
    Create schedule request for generating API request URLs to Tableau Server.

    :param ts_connection:               The Tableau Server connection object.
    :type ts_connection:                class
    :param schedule_name:               The name of the schedule being created.
    :type schedule_name:                string
    :param schedule_priority:           The priority value (1-100) for the schedule
    :type schedule_priority:            string or int
    :param schedule_type:               The schedule type (Flow, Extract, or Subscription)
    :type schedule_type:                string
    :param schedule_execution_order:    Set this value to 'Parallel' to allow jobs associated with this schedule to
                                        run in parallel; set the value to 'Serial' to require the jobs to run one at
                                        a time.
    :type schedule_execution_order:     string
    :param schedule_frequency:          The granularity of the schedule (Hourly, Daily, Weekly, or Monthly).
    :type schedule_frequency:           string
    :param start_time:                  The time of day when the schedule should run (HH:MM:SS). If the frequency is
                                        set to 'Hourly', this value indicates the hour the schedule starts running.
    :type start_time:                   string
    :param end_time:                    Only set this value if the schedule frequency has been set to 'Hourly'. This
                                        value indicates the hour the schedule will stop running (HH:MM:SS).
    :type end_time:                     string
    :param interval_expression_dict:    This dict specifies the time interval(s) between jobs associated with the
                                        schedule. The value required here depends on the 'schedule_frequency' value.
                                        If 'schedule_frequency' = 'Hourly', the interval expression should be
                                        hours="interval" (where "interval" is a number [1, 2, 4, 6, 8, 12] in quotes).
                                        If 'schedule_frequency' = 'Daily', no interval needs to be specified.
                                        If 'schedule_frequency' = 'Weekly, the interval is weekDay="weekday", where
                                        weekday is one of ['Sunday', 'Monday', 'Tuesday', etc.] wrapped in quotes.
                                        If 'schedule_frequency' = 'Monthly', the interval expression is monthDay="day",
                                        where day is either the day of the month (1-31), or 'LastDay'. In both cases
                                        the value is wrapped in quotes.
    :type interval_expression_dict:     dict
    """
    def __init__(self,
                 ts_connection,
                 schedule_name,
                 schedule_priority=50,
                 schedule_type='Extract',
                 schedule_execution_order='Parallel',
                 schedule_frequency='Weekly',
                 start_time='07:00:00',
                 end_time='23:00:00',
                 interval_expression_dict={'weekDay': 'Monday'}
                 ):

        super().__init__(ts_connection)
        self._schedule_name = schedule_name
        self._schedule_priority = schedule_priority
        self._schedule_type = schedule_type
        self._schedule_execution_order = schedule_execution_order
        self._schedule_frequency = schedule_frequency
        self._start_time = start_time
        self._end_time = end_time
        self._interval_expression_dict = interval_expression_dict
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
        if self._interval_expression_dict:
            self._set_interval_expressions()
        else:
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

    def _unpack_interval_expressions_dict(self, interval_dict):
        interval_keys = []
        interval_values = []
        for key in interval_dict.keys():
            if key in self.valid_interval_keys:
                interval_keys.append(key)
                interval_values.append(interval_dict[key])
            else:
                self._invalid_parameter_exception()
        return interval_keys, interval_values

    def _set_interval_expressions(self):
        if self._interval_expression_dict:
            if any(self._interval_expression_dict.values()):
                self._interval_expression_keys, self._interval_expression_values = \
                    self._unpack_interval_expressions_dict(self._interval_expression_dict)

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
        if self._interval_expression_dict:
            self._request_body['schedule']['frequencyDetails'].update({'intervals': {}})
            self._request_body['schedule']['frequencyDetails']['intervals'].update(
                {'interval': self._get_parameters_list(self._interval_expression_keys,
                                                       self._interval_expression_values)})
        return self._request_body

    def get_request(self):
        return self.modified_create_schedule_request()
