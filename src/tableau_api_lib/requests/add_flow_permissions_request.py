from tableau.client.requests import BaseRequest


class AddFlowPermissionsRequest(BaseRequest):
    """
    Add flow permissions request body for issuing API requests to Tableau Server.

    :param ts_connection:           The Tableau Server connection object.
    :type ts_connection:            class
    :param user_capability_dict:    The dict defining user capabilities / permissions.
    :type user_capability_dict:     dict
    :param group_capability_dict:   The dict defining group capabilities / permissions.
    :type group_capability_dict:    dict
    :param flow_id:                 The flow ID for the flow whose permissions are being modified.
    :type flow_id:                  string
    :param user_id:                 The user ID.
    :type user_id:                  string
    :param group_id:                The group ID.
    :type group_id:                 string
    """
    def __init__(self,
                 ts_connection,
                 user_capability_dict=None,
                 group_capability_dict=None,
                 flow_id=None,
                 user_id=None,
                 group_id=None):

        super().__init__(ts_connection)
        self._flow_id = flow_id
        self._user_id = user_id
        self._group_id = group_id
        self._user_capability_dict = user_capability_dict
        self._group_capability_dict = group_capability_dict
        self._user_capability_names, self._user_capability_modes = None, None
        self._group_capability_names, self._group_capability_modes = None, None
        self._validate_inputs()
        self.base_add_permissions_request()

    @property
    def valid_capability_names(self):
        return [
            'ChangeHierarchy',
            'ChangePermissions',
            'Delete',
            'ExportXml',
            'Read',
            'Write'
        ]

    @property
    def valid_capability_modes(self):
        return [
            'Allow',
            'Deny'
        ]

    def _validate_inputs(self):
        if self._user_id or self._group_id:
            pass
        else:
            self._invalid_parameter_exception()
        if self._user_capability_dict or self._group_capability_dict:
            self._set_capability_variables()
        else:
            self._invalid_parameter_exception()

    def _unpack_capability_dict(self, capability_dict):
        capability_names = []
        capability_modes = []
        for key in capability_dict.keys():
            if key in self.valid_capability_names and capability_dict[key] in self.valid_capability_modes:
                capability_names.append(key)
                capability_modes.append(capability_dict[key])
            else:
                self._invalid_parameter_exception()
        return capability_names, capability_modes

    def _set_capability_variables(self):
        if self._user_capability_dict:
            if any(self._user_capability_dict.values()):
                self._user_capability_names, self._user_capability_modes = self._unpack_capability_dict(
                    self._user_capability_dict)
        if self._group_capability_dict:
            if any(self._group_capability_dict.values()):
                self._group_capability_names, self._group_capability_modes = self._unpack_capability_dict(
                    self._group_capability_dict)

    @staticmethod
    def _get_capability_parameters_list(param_keys, param_values):
        params_list = []
        for i, key in enumerate(param_keys):
            if param_values[i]:
                params_list.append({'name': key,
                                    'mode': param_values[i]})
        return params_list

    @property
    def optional_flow_param_keys(self):
        return ['id']

    @property
    def optional_flow_param_values(self):
        return [self._flow_id]

    def base_add_permissions_request(self):
        self._request_body.update({'permissions': {'granteeCapabilities': []}})
        return self._request_body

    def modified_add_permissions_request(self):
        if any(self.optional_flow_param_values):
            self._request_body['permissions'].update({'flow': {}})
            self._request_body['permissions']['flow'].update(
                self._get_parameters_dict(self.optional_flow_param_keys,
                                          self.optional_flow_param_values))
        if self._user_capability_names:
            capability_dict = {}
            capability_dict.update({'user': {'id': self._user_id}})
            capability_dict.update({'capabilities': {
                'capability': self._get_capability_parameters_list(self._user_capability_names,
                                                                   self._user_capability_modes)
            }})
            self._request_body['permissions']['granteeCapabilities'].append(capability_dict)

        if self._group_capability_names:
            capability_dict = {}
            capability_dict.update({'group': {'id': self._group_id}})
            capability_dict.update({'capabilities': {
                'capability': self._get_capability_parameters_list(self._group_capability_names,
                                                                   self._group_capability_modes)
            }})
            self._request_body['permissions']['granteeCapabilities'].append(capability_dict)

        return self._request_body

    def get_request(self):
        return self.modified_add_permissions_request()
