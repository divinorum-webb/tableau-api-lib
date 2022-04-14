from typing import List

from tableau_api_lib.api_requests import BaseRequest
from tableau_api_lib.tableau_server_connection import TableauServerConnection


class CreateGroupRequest(BaseRequest):
    """Builds the request body for creating groups using Tableau's REST API."""

    def __init__(self, ts_connection: TableauServerConnection, **kwargs):
        super().__init__(ts_connection)
        self._new_group_name = kwargs.get("new_group_name")
        self._active_directory_group_name = kwargs.get("active_directory_group_name")
        self._active_directory_domain_name = kwargs.get("active_directory_domain_name")
        self._minimum_site_role = kwargs.get("minimum_site_role")
        self._license_mode = kwargs.get("license_mode")
        self._active_directory_source = "ActiveDirectory"
        self._validate_inputs()

    def _validate_inputs(self) -> None:
        valid = True
        if self._license_mode and not self._minimum_site_role:
            valid = False
        if self._active_directory_group_name and not self._active_directory_domain_name:
            valid = False
        if self._active_directory_group_name and not (self._minimum_site_role and self._license_mode):
            raise ValueError("Active Directory groups require setting `minimum_site_role` role and `license_mode`.")
        if valid is False:
            self._invalid_parameter_exception()

    @property
    def is_active_directory_request(self) -> bool:
        return all([self._active_directory_domain_name, self._active_directory_group_name])

    @property
    def valid_site_roles(self) -> List[str]:
        # TODO(elliott): move this type of configuration to enumeration (do for all request classes)
        return [
            "Unlicensed",
            "Viewer",
            "Explorer",
            "ExplorerCanPublish",
            "Creator",
            "SiteAdministratorExplorer",
        ]

    @property
    def valid_license_modes(self) -> List[str]:
        return ["onSync", "onLogin"]

    @property
    def required_local_group_param_keys(self) -> List[str]:
        return ["name"]

    @property
    def optional_local_source_param_keys(self) -> List[str]:
        return ["grantLicenseMode", "minimumSiteRole"]

    @property
    def required_local_group_param_values(self) -> List[str]:
        return [self._new_group_name]

    @property
    def optional_local_group_param_values(self) -> List[str]:
        return [self._license_mode, self._minimum_site_role]

    @property
    def required_active_directory_group_param_keys(self) -> List[str]:
        return ["name", "source", "domainName"]

    @property
    def optional_active_directory_group_param_keys(self) -> List[str]:
        return ["grantLicenseMode", "siteRole"]

    @property
    def required_active_directory_group_param_values(self) -> List[str]:
        return [
            self._new_group_name,
            self._active_directory_source,
            self._active_directory_domain_name,
        ]

    @property
    def optional_active_directory_source_param_values(self) -> List[str]:
        return [self._license_mode, self._minimum_site_role]

    def base_create_local_group_request(self) -> dict:
        if all(self.required_local_group_param_values):
            self._request_body.update({"group": {}})
            self._request_body["group"].update(
                # TODO(elliott): refactor this process for all classes to iterate dict.items()
                self._get_parameters_dict(
                    self.required_local_group_param_keys,
                    self.required_local_group_param_values,
                )
            )
        else:
            self._invalid_parameter_exception()
        return self._request_body

    def modified_create_local_group_request(self) -> dict:
        if any(self.optional_local_group_param_values):
            self._request_body["group"].update(
                self._get_parameters_dict(
                    self.optional_local_source_param_keys,
                    self.optional_local_group_param_values,
                )
            )
        return self._request_body

    def base_create_active_directory_group_request(self) -> dict:
        if all(self.required_active_directory_group_param_values):
            self._request_body.update({"group": {}})
            self._request_body["group"].update(
                self._get_parameters_dict(
                    self.required_active_directory_group_param_keys[:1],
                    self.required_active_directory_group_param_values[:1],
                )
            )
            self._request_body["group"].update({"import": {}})
            self._request_body["group"]["import"].update(
                self._get_parameters_dict(
                    self.required_active_directory_group_param_keys[1:],
                    self.required_active_directory_group_param_values[1:],
                )
            )
        else:
            self._invalid_parameter_exception()
        return self._request_body

    def modified_create_active_directory_request(self) -> dict:
        if any(self.optional_active_directory_source_param_values):
            self._request_body["group"]["import"].update(
                self._get_parameters_dict(
                    self.optional_active_directory_group_param_keys,
                    self.optional_active_directory_source_param_values,
                )
            )
        return self._request_body

    def get_request(self):
        if self.is_active_directory_request:
            self.base_create_active_directory_group_request()
            return self.modified_create_active_directory_request()
        else:
            self.base_create_local_group_request()
            return self.modified_create_local_group_request()
