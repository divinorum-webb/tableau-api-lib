from typing import Any, Dict, List, Union

from tableau_api_lib.api_requests import BaseRequest
from tableau_api_lib.config.api_requests import SiteConfig
from tableau_api_lib.tableau_server_connection import TableauServerConnection


class UpdateSiteRequest(BaseRequest):
    """Builds the request body for Tableau Server REST API requests updating sites."""

    def __init__(self, ts_connection: TableauServerConnection, **kwargs: Dict[str, Any]):

        super().__init__(ts_connection)
        self._kwargs = kwargs
        self._validate_inputs()
        self._request_body = {"site": {}}

    @property
    def valid_extract_encryption_modes(self) -> List[Union[str, None]]:
        return ["enforced", "enabled", "disabled", None]

    def _validate_inputs(self) -> None:
        invalid_inputs = []
        if self._kwargs.get("extract_encryption_mode") not in self.valid_extract_encryption_modes:
            invalid_inputs.append("extract_encryption_mode")
        if any(invalid_inputs):
            self._invalid_parameter_exception()

    @property
    def optional_param_keys(self) -> List[str]:
        return ["name", "contentUrl", "adminMode", "state", *SiteConfig.OPTIONAL_PARAM_KEYS.value]

    @property
    def optional_param_values(self) -> List[Union[str, None]]:
        params = ["site_name", "content_url", "admin_mode", "state", *SiteConfig.OPTIONAL_PARAM_VALUES.value]
        return [self._kwargs.get(param) for param in params]

    def base_update_site_request(self) -> Dict[str, Any]:
        if self._kwargs.get("user_quota") and self._kwargs.get("admin_mode") != "ContentOnly":
            self._request_body["site"].update({"userQuota": str(self._kwargs.get("user_quota"))})
        elif self._kwargs.get("user_quota"):
            self._invalid_parameter_exception()

        self._request_body["site"].update(
            self._get_parameters_dict(self.optional_param_keys, self.optional_param_values)
        )
        return self._request_body

    def get_request(self) -> Dict[str, Any]:
        return self.base_update_site_request()
