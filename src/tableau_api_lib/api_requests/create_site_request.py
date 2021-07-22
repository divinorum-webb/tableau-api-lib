from typing import Any, Dict, List, Union

from tableau_api_lib.api_requests import BaseRequest
from tableau_api_lib.config.api_requests import SiteConfig
from tableau_api_lib.tableau_server_connection import TableauServerConnection


class CreateSiteRequest(BaseRequest):
    """Builds the body for Tableau Server REST API requests creating new Tableau sites."""

    def __init__(
        self, ts_connection: TableauServerConnection, site_name: str, content_url: str, **kwargs: Dict[str, Any]
    ):

        super().__init__(ts_connection)
        self._site_name = site_name
        self._content_url = content_url
        self._kwargs = kwargs
        self._validate_inputs()
        self.base_create_site_request()

    @property
    def valid_extract_encryption_modes(self) -> List[Union[str, None]]:
        return ["enforced", "enabled", "disabled", None]

    def _validate_inputs(self) -> None:
        invalid_inputs = []
        if self._kwargs.get("extract_encryption_mode") not in self.valid_extract_encryption_modes:
            invalid_inputs.append(self._kwargs.get("extract_encryption_mode"))
        if any(invalid_inputs):
            self._invalid_parameter_exception()

    @property
    def optional_param_keys(self) -> List[str]:
        return SiteConfig.OPTIONAL_PARAM_KEYS.value

    @property
    def optional_param_values(self) -> List[Union[str, None]]:
        params = SiteConfig.OPTIONAL_PARAM_VALUES.value
        return [self._kwargs.get(param) for param in params]

    def base_create_site_request(self) -> Dict[str, Any]:
        self._request_body.update(
            {
                "site": {
                    "name": self._site_name,
                    "contentUrl": self._content_url,
                    "adminMode": self._kwargs.get("admin_mode"),
                }
            }
        )
        return self._request_body

    def modified_create_site_request(self) -> Dict[str, Any]:
        if self._kwargs.get("user_quota") and self._kwargs.get("admin_mode") != "ContentOnly":
            self._request_body["site"].update({"userQuota": str(self._kwargs.get("user_quota"))})
        elif self._kwargs.get("user_quota"):
            self._invalid_parameter_exception()

        self._request_body["site"].update(
            self._get_parameters_dict(self.optional_param_keys, self.optional_param_values)
        )
        return self._request_body

    def get_request(self) -> Dict[str, Any]:
        return self.modified_create_site_request()
