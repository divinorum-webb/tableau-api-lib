from tableau_api_lib.api_requests import BaseRequest


class SwitchSiteRequest(BaseRequest):
    """
    Switch site request for generating API api_requests to Tableau Server.
    :param class ts_connection: the Tableau Server connection object
    :param str site_name: the name, AKA content url, of the site you are switching to
    """
    def __init__(self,
                 ts_connection,
                 site_name):

        super().__init__(ts_connection)
        self._site_name = site_name

    def base_switch_site_request(self):
        self._request_body.update({
            'site': {
                'contentUrl': self._site_name
            }
        })
        return self._request_body

    def get_request(self):
        return self.base_switch_site_request()
