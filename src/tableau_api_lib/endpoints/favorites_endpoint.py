from tableau.client.endpoints import BaseEndpoint


class FavoritesEndpoint(BaseEndpoint):
    """
    Favorites endpoint for Tableau Server API requests.

    :param ts_connection:           The Tableau Server connection object.
    :type ts_connection:            class
    :param add_to_favorites:        Boolean flag; True if adding item to favorites, False otherwise.
    :type add_to_favorites:         boolean
    :param delete_from_favorites:   Boolean flag; True if deleting item from favorites, False otherwise.
    :type delete_from_favorites:    boolean
    :param object_type:             The Tableau object type being considered.
    :type object_type:              string
    :param object_id:               The ID of the specific Tableau object being considered.
    :type object_id:                string
    :param get_user_favorites:      Boolean flag; True if getting the user's favorite items, False otherwise.
    :type get_user_favorites:       boolean
    :param user_id:                 The user ID.
    :type user_id:                  string
    :param parameter_dict:          Dictionary of URL parameters to append. The value in each key-value pair
                                    is the literal text that will be appended to the URL endpoint.
    :type parameter_dict:           dict
    """
    def __init__(self,
                 ts_connection,
                 add_to_favorites=False,
                 delete_from_favorites=False,
                 object_type=None,
                 object_id=None,
                 get_user_favorites=False,
                 user_id=None,
                 parameter_dict=None):

        super().__init__(ts_connection)
        self._add_to_favorites = add_to_favorites
        self._delete_from_favorites = delete_from_favorites
        self._object_type = object_type.lower() if object_type else None
        self._object_id = object_id
        self._get_user_favorites = get_user_favorites
        self._user_id = user_id
        self._parameter_dict = parameter_dict

    @property
    def valid_object_types(self):
        return [
            'datasource',
            'project',
            'workbook',
            'view'
        ]

    def _validate_object_type(self):
        if self._object_type.capitalize() in self.valid_object_types:
            pass
        else:
            self._invalid_parameter_exception()

    @property
    def base_favorites_url(self):
        return "{0}/api/{1}/sites/{2}/favorites".format(self._connection.server,
                                                        self._connection.api_version,
                                                        self._connection.site_id)

    @property
    def base_favorites_user_id_url(self):
        return "{0}/{1}".format(self.base_favorites_url,
                                self._user_id)

    @property
    def base_favorites_user_object_url(self):
        return "{0}/{1}s/{2}".format(self.base_favorites_user_id_url,
                                     self._object_type,
                                     self._object_id)

    def get_endpoint(self):
        if self._add_to_favorites and self._user_id:
            url = self.base_favorites_user_id_url
        elif self._delete_from_favorites and self._user_id:
            url = self.base_favorites_user_object_url
        elif self._get_user_favorites and self._user_id:
            url = self.base_favorites_user_id_url
        else:
            self._invalid_parameter_exception()

        return self._append_url_parameters(url)
