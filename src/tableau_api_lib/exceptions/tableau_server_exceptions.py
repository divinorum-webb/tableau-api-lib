class InvalidRestApiVersion(Exception):
    """
    Exception for flagging API method calls to endpoints that do not exist for the (older) version being used.
    """
    def __init__(self, func, api_version_used, api_version_required):
        """
        Generates an error message citing the api_version_used and api_version_required.
        :param str api_version_used: the REST API being used by the active connection
        :param str api_version_required: the minimum REST API version required
        """
        error_message = """
        The REST API endpoint referenced in function '{0}' requires a minimum API version of {1}.
        The API version you are using is {2}, which pre-dates the endpoint referenced.
        Please visit Tableau's REST API reference to identify the methods relevant for your legacy API version.
        """.format(func.__name__, api_version_required, api_version_used)
        super().__init__(error_message)


class InvalidTableauServerQuery(Exception):
    """
    Exception for flagging invalid Tableau Server site queries.
    """
    def __init__(self, query_object, object_property):
        """
        Generates an error message for the query object and object property provided.
        :param str query_object: the object being queries ['site', 'project', 'user', etc.]
        :param str object_property: the property being queried ['id', 'name', etc.]
        """
        error_message = """
        The active Tableau Server {} query did not contain a property for "{}".
        Please check that your Tableau Server connection is valid and signed in.
        """.format(query_object, object_property)
        super().__init__(error_message)


class ContentNotFound(Exception):
    """
    Exception for situations where Tableau Server content is queried but does not exist on the site.
    """
    def __init__(self, content_type=None, content_id=None):
        if content_type:
            error_message = """
            The {0} (id='{1}') does not exist on the active site.
            """.format(content_type, content_id)
        else:
            error_message = """
            The queried Tableau Server content [users, projects, etc.] does not exist on the active site.
            """
        super().__init__(error_message)


class ContentOverwriteDisabled(Exception):
    """
    Exception for flagging that the target server has objects with the same name as those being cloned from the source.
    """
    def __init__(self, content_object):
        error_message = """
        The target connection has at least one named {} that already exists, and no 'overwrite_policy' was specified.
        If you'd like to overwrite the existing content, set the 'overwrite_policy' value to 'overwrite'.
        """.format(content_object)
        super().__init__(error_message)


class UsersNotFound(Exception):
    """
    Exception for situations where Tableau Server users are queried but non exist on the site.
    """
    def __init__(self):
        error_message = """
        An attempt was made to query Tableau Server users, but none exist for the active site.
        Make sure users have been created on the site and try again.
        """
        super().__init__(error_message)


class PaginationError(Exception):
    """Raise an exception when users attempt to extract pages from a non-paginated REST API response."""
    def __init__(self, func):
        error_message = """
        The Tableau Server REST API method {} did not return paginated results. 
        Please verify that your connection is logged in and has a valid auth token.
        If using personal access tokens, note that only one session can be active at a time using a single token.
        Also note that the extract_pages() method wrapping this call is intended for paginated results only.
        Not all Tableau Server REST API methods support pagination. 
        """.format(func.__name__)
        super().__init__(error_message)
