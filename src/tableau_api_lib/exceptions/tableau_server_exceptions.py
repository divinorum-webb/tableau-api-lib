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
    def __init__(self):
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
