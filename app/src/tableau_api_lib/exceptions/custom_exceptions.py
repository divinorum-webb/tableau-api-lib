class InvalidParameterException(Exception):
    """Raised when an invalid set of parameters are passed to an Endpoint or Request class"""
    def __init__(self, class_name, parameters):
        error_message = """
        "\n{} received an invalid combination of parameters.
         Evaluate the parameters below and correct accordingly:\n{}
         """.format(class_name, parameters)
        super().__init__(error_message)


class InvalidFileTypeException(Exception):
    """Raised when attempting to call publish_workbook() with an invalid file type (valid types are twbx or twb)"""
    def __init__(self, class_name, file_variety, file_extension):
        error_message = ''
        if file_variety.lower() == 'workbook':
            error_message = """
            An error occurred while calling {0}.
            The provided file extension '{2}' is not a valid Tableau {1} file extension.
            Tableau {2} file extensions must be 'twbx' or 'twb'.
            """.format(class_name, file_variety, file_extension)
        elif file_variety.lower() == 'datasource':
            error_message = """
            An error occurred while calling {0}
            The provided file extension '{2}' is not a valid Tableau {1} file extension.
            Tableau {2} file extensions must be 'tds' or 'tdsx'.
            """.format(class_name, file_variety, file_extension)
        super().__init__(error_message)
