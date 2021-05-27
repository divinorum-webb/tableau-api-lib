# Changelog for tableau-api-lib

# V0.1.12
- (divinorum-webb) Removed various config details from authentication validation exceptions.

# V0.1.11
- (divinorum-webb) Improved error handling when 'env' does not exist in the given config.
- (divinorum-webb) Added `packaging` as a requirement for the `tableau-api-lib` library during install.

# V0.1.10
- (divinorum-webb) Added the download_workbook_powerpoint() method.

# V0.1.09
- (divinorum-webb) Removed unnecessary print statements from `utils.pagination` functions.

# V0.1.08
- (divinorum-webb) Removed unnecessary print statements from `utils.querying.tasks` functions.

# V0.1.07
- (divinorum-webb) Added support for a querying function returning a Pandas DataFrame for all extract refresh tasks on a site.

# V0.1.06
- (divinorum-webb) Refactored docstrings and TableauServerConnection method parameter documentation.
- (divinorum-webb) Improved error message when an invalid server address (no scheme) is provided.

# V0.1.05
- (divinorum-webb) Fixed a bug related to typechecking paginated results.

# V0.1.04
- (divinorum-webb) Added typeguard as an install requirement.

# V0.0.112
- (divinorum-webb) Added support for new create_subscription and update_subscription methods.
- (divinorum-webb) Added support for response.encoding = response.apparent_encoding.
