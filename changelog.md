# Changelog for tableau-api-lib

# V0.1.34
- (divinorum-webb) Fixed a bug related to the `update_group` method.

# V0.1.34
- (divinorum-webb) Fixed typo related to `requestAccessEnabled` flag in site-specific requests.

# V0.1.33
- (divinorum-webb) Added usage statistics by default in `querying.get_all_view_fields`.

# V0.1.32
- (divinorum-webb) Added support for user-defined `sep` param in `querying.get_view_data_dataframe`.

# V0.1.31
- (divinorum-webb) Merged changes from remote branch replacing a deprecated Pandas method.

# V0.1.30
- (divinorum-webb) Fixed a bug that was passing `siteRole` instead of `minimumSiteRole` when creating local groups.

# V0.1.29
- (divinorum-webb) Added support for metrics, dataroles, and lenses when querying default permissions.

# V0.1.28
- (divinorum-webb) Modified `ConnectionError` to properly describe that some connection errors may occur due to having insufficient user permissions.

# V0.1.27
- (divinorum-webb) Fixed a bug where default headers were not reset for the `run_flow_now` method.

# V0.1.26
- (divinorum-webb) Fixed bug in `delete_data_driven_alert` method.

# V0.1.25
- (divinorum-webb) Added method `run_flow_now` to support the Run Flow Now endpoint.

# V0.1.24
- (divinorum-webb) Added utils function `get_views_for_workbook_dataframe`.

# V0.1.23
- (divinorum-webb) Added support for the `Get Schedule` endpoint.
- (divinorum-webb) Added support for the `Revoke Administrator Personal Access Tokens` endpoint.

# V0.1.22
- (divinorum-webb) Added temporary workaround for Tableau Server no longer supporting the `_all_` fields 
  parameter when hitting the `Query Workbooks for Site` endpoint: by default the `querying.get_workbooks_dataframe()`
  function now returns only the `_default_` fields instead of the `_all_` fields.
- (divinorum-webb) Removed the requirement for the `publish_workbook()` method to have a password set
when publishing a workbook with embedded credentials.


# V0.1.21
- (divinorum-webb) FIX modified import statements to remove import errors.

# V0.1.20
- (divinorum-webb) FIX refactored `create_group` logic and fixed a bug that caused errors
when creating groups with active directory.

# V0.1.19
- (divinorum-webb) FIX resolved the issue raised in V0.1.18 with the `create_site` method.
The issue was that some optional parameters were incorrectly being passed as tuples.

# V0.1.18
- (divinorum-webb) FIX (temporary) disabled multiple optional parameters for the `create_site` method. 
  These parameters are triggering Java exceptions on the Tableau Server side.

# V0.1.17
- (divinorum-webb) FIX removed print statements used for testing / troubleshooting.

# V0.1.16
- (divinorum-webb) Corrected a type hints in `querying/groups.py`.
- (divinorum-webb) `extract_pages()` now always returns a list; if no pages are found for the content than the list contains an empty dict.

# V0.1.15
- (divinorum-webb) Added type checking to all functions in `querying/groups.py`.
- (divinorum-webb) Querying group users when there are no users in the group now returns an empty DataFrame.
 
# V0.1.13-14
- (divinorum-webb) Fixed a bug where downloading a workbook as .pptx was downloading PDF files.

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
