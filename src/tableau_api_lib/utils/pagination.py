from types import MethodType
from typing import Any, Dict, List, Optional, Tuple, Union

from typeguard import typechecked

from tableau_api_lib.exceptions import ContentNotFound, PaginationError


def get_page_attributes(query: dict, query_func: MethodType) -> Tuple:
    """Returns page attributes (pageNumber, pageSize, totalAvailable) from a REST API paginated response.

    Args:
        query: The results of the GET request query, containing paginated data.
        query_func: A TableauServerConnection method that will issue a GET request to Tableau Server.

    Returns:
        A tuple describing the active page number, the page size, and the total items available.

    Raises:
        PaginationError: An error triggered when pagination is attempted on a non-paginated object.
    """
    try:
        pagination = query["pagination"]
        page_number = int(pagination["pageNumber"])
        page_size = int(pagination["pageSize"])
        total_available = int(pagination["totalAvailable"])
        return page_number, page_size, total_available
    except KeyError:
        raise PaginationError(query_func)


def extract_pages(
    query_func: MethodType,
    content_id: Optional[str] = None,
    *,
    starting_page: Optional[int] = 1,
    page_size: Optional[int] = 100,
    limit: Optional[int] = None,
    parameter_dict: Optional[Dict[str, Any]] = None,
) -> Union[List[Dict[str, Any]], Dict]:
    """Extracts all available pages from a paginated Tableau Server API response.

    Args:
        query_func: A function that will issue a GET request via the Tableau REST API.
        content_id: The luid for the desired content [group_id, site_id, etc].
        starting_page: The page number to start on. Defaults to the first page (page_number = 1).
        page_size: The maximum number of objects (results) to be returned in any given page.
        limit: The maximum number of objects to return. By default there is no limit.
        parameter_dict: A dict whose values are appended to the REST API URL endpoint as URL parameters.

    Returns:
        A list of JSON / dicts containing the contents of the paginated items.
    """
    parameter_dict = parameter_dict or {}
    extracted_pages = []
    page_number = starting_page
    extracting = True

    while extracting:
        parameter_dict.update({"pageNumber": f"pageNumber={page_number}", "pageSize": f"pageSize={page_size}"})
        query_results = process_query(query_func=query_func, content_id=content_id, parameter_dict=parameter_dict)
        page_number, page_size, total_available = get_page_attributes(query=query_results, query_func=query_func)
        if total_available == 0:
            return [{}]
        extracted_pages, extracting, page_number = update_pagination_params(
            query_results, extracted_pages, page_number, page_size, total_available, limit, extracting
        )
    return extracted_pages


@typechecked
def process_query(query_func: MethodType, content_id: Optional[str], parameter_dict: Dict[str, Any]) -> Dict[Any, Any]:
    """Processes a dynamic GET request via the Tableau REST API.

    Some of the tableau-api-lib methods require a content ID while others will throw an error if an unexpected
    content ID value is passed in when the function is invoked. This function handles both scenarios dynamically.

    Args:
        query_func: The tableau-api-lib method that will be invoked.
        content_id: The luid for the content variety being queried.
        parameter_dict: The dict describing optional additional query parameters.

    Returns:
        The JSON / dict response from the Tableau Server whose content is being queried.

    Raises:
        PaginationError: An error triggered when pagination is attempted on a non-paginated object.
    """
    if content_id:
        try:
            query_results = query_func(content_id, parameter_dict=parameter_dict).json()
        except TypeError:
            raise PaginationError(func=query_func)
    else:
        try:
            query_results = query_func(parameter_dict=parameter_dict).json()
        except TypeError:
            raise PaginationError(func=query_func)
    return query_results


def update_pagination_params(
    query_results: Dict[str, Any],
    extracted_pages: Any,
    page_number: int,
    page_size: int,
    total_available: int,
    limit: int,
    extracting: bool,
) -> Tuple:
    """Updates pagination parameters when iterating through all available pages for a variety of content.

    Args:
        query_results: The JSON / dict response received from Tableau Server following a GET request.
        extracted_pages: The pages that have been extracted from the REST API request.
        page_number: tbd
        page_size: tbd
        total_available: tbd
        limit: tbd
        extracting: tbd

    Returns:
        Pagination parameters describing active state of the pagination process.

    Raises:
        ContentNotFound: An exception thrown when no content of the variety queried exists on the Tableau Server.
    """
    try:
        outer_key = [key for key in query_results.keys() if key != "pagination"].pop()
        inner_key = list(query_results[outer_key].keys()).pop()
        extracted_pages += query_results[outer_key][inner_key]
    except IndexError:
        raise ContentNotFound()

    if limit:
        if limit <= len(extracted_pages):
            extracted_pages = extracted_pages[:limit]
            extracting = False
    elif total_available <= (page_number * page_size):
        extracting = False
    else:
        page_number += 1
    return extracted_pages, extracting, page_number
