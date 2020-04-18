from tableau_api_lib.exceptions import ContentNotFound, PaginationError


def get_page_attributes(query, query_func):
    """
    Get page attributes (pageNumber, pageSize, totalAvailable) from a REST API paginated response.
    :param dict query: the results of the GET request query, containing paginated data
    :param function query_func: a callable function that will issue a GET request to Tableau Server
    :return: page_number, page_size, total_available
    """
    try:
        pagination = query['pagination']
        page_number = int(pagination['pageNumber'])
        page_size = int(pagination['pageSize'])
        total_available = int(pagination['totalAvailable'])
        return page_number, page_size, total_available
    except KeyError:
        raise PaginationError(query_func)


def extract_pages(query_func,
                  content_id=None,
                  *,
                  starting_page=1,
                  page_size=100,
                  limit=None,
                  parameter_dict=None):
    """
    Extracts pages from paginated Tableau Server API responses.
    :param function query_func: a callable function that will issue a GET request to Tableau Server
    :param str content_id: the content ID value, if the query_func requires one [group_id, site_id, etc.]
    :param int starting_page: the page number to start on. Defaults to the first page (page_number = 1)
    :param int page_size: the number of objects per page. If querying users, this is the number of users per page
    :param int limit: the maximum number of objects to return. Default is no limit
    :param dict parameter_dict: a dict whose values are appended to the REST API URL endpoint as URL parameters
    :return: JSON or dict
    """
    parameter_dict = parameter_dict or {}
    extracted_pages = []
    page_number = starting_page
    extracting = True

    while extracting:
        parameter_dict.update({
            'pageNumber': 'pageNumber={}'.format(page_number),
            'pageSize': 'pageSize={}'.format(page_size)
        })
        query = process_query(query_func, content_id, parameter_dict)
        page_number, page_size, total_available = get_page_attributes(query, query_func)
        if total_available == 0:
            return {}
        extracted_pages, extracting, page_number = update_pagination_params(query,
                                                                            extracted_pages,
                                                                            page_number,
                                                                            page_size,
                                                                            total_available,
                                                                            limit,
                                                                            extracting)
    return extracted_pages


def process_query(query_func, content_id, parameter_dict):
    if content_id:
        if isinstance(content_id, str):
            try:
                query = query_func(content_id, parameter_dict=parameter_dict).json()
            except TypeError:
                raise PaginationError(query_func)
        else:
            raise TypeError('The content ID value passed to {} must be a string.'.format(query_func.__name__))
    else:
        try:
            query = query_func(parameter_dict=parameter_dict).json()
        except TypeError:
            raise PaginationError(query_func)
    return query


def update_pagination_params(query, extracted_pages, page_number, page_size, total_available, limit, extracting):
    try:
        outer_key = [key for key in query.keys() if key != 'pagination'].pop()
        inner_key = list(query[outer_key].keys()).pop()
        extracted_pages += query[outer_key][inner_key]
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
