# Overview of tableau-api-lib

This library allows developers to call all methods as seen in Tableau Server's REST API reference.
Each method returns the corresponding HTTP response, providing among other things the status code and a JSON response body.

Tableau's REST API has numerous methods you can leverage. Use their [API reference](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm) to identify the methods that help you accomplish whatever tasks you need. This library's purpose is to make calling on those methods as easy as possible.

## Use case

Suppose you wanted to download a PDF or a screenshot of each dashboard on Tableau Server. Generally speaking, you would need to:
1. get a list of all sites on your Tableau Server
2. for each site, get a list of all workbooks on the site
3. for each workbook, download the PDF / screenshot for each view in the workbook

While there is not a method in the Tableau Server REST API to print all workbook PDFs on the server, this library gives you the tools you need in order to chain together existing methods and build the functionality you need. This library makes it possible to call on all of the Tableau Server REST API methods, enabling you to automate much of your Tableau Server administrative tasks.

In the scenario above, we could accomplish the task by identifying the following methods in the REST API Reference:
1. API Reference: Query Sites  |  tableau-api-lib: query_sites()
2. API Reference: Query Workbooks on Site | tableau-api-lib: query_workbooks_on_site()
3. API Reference: Query Views for Workbook | tableau-api-lib: query_views_for_workbook()
4. API Refrerence: Query View PDF | tableau-api-lib: query_view_pdf()
5. API Reference: Query View Image | tableau-api-lib: query_view_image()
6. API Reference: Switch Site | tableau-api-lib: switch_site()

## Why use tableau-api-lib

This library strives to mirror each and every REST API method, word for word. Once you find the methods you need on the Tableau Server REST API reference, this library helps you chain them together. To use this library effectively, you first browse Tableau's REST API reference and identify the specific methods you intend to use. You may then invoke those methods using this library.

When you call on the tableau-api-lib methods, you receive an HTTP response. If you are expecting data to be returned to you (querying users, workbooks, groups, etc.) then you will likely want to access the JSON body of the response. 

``response = connection.query_sites()``

``response.json()``  --> accesses the JSON body of the response, can be accessed directly as a dict

``sites = response.json()['sites']['site']``

How did we know to access the 'sites' and 'site' element? Because these elements are documented on Tableau Server's REST API reference. Use the [reference](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm) to understand how the server will respond to your requests.

## How it all works

1. ``pip install --upgrade tableau-api-lib``
2. Open a Jupyter notebook or a Python file in the text editor of your choice
3. ``from tableau_api_lib import TableauServerConnection``
4. Import a config object (dict / JSON), or build it from scratch using the config guidelines below
5. ``connection = TableauServerConnection(your_config_object)``
6. ``connection.sign_in()``
7. Call on your Tableau Server connection to perform tasks from the REST API reference
8. ``your_connection.sign_out()``

## Defining your config object

A sample / starter configuration object is provided.

``from tableau_api_lib import sample_config``
``print(sample_config)``

The config object can have multiple environments. The default environment is defined as 'tableau_prod', and you can change this if needed by specifying an 'env' parameter when calling the TabelauServerConnection class.
For each environment you have, define them. For example:

    tableau_config = {
        'tableau_prod': {
            'server': 'https://<YOUR_PROD_SERVER>.com',
            'api_version': '<YOUR_PROD_API_VERSION>',
            'username': '<YOUR_PROD_USERNAME>',
            'password': '<YOUR_PROD_PASSWORD>',
            'site_name': '<YOUR_PROD_ITE_NAME>',
            'site_url': '<YOUR_PROD_SITE_URL>',
            'cache_buster': '',
            'temp_dir': ''
        },
        'tableau_dev': {
            'server': 'https://<YOUR_DEV_SERVER>.com',
            'api_version': '<YOUR_API_VERSION>',
            'username': '<YOUR_DEV_USERNAME>',
            'password': '<YOUR_DEV_PASSWORD>',
            'site_name': '<YOUR_DEV_SITE_NAME>',
            'site_url': '<YOUR_DEV_SITE_CONTENT_URL>',
            'cache_buster': '',
            'temp_dir': ''
        }
    }

## Try it out
### This example creates a Tableau Server connection and prints the response from invoking the REST API's "query_sites" method.
### For more details, see Tableau's [REST API reference](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm#query_sites).

    from tableau_api_lib import TableauServerConnection
        
    tableau_config = {
        'tableau_prod': {
            'server': 'https://<YOUR_PROD_SERVER>.com',
            'api_version': '<YOUR_PROD_API_VERSION>',
            'username': '<YOUR_PROD_USERNAME>',
            'password': '<YOUR_PROD_PASSWORD>',
            'site_name': '<YOUR_PROD_ITE_NAME>',
            'site_url': '<YOUR_PROD_SITE_URL>',
            'cache_buster': '',
            'temp_dir': ''
        }
    }
        
    connection = TableauServerConnection(config_json=tableau_config, env='tableau_prod')
    connection.sign_in()
    
    print(connection.query_sites().json())
    
    connection.sign_out()

## Example of building a list of tuples storing the site name and site ID for each site on the server

    connection = TableauServerConnection(tableau_config)
    connection.sign_in()
    response = connection.query_sites()
    site_list = [(site['name'], site['id']) for site in response.json()['sites']['site']]
    connection.sign_out()
    
The variable site_list is a list of all available sites, and each element of this list is a tuple which contains the site name and site ID.


## Example of unpacking paginated results

Let's say you attempt the previous example (printing all site names and site IDs), but you only see 100 sites, when your organization really has 170 sites. 
The HTTP responses you receive will paginate the results whenever you query the server for items such as sites, users, groups, etc.

By default, these HTTP responses return 100 results per page. You can modify this amount manually, or you can use our built-in util functions
to control the behavior of paginated results.

Below, you can find an example of how to unpack all of the pages in a paginated result.

    from tableau_api_lib.utils import extract_pages
    
    connection = TableauServerConnection(tableau_config)
    connection.sign_in()
    all_sites = extract_pages(connection.query_sites)
    site_list = [(site['name'], site['id']) for site in all_sites]
    connection.sign_out()
    
The extract_pages() function automatically unpacks all of the pages and results available to us via our connection's query_sites() method.

We could also play with the optional parameters in the extract_pages() function, as seen below:

    from tableau_api_lib.utils import extract_pages
    
    connection = TableauServerConnection(tableau_config)
    connection.sign_in()
    all_sites = extract_pages(conn.query_sites, starting_page=1, page_size=200, limit=500)
    site_list = [(site['name'], site['id']) for site in all_sites]
    connection.sign_out()
    
The above example will iterate through each available page of site results (each page has 200 results, a number we specified) and cuts the results off at 500 sites.
There is no default cutoff limit when using extract_pages(); if you only want to pull the first X results, you have to pass X as the 'limit' argument.

Note that in the first example, we called query_sites() directly and then needed to parse the JSON object returned by that method.
However, when we used extract_pages(), we pass the 'query_sites' method as an argument and the result returned to us is already parsed. The extract_pages() method is a clean way of obtaining the desired objects we are querying.

The functions found within tableau_api_lib.utils all build upon the base functionality supported by the Tableau Server REST API reference. You can use these pre-made functions or build your own as you see fit.