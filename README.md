This library allows developers to call all methods as seen in Tableau Server's REST API reference.
Each method returns the corresponding HTTP response, providing among other things the status code and a JSON response body.

Tableau's REST API has several methods you can leverage. Use their [API reference](https://help.tableau.com/current/api/rest_api/en-us/REST/rest_api_ref.htm) to identify the methods that help you accomplish whatever tasks you need. This library's purpose is to make calling on those methods as easy as possible.

If you wanted to download a PDF or a screenshot of every dashboard on Tableau Server, you would need to:
1. get a list of all sites
2. for each site, get a list of all workbooks
3. for each workbook, download the PDF / screenshot

While there is not a method in the Tableau Server REST API to print all workbook PDFs, this library gives you the tools you need to chain together all of the methods which do exist, enabling you to automate much of your Tableau Server administrative tasks.

In the scenario above, we could accomplish the task by identifying the following methods in the REST API Reference:
1. API Reference: Query Sites  |  tableau-api-lib: query_sites()
2. API Reference: Query Workbooks on Site | tableau-api-lib: query_workbooks_on_site()
3. API Reference: Query Views for Workbook | tableau-api-lib: query_views_for_workbook()
4. API Refrerence: Query View PDF | tableau-api-lib: query_view_pdf()
5. API Reference: Query View Image | tableau-api-lib: query_view_image()

This library strives to mirror each and every REST API method, word for word. Once you find the methods you need on the Tableau Server REST API reference, this library helps you chain them together. 

When you call on the tableau-api-lib methods, you receive an HTTP response. If you are expecting data to be returned to you (querying users, workbooks, groups, etc.) then you will likely want to access the JSON body of the response. 

``response = connection.query_sites()``

``response.json()``  --> accesses the JSON body of the response, can be accessed directly as a dict

``sites = response.json()['sites']['site']``

How did we know to access the 'sites' and 'site' element? Because these elements are documented on Tableau Server's REST API reference. Use the reference to understand how the server will respond to your requests.

## How it all works

1. **pip install --upgrade tableau-api-lib**
2. Open a Jupyter notebook or a Python file in the text editor of your choice
3. **from tableau_api_lib import TableauServerConnection**
4. Import a config object (dict / JSON), or build it from scratch using the config guidelines below
5. **your_connection = TableauServerConnection(your_config_object)**
6. **your_connection.sign_in()**
7. Call on your Tableau Server connection to perform tasks from the REST API reference
8. **your_connection.sign_out**

## Defining your config object

A sample / starter configuration object is provided.

**from tableau_api_lib import sample_config**
**print(sample_config)**

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
