This library allows developers to call all methods as seen in Tableau Server's REST API reference.
Each method returns the corresponding HTTP response, providing among other things the status code and a JSON response body.

## How it works

1. `pip install --upgrade tableau-api-lib
2. Open a Jupyter notebook or a Python file in the text editor of your choice
3. `from tableau_api_lib import TableauServerConnection
4. Import a config object (dict / JSON), or build it from scratch using the config guidelines below
5. **your_connection = TableauServerConnection(your_config_object)**
6. **your_connection.sign_in()**
7. Call on your Tableau Server connection to perform tasks from the REST API reference
8. **your_connection.sign_out**

## Defining your config object

A sample / starter configuration object is provided

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
