This library allows developers to call all methods as seen in Tableau Server's REST API reference.
Each method returns the corresponding HTTP response, providing among other things the status code and a JSON response body.

## How it works

1. **pip install --upgrade tableau-api-lib**
2. Open a Jupyter notebook or a Python file in the text editor of your choice
3. **from tableau_api_lib import TableauServerConnection**
4. Import a config object (dict / JSON), or build it from scratch using the config guidelines below
5. **your_connection = TableauServerConnection(your_config_object)**
6. **your_connection.sign_in()**
7. Call on your Tableau Server connection to perform tasks from the REST API reference
8. **your_connection.sign_out**
