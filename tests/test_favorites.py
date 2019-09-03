from tableau_api_lib import TableauServerConnection
from .config import tableau_server_config


TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_DATASOURCE_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_datasource_with_extract.tdsx'
TEST_DATASOURCE_PREFIX = 'test_ds_'
TEST_WORKBOOK_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_workbook_with_extract.twbx'
TEST_WORKBOOK_PREFIX = 'test_wb_'
TEST_PROJECT_NAME = 'Test'
TEST_FAVORITE_LABEL = 'Test temp favorite label'


def get_test_user_id(connection):
    users = connection.get_users_on_site().json()['users']['user']
    for user in users:
        if user['name'] == TEST_USERNAME:
            return user['id']
    return users.pop()['id']


def get_test_project_id(connection):
    projects = connection.query_projects().json()['projects']['project']
    for project in projects:
        if project['name'] == TEST_PROJECT_NAME:
            return project['id']
    return projects.pop()['id']


def get_test_workbook_id(connection):
    print("test_workbook_id: ", connection.query_workbooks_for_site().content)
    workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
    for workbook in workbooks:
        if workbook['name'] == TEST_WORKBOOK_PREFIX + 'a':
            return workbook['id']
    return workbooks.pop()['id']


def get_test_view_id(connection):
    test_workbook_id = get_test_workbook_id(connection)
    test_views = connection.query_views_for_workbook(test_workbook_id).json()['views']['view']
    return test_views.pop()['id']


def get_test_datasource_id(connection):
    datasources = connection.query_data_sources().json()['datasources']['datasource']
    for datasource in datasources:
        if datasource['name'] == TEST_DATASOURCE_PREFIX + 'a':
            return datasource['id']
    return datasources.pop()['id']


def publish_data_source():
    test_project_id = get_test_project_id(conn)
    response = conn.publish_data_source(datasource_file_path=TEST_DATASOURCE_FILE_PATH,
                                        datasource_name=TEST_DATASOURCE_PREFIX + 'a',
                                        project_id=test_project_id,
                                        parameter_dict={
                                            'overwrite': 'overwrite=true',
                                            'asJob': 'asJob=true'
                                        })
    assert response.status_code in [201, 202]


def publish_test_workbook():
    test_project_id = get_test_project_id(conn)
    response = conn.publish_workbook(workbook_file_path=TEST_WORKBOOK_FILE_PATH,
                                     workbook_name=TEST_WORKBOOK_PREFIX+'a',
                                     project_id=test_project_id,
                                     show_tabs_flag=False,
                                     parameter_dict={
                                         'overwrite': 'overwrite=true',
                                         'asJob': 'asJob=true'
                                     })
    print("Published workbook: {}".format(response.content))
    assert response.status_code in [201, 202]


def sign_in():
    conn = TableauServerConnection(tableau_server_config)
    conn.sign_in()
    return conn


conn = sign_in()
publish_data_source()
publish_test_workbook()


def test_add_data_source_to_favorites():
    response = conn.add_data_source_to_favorites(datasource_id=get_test_datasource_id(conn),
                                                 user_id=get_test_user_id(conn),
                                                 favorite_label=TEST_FAVORITE_LABEL)
    assert response.status_code == 200


def test_add_project_to_favorites():
    response = conn.add_project_to_favorites(project_id=get_test_project_id(conn),
                                             user_id=get_test_user_id(conn),
                                             favorite_label=TEST_FAVORITE_LABEL)
    assert response.status_code == 200


def test_add_view_to_favorites():
    response = conn.add_view_to_favorites(view_id=get_test_view_id(conn),
                                          user_id=get_test_user_id(conn),
                                          favorite_label=TEST_FAVORITE_LABEL)
    assert response.status_code == 200


def test_add_workbook_to_favorites():
    response = conn.add_workbook_to_favorites(workbook_id=get_test_workbook_id(conn),
                                              user_id=get_test_user_id(conn),
                                              favorite_label=TEST_FAVORITE_LABEL)
    assert response.status_code == 200


def test_delete_data_source_from_favorites():
    response = conn.delete_data_source_from_favorites(datasource_id=get_test_datasource_id(conn),
                                                      user_id=get_test_user_id(conn))
    assert response.status_code == 204


def test_delete_project_from_favorites():
    response = conn.delete_project_from_favorites(project_id=get_test_project_id(conn),
                                                  user_id=get_test_user_id(conn))
    assert response.status_code == 204


def test_delete_view_from_favorites():
    response = conn.delete_view_from_favorites(view_id=get_test_view_id(conn),
                                               user_id=get_test_user_id(conn))
    assert response.status_code == 204


def test_delete_workbook_from_favorites():
    response = conn.delete_workbook_from_favorites(workbook_id=get_test_workbook_id(conn),
                                                   user_id=get_test_user_id(conn))
    assert response.status_code == 204


def test_get_favorites_for_user():
    response = conn.get_favorites_for_user(user_id=get_test_user_id(conn))
    assert response.status_code == 200
