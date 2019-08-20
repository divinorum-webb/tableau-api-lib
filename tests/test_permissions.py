import time

from tableau.client.tableau_server_connection import TableauServerConnection
from tableau.client.config.config import tableau_server_config

TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_DATASOURCE_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_datasource_with_extract.tdsx'
TEST_DATASOURCE_PREFIX = 'test_ds_'
TEST_WORKBOOK_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_workbook_with_extract.twbx'
TEST_WORKBOOK_PREFIX = 'test_wb_'
TEST_PROJECT_NAME = 'Test'
TEST_ALT_PROJECT_NAME = 'Test Alt'
TEST_GROUP_NAME = 'Test Group'
TEST_USER = 'estam_test'


def sign_in():
    connection = TableauServerConnection(tableau_server_config)
    connection.sign_in()
    return connection


def get_test_user_id(connection):
    users = connection.get_users_on_site().json()['users']['user']
    for user in users:
        if user['name'] == TEST_USER:
            return user['id']
    return users.pop()['id']


def get_test_project_id(connection):
    projects = connection.query_projects().json()['projects']['project']
    for project in projects:
        if project['name'] == TEST_PROJECT_NAME:
            return project['id']
    return projects.pop()['id']


def get_test_datasource_id(connection):
    datasources = connection.query_data_sources().json()['datasources']['datasource']
    for datasource in datasources:
        if datasource['name'] == TEST_DATASOURCE_PREFIX + 'a':
            return datasource['id']
    return datasources.pop()['id']


def get_test_workbook_id(connection):
    workbooks = connection.query_workbooks_for_site().json()['workbooks']['workbook']
    for workbook in workbooks:
        if workbook['name'] == TEST_WORKBOOK_PREFIX + 'a':
            return workbook['id']
    return workbooks.pop()['id']


def get_test_view_id(connection):
    test_workbook_id = get_test_workbook_id(connection)
    test_views = connection.query_views_for_workbook(test_workbook_id).json()['views']['view']
    return test_views.pop()['id']


def get_test_group_id(connection):
    groups = connection.query_groups().json()['groups']['group']
    for group in groups:
        if group['name'] == TEST_GROUP_NAME:
            return group['id']
    return groups.pop()['id']


def publish_workbook():
    test_project_id = get_test_project_id(conn)
    response = conn.publish_workbook(workbook_file_path=TEST_WORKBOOK_FILE_PATH,
                                     workbook_name=TEST_WORKBOOK_PREFIX+'a',
                                     project_id=test_project_id,
                                     show_tabs_flag=False,
                                     parameter_dict={
                                         'overwrite': 'overwrite=true',
                                         'asJob': 'asJob=true'
                                     })
    if response.status_code in [201, 202]:
        print("Workbook '{}' successfully published.".format(TEST_WORKBOOK_PREFIX + 'a'))


def publish_data_source():
    test_project_id = get_test_project_id(conn)
    response = conn.publish_data_source(datasource_file_path=TEST_DATASOURCE_FILE_PATH,
                                        datasource_name=TEST_DATASOURCE_PREFIX + 'a',
                                        project_id=test_project_id,
                                        parameter_dict={
                                            'overwrite': 'overwrite=true',
                                            'asJob': 'asJob=true'
                                        })
    if response.status_code in [201, 202]:
        print("Datasource '{}' successfully published.".format(TEST_DATASOURCE_PREFIX + 'a'))


conn = sign_in()
# publish_workbook()
# publish_data_source()


# def test_add_test_user():
#     response = conn.add_user_to_site(TEST_USER, site_role='creator')
#     assert response.status_code in [201, 202]
#
#
# def test_add_test_group():
#     response = conn.create_group(TEST_GROUP_NAME)
#     assert response.status_code == 201
#
#
def test_query_data_source_permissions():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.query_data_source_permissions(test_datasource_id)
    print(response.content)
    assert response.status_code == 200


def test_query_project_permissions():
    test_project_id = get_test_project_id(conn)
    response = conn.query_project_permissions(test_project_id)
    print(response.content)
    assert response.status_code == 200


def test_query_default_permissions_workbooks():
    test_project_id = get_test_project_id(conn)
    response = conn.query_default_permissions(test_project_id, 'workbooks')
    print(response.content)
    assert response.status_code == 200


def test_query_default_permissions_datasources():
    test_project_id = get_test_project_id(conn)
    response = conn.query_default_permissions(test_project_id, 'datasource')
    print(response.content)
    assert response.status_code == 200


def test_query_default_permissions_flows():
    test_project_id = get_test_project_id(conn)
    response = conn.query_default_permissions(test_project_id, 'flows')
    print(response.content)
    assert response.status_code == 200


def test_query_view_permissions():
    test_view_id = get_test_view_id(conn)
    response = conn.query_view_permissions(test_view_id)
    assert(response.status_code == 200)


def test_query_workbook_permissions():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.query_workbook_permissions(test_workbook_id)
    assert response.status_code == 200


def test_add_data_source_permissions_user():
    test_datasource_id = get_test_datasource_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_data_source_permissions(test_datasource_id,
                                                user_capability_dict={
                                                    'Connect': 'Allow',
                                                    'Read': 'Allow'
                                                },
                                                user_id=test_user_id)
    print(response.content)
    assert response.status_code == 200


def test_add_project_permissions_user():
    test_project_id = get_test_project_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_project_permissions(test_project_id,
                                            user_capability_dict={
                                                'ProjectLeader': 'Allow',
                                                'Read': 'Deny'
                                            },
                                            user_id=test_user_id)
    print(response.content)
    assert response.status_code == 200


def test_add_default_permissions_workbooks():
    test_project_id = get_test_project_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_default_permissions(test_project_id,
                                            project_permissions_object='workbook',
                                            user_capability_dict={
                                                'Read': 'Allow',
                                                'ExportXml': 'Deny'
                                            },
                                            user_id=test_user_id)
    assert response.status_code == 200


def test_add_default_permissions_datasources():
    test_project_id = get_test_project_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_default_permissions(test_project_id,
                                            project_permissions_object='datasource',
                                            user_capability_dict={
                                                'Read': 'Allow',
                                                'ExportXml': 'Deny'
                                            },
                                            user_id=test_user_id)
    assert response.status_code == 200


def test_add_default_permissions_flows():
    test_project_id = get_test_project_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_default_permissions(test_project_id,
                                            project_permissions_object='flows',
                                            user_capability_dict={
                                                'Read': 'Allow',
                                                'ExportXml': 'Deny'
                                            },
                                            user_id=test_user_id)
    assert response.status_code == 200


def test_add_view_permissions():
    test_view_id = get_test_view_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_view_permissions(view_id=test_view_id,
                                         user_capability_dict={
                                             'Read': 'Allow',
                                             'ExportData': 'Deny'
                                         },
                                         user_id=test_user_id)
    print(response.content)
    assert response.status_code == 200


def test_add_workbook_permissions():
    test_workbook_id = get_test_workbook_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_workbook_permissions(test_workbook_id,
                                             user_capability_dict={
                                                 'Read': 'Allow',
                                                 'ExportXml': 'Deny'
                                             },
                                             user_id=test_user_id)
    assert response.status_code == 200


def test_add_datasource_permissions():
    test_datasource_id = get_test_datasource_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.add_data_source_permissions(test_datasource_id,
                                                user_capability_dict={
                                                    'Read': 'Allow',
                                                    'ExportXml': 'Deny'
                                                },
                                                user_id=test_user_id)
    assert response.status_code == 200


def test_delete_data_source_permission():
    test_datasource_id = get_test_datasource_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.delete_data_source_permission(test_datasource_id,
                                                  delete_permissions_object='user',
                                                  delete_permissions_object_id=test_user_id,
                                                  capability_name='ExportXml',
                                                  capability_mode='Deny')
    print(response.content)
    assert response.status_code == 204


def test_delete_project_permission():
    test_project_id = get_test_project_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.delete_project_permission(test_project_id,
                                              'user',
                                              test_user_id,
                                              capability_name='ProjectLeader',
                                              capability_mode='Allow')
    print(response.content)
    assert response.status_code == 204


def test_delete_default_permission_workbook():
    test_project_id = get_test_project_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.delete_default_permission(project_id=test_project_id,
                                              project_permissions_object='workbook',
                                              delete_permissions_object='user',
                                              delete_permissions_object_id=test_user_id,
                                              capability_name='ExportXml',
                                              capability_mode='Deny')
    print(response.content)
    assert response.status_code == 204


def test_delete_view_permission():
    test_view_id = get_test_view_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.delete_view_permission(test_view_id,
                                           'user',
                                           test_user_id,
                                           capability_name='ExportData',
                                           capability_mode='Deny')
    print(response.content)
    assert response.status_code == 204


def test_delete_workbook_permission():
    test_workbook_id = get_test_workbook_id(conn)
    test_user_id = get_test_user_id(conn)
    response = conn.delete_workbook_permission(test_workbook_id,
                                               'user',
                                               test_user_id,
                                               capability_name='ExportXml',
                                               capability_mode='Deny')
    print(response.content)
    assert response.status_code == 204


# def test_remove_test_user():
#     test_user_id = get_test_user_id(conn)
#     response = conn.remove_user_from_site(test_user_id)
#     assert response.status_code == 204
#
#
# def test_remove_test_group():
#     test_group_id = get_test_group_id(conn)
#     response = conn.delete_group(test_group_id)
#     assert response.status_code == 204
