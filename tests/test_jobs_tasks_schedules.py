from tableau_api_lib import TableauServerConnection
from .config import tableau_server_config


TABLEAU_SERVER_CONFIG_ENV = 'tableau_prod'
TEST_USERNAME = tableau_server_config[TABLEAU_SERVER_CONFIG_ENV]['username']
TEST_SCHEDULE_NAME = 'temp_test_schedule'
TEST_DATASOURCE_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_datasource_with_extract.tdsx'
TEST_DATASOURCE_PREFIX = 'test_ds_'
TEST_WORKBOOK_FILE_PATH = r'C:\Users\estam\Documents\Development\python-tableau-api\test_workbook_with_extract.twbx'
TEST_WORKBOOK_PREFIX = 'test_wb_'
TEST_PROJECT_NAME = 'Test'

extract_job_id = None


def sign_in():
    connection = TableauServerConnection(tableau_server_config)
    connection.sign_in()
    return connection


def get_test_schedule_id():
    schedules = conn.query_schedules().json()['schedules']['schedule']
    for schedule in schedules:
        if schedule['name'] == TEST_SCHEDULE_NAME:
            return schedule['id']
    raise Exception('The test schedule {} was not found on Tableau Server.'.format(TEST_SCHEDULE_NAME))


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


def get_test_extract_refresh_task_id():
    test_datasource_id = get_test_datasource_id(conn)
    try:
        refresh_tasks = conn.get_extract_refresh_tasks_for_site().json()['tasks']['task']
        for task in refresh_tasks:
            task_id = task['extractRefresh']['id']
            datasource_id = task['extractRefresh']['datasource']['id']
            if datasource_id == test_datasource_id:
                return task_id
    except KeyError:
        print("No extract refresh tasks appear to exist on your Tableau Server.")


def publish_test_data_source():
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
    assert response.status_code in [201, 202]


conn = sign_in()
publish_test_data_source()
publish_test_workbook()


def test_create_schedule():
    response = conn.create_schedule(schedule_name=TEST_SCHEDULE_NAME,
                                    start_time='09:00:00')
    print(response)
    assert response.status_code in [200, 201]


def test_query_schedules():
    response = conn.query_schedules()
    assert response.status_code == 200


def test_add_data_source_to_schedule():
    test_datasource_id = get_test_datasource_id(conn)
    test_schedule_id = get_test_schedule_id()
    response = conn.add_data_source_to_schedule(test_datasource_id, test_schedule_id)
    print("test_add_data_source_to_schedule: ", response.content)
    assert response.status_code == 200


def test_get_extract_refresh_tasks_for_schedule():
    test_schedule_id = get_test_schedule_id()
    response = conn.get_extract_refresh_tasks_for_schedule(test_schedule_id)
    print("test_get_extract_refresh_tasks_for_schedule: ", response.content)
    assert response.status_code == 200


def test_get_extract_refresh_tasks_for_site():
    response = conn.get_extract_refresh_tasks_for_site()
    print("test_get_extract_refresh_tasks_for_schedule: ", response.content)
    assert response.status_code == 200


def test_get_extract_refresh_task():
    test_extract_task_id = get_test_extract_refresh_task_id()
    response = conn.get_extract_refresh_task(test_extract_task_id)
    print("test_get_extract_refresh_task: ", response.content)
    assert response.status_code == 200


def test_add_workbook_to_schedule():
    test_workbook_id = get_test_workbook_id(conn)
    test_schedule_id = get_test_schedule_id()
    response = conn.add_workbook_to_schedule(test_workbook_id, test_schedule_id)
    print("test_add_workbook_to_schedule: ", response.content)
    assert response.status_code == 200


def test_run_extract_refresh_task():
    global extract_job_id
    test_extract_task_id = get_test_extract_refresh_task_id()
    response = conn.run_extract_refresh_task(test_extract_task_id)
    assert response.status_code == 200
    extract_job_id = response.json()['job']['id']


def test_query_jobs():
    response = conn.query_jobs()
    assert response.status_code == 200


def test_query_job():
    response = conn.query_job(extract_job_id)
    assert response.status_code == 200


def test_cancel_job():
    response = conn.cancel_job(extract_job_id)
    print(response.content)
    assert response.status_code == 200


def test_update_schedule():
    test_schedule_id = get_test_schedule_id()
    response = conn.update_schedule(test_schedule_id,
                                    schedule_priority='25',
                                    schedule_frequency='Monthly',
                                    start_time='07:30:00',
                                    interval_expression_list=[{'monthDay': '5'}])
    print(response.content)
    assert response.status_code == 200


def test_delete_schedule():
    test_schedule_id = get_test_schedule_id()
    response = conn.delete_schedule(test_schedule_id)
    assert response.status_code == 204


def test_delete_data_source():
    test_datasource_id = get_test_datasource_id(conn)
    response = conn.delete_data_source(test_datasource_id)
    assert response.status_code == 204


def test_delete_workbook():
    test_workbook_id = get_test_workbook_id(conn)
    response = conn.delete_workbook(test_workbook_id)
    assert response.status_code == 204
