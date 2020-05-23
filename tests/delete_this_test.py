from tableau_api_lib import TableauServerConnection
from tableau_api_lib import sample_config


tableau_server_config = {
    'tableau_prod': {
        'server': 'https://tableaupoc.interworks.com',
        'api_version': '3.4',
        'username': 'estam',
        'password': 'Act1Andariel!',
        'site_name': 'estam',
        'site_url': 'estam',
        'cache_buster': 'Donut',
        'temp_dir': '/dags/tableau/temp/'
    }
}


def get_schedule_ids_to_clone(required_text=None):
    if required_text:
        schedule_ids = [schedule['id'] for schedule in conn.query_schedules().json()['schedules']['schedule']
                        if required_text in schedule['name']]
    else:
        schedule_ids = [schedule['id'] for schedule in conn.query_schedules().json()['schedules']['schedule']]
    print("schedules to clone: ", schedule_ids)
    return schedule_ids


def get_schedule_details(schedule_ids):
    schedule_details = [conn.update_schedule(schedule_id).json()['schedule'] for schedule_id in schedule_ids]
    print("schedule details: ", schedule_details)
    return schedule_details


def clone_schedule_first(schedules):
    test_schedules = schedules[0:5]
    name_prefix = 'estam_test '
    responses = []
    for schedule in test_schedules:
        response = conn.create_schedule(schedule_name=name_prefix + schedule['name'],
                                        schedule_priority=schedule['priority'],
                                        schedule_type=schedule['type'],
                                        schedule_execution_order=schedule['executionOrder'],
                                        schedule_frequency=schedule['frequency'],
                                        start_time=schedule['frequencyDetails']['start'],
                                        end_time=schedule['frequencyDetails'].get('end', None),
                                        interval_expression_list=schedule['frequencyDetails']['intervals']['interval'])
        responses.append(response.json())
    print("first pass responses: ", responses)
    return responses


def clone_schedules_second(original_details, clone_details):
    responses = []
    for i, schedule in enumerate(clone_details):
        response = conn.update_schedule(schedule_id=clone_details[i]['id'],
                                        schedule_state=original_details[i]['state'])
        print(response.content)
        responses.append(response)
    print("second pass responses: ", responses)
    return responses


conn = TableauServerConnection(config_json=tableau_server_config)
conn.sign_in()

schedule_ids_to_clone = get_schedule_ids_to_clone()
schedule_details = get_schedule_details(schedule_ids_to_clone)
first_pass = clone_schedule_first(schedule_details)

print(first_pass)

schedule_ids_to_update = get_schedule_ids_to_clone(required_text='estam_test')
schedule_details_to_update = get_schedule_details(schedule_ids_to_update)
second_pass = clone_schedules_second(schedule_details, schedule_details_to_update)

print(second_pass)


# response = conn.query_schedules().json()
# schedule_properties = response['schedules']['schedule'][0].keys()
# print("schedule properties: ", schedule_properties)
#
# test_schedule_id = response['schedules']['schedule'][0]['id']
# test_schedule_name = response['schedules']['schedule'][0]['name']
# test_schedule_details = conn.update_schedule(test_schedule_id,
#                                              schedule_name=test_schedule_name)
#
# print("test schedule details: ", test_schedule_details.json())
# print("test schedule properties: ", test_schedule_details.json()['schedule'].keys())
#
# test_interval_list = test_schedule_details.json()['schedule']['frequencyDetails']['intervals']['interval']
# test_execution_order = test_schedule_details.json()['schedule']['executionOrder']
#
# print("test_interval_list: ", test_interval_list)
# print("test_execution_order: ", test_execution_order)
#
# test_schedule = test_schedule_details.json()['schedule']
#
# print("name: ", test_schedule['name'])
# print("state: ", test_schedule['state'])
# print("priority: ", test_schedule['priority'])
#
# properties_to_add = [prop for prop in test_schedule_details.json()['schedule'].keys() if
#                      prop not in schedule_properties]
# print("properties to add: ", properties_to_add)
#
# response = conn.create_schedule(schedule_name='test ' + test_schedule['name'],
#                                 schedule_priority=test_schedule['priority'],
#                                 schedule_type=test_schedule['type'],
#                                 schedule_execution_order=test_schedule['executionOrder'],
#                                 schedule_frequency=test_schedule['frequency'],
#                                 start_time=test_schedule['frequencyDetails']['start'],
#                                 end_time=None,
#                                 interval_expression_list=test_interval_list)

conn.sign_out()
