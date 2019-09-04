"""
The functions below enable you to clone the schedules from Server A to Server B.
This is particularly useful when doing site exports / imports.
The only function that should be directly called upon from outside this file is the 'clone_schedules' function.
"""
from tableau_api_lib.utils import extract_pages


def get_schedule_ids(conn, schedule_names=None):
    if schedule_names:
        schedule_ids = [schedule['id'] for schedule in extract_pages(conn.query_schedules)
                        if schedule['name'] in schedule_names]
    else:
        schedule_ids = [schedule['id'] for schedule in extract_pages(conn.query_schedules)]
    return schedule_ids


def get_schedule_details(conn, schedule_ids):
    schedule_details = [conn.update_schedule(schedule_id).json()['schedule'] for schedule_id in schedule_ids]
    return schedule_details


def create_base_schedules(destination_conn, schedules, clone_name_prefix=None):
    responses = []
    for schedule in schedules:
        response = destination_conn.create_schedule(schedule_name=clone_name_prefix + schedule['name'],
                                                    schedule_priority=schedule['priority'],
                                                    schedule_type=schedule['type'],
                                                    schedule_execution_order=schedule['executionOrder'],
                                                    schedule_frequency=schedule['frequency'],
                                                    start_time=schedule['frequencyDetails']['start'],
                                                    end_time=schedule['frequencyDetails'].get('end', None),
                                                    interval_expression_list=
                                                    schedule['frequencyDetails']['intervals']['interval'])
        responses.append(response.json())
    return responses


def update_schedule_state(destination_conn, original_schedules, destination_schedules):
    responses = []
    for i, schedule in enumerate(destination_schedules):
        response = destination_conn.update_schedule(schedule_id=destination_schedules[i]['id'],
                                                    schedule_state=original_schedules[i]['state'])
        responses.append(response.json())
    return responses


def clone_schedules(origin_conn, destination_conn, schedule_names=None, clone_name_prefix=None):
    origin_schedule_ids = get_schedule_ids(origin_conn, schedule_names)
    origin_schedule_details = get_schedule_details(origin_conn, origin_schedule_ids)
    base_schedules = create_base_schedules(destination_conn, origin_schedule_details, clone_name_prefix)
    base_schedule_names = [schedule['schedule']['name'] for schedule in base_schedules]

    destination_schedule_ids = get_schedule_ids(destination_conn, base_schedule_names)
    destination_schedule_details = get_schedule_details(destination_conn, destination_schedule_ids)
    cloned_schedules = update_schedule_state(destination_conn, origin_schedule_details, destination_schedule_details)
    return cloned_schedules
