"""
The functions below enable you to clone the schedules from Server A to Server B.
This is particularly useful when doing site exports / imports.
The only functions are for use outside of this module:

clone_schedules
copy_schedule_state
override_schedule_state
"""
from tableau_api_lib.utils import extract_pages
from tableau_api_lib.exceptions import InvalidParameterException


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
        responses.append(response.json()['schedule'])
    return responses


def update_schedule_state(destination_conn, original_schedules, destination_schedules):
    responses = []
    for i, schedule in enumerate(destination_schedules):
        response = destination_conn.update_schedule(schedule_id=destination_schedules[i]['id'],
                                                    schedule_state=original_schedules[i]['state'])
        responses.append(response.json()['schedule'])
    return responses


def is_valid_state(new_state):
    new_state = new_state.lower().capitalize() if new_state else None
    if new_state in ['Suspended', 'Active', None]:
        return True
    else:
        return False


def override_schedule_state(conn, new_state, schedule_names=None):

    new_state = new_state.lower().capitalize() if new_state else None
    if not is_valid_state(new_state):
        raise InvalidParameterException('override_schedule_state()', new_state)

    if new_state:
        responses = []
        schedule_ids = get_schedule_ids(conn, schedule_names)
        for schedule_id in schedule_ids:
            response = conn.update_schedule(schedule_id=schedule_id, schedule_state=new_state)
            responses.append(response.json()['schedule'])
        return responses
    else:
        pass


def copy_schedule_state(origin_conn,
                        destination_conn,
                        origin_schedule_names=None,
                        destination_schedule_names=None):

    if origin_schedule_names and destination_schedule_names:
        if len(origin_schedule_names) != len(destination_schedule_names):
            print("When specifying schedule names, \
            you must provide an equal number of names for both the origin and destination.")
            raise InvalidParameterException('copy_schedule_state()',
                                            (origin_schedule_names, destination_schedule_names))
    elif origin_schedule_names or destination_schedule_names:
        raise InvalidParameterException('copy_schedule_state()',
                                        (origin_schedule_names, destination_schedule_names))

    origin_schedule_ids = get_schedule_ids(origin_conn, schedule_names=origin_schedule_names)
    origin_schedule_details = get_schedule_details(origin_conn, schedule_ids=origin_schedule_ids)
    destination_schedule_ids = get_schedule_ids(destination_conn, schedule_names=destination_schedule_names)
    destination_schedule_details = get_schedule_details(destination_conn, schedule_ids=destination_schedule_ids)
    updated_schedules = update_schedule_state(destination_conn, origin_schedule_details, destination_schedule_details)
    print(updated_schedules)
    return updated_schedules


def clone_schedules(origin_conn,
                    destination_conn,
                    state_override=None,
                    schedule_names=None,
                    clone_name_prefix=None):

    state_override = state_override.lower().capitalize() if state_override else None

    if not is_valid_state(state_override):
        raise InvalidParameterException('clone_schedules()', state_override)

    origin_schedule_ids = get_schedule_ids(origin_conn, schedule_names)
    origin_schedule_details = get_schedule_details(origin_conn, origin_schedule_ids)
    base_schedules = create_base_schedules(destination_conn, origin_schedule_details, clone_name_prefix)
    base_schedule_names = [schedule['name'] for schedule in base_schedules]

    destination_schedule_ids = get_schedule_ids(destination_conn, base_schedule_names)
    destination_schedule_details = get_schedule_details(destination_conn, destination_schedule_ids)
    cloned_schedules = update_schedule_state(destination_conn, origin_schedule_details, destination_schedule_details)
    override_schedule_state(destination_conn, state_override, schedule_names=base_schedule_names)
    print(cloned_schedules)
    return cloned_schedules
