"""
The functions below enable you to clone the schedules from Server A to Server B.
This is particularly useful when doing site exports / imports.
The only functions are for use outside of this module:

clone_schedules
clone_schedule_state
override_schedule_state
"""
import pandas as pd
import warnings

from tableau_api_lib.utils.querying import get_schedules_dataframe
from tableau_api_lib.decorators import validate_schedule_state_override
from tableau_api_lib.exceptions import InvalidParameterException


def get_schedule_details(conn, schedule_names=None):
    schedules_df = get_schedules_dataframe(conn)
    if schedule_names:
        schedules_df = schedules_df.loc[schedules_df['name'].isin(schedule_names)]
    schedule_ids = list(schedules_df['id'])
    schedule_details = [conn.update_schedule(schedule_id).json()['schedule'] for schedule_id in schedule_ids]
    return pd.DataFrame(schedule_details)


def create_base_schedules(conn,
                          schedules_df,
                          prefix=None,
                          suffix=None):
    prefix = prefix or ''
    suffix = suffix or ''
    responses = []
    for index, schedule in schedules_df.iterrows():
        response = conn.create_schedule(schedule_name=prefix + schedule['name'] + suffix,
                                        schedule_priority=schedule['priority'],
                                        schedule_type=schedule['type'],
                                        schedule_execution_order=schedule['executionOrder'],
                                        schedule_frequency=schedule['frequency'],
                                        start_time=schedule['frequencyDetails']['start'],
                                        end_time=schedule['frequencyDetails'].get('end', None),
                                        interval_expression_list=schedule['frequencyDetails']['intervals']['interval']
                                        if schedule['frequencyDetails'].get('intervals') else None)
        try:
            responses.append(response.json()['schedule'])
        except KeyError:
            warnings.warn(f"""
            There is already a schedule named '{prefix + schedule['name'] + suffix}'. 
            Duplicates .
            """)
    return responses


def update_schedule_state(conn,
                          source_schedules_df,
                          destination_schedules_df):
    responses = []
    for index, schedule in destination_schedules_df.iterrows():
        response = conn.update_schedule(schedule_id=schedule['id'],
                                        schedule_state=source_schedules_df.iloc[index]['state'])
        try:
            responses.append(response.json()['schedule'])
        except KeyError:
            warnings.warn(f"Unable to update schedule '{schedule['name']}'")
    return pd.DataFrame(responses)


@validate_schedule_state_override
def override_schedule_state(conn, state_override_value=None, schedule_names=None):
    """
    Replace the existing schedule state (Active or Suspended) with the specified state override value.
    :param TableauServerConnection conn: the Tableau Server connection
    :param str state_override_value: specify a state override value ('Active' or 'Suspended')
    :param list schedule_names: specify schedule names if overriding a subset of existing schedules
    :return: pd.DataFrame
    """
    if state_override_value:
        responses = []
        schedules_df = get_schedule_details(conn, schedule_names)
        for index, schedule in schedules_df.iterrows():
            response = conn.update_schedule(schedule_id=schedule['id'], schedule_state=state_override_value)
            try:
                responses.append(response.json()['schedule'])
            except KeyError:
                warnings.warn(f"Unable to update schedule '{schedule['name']}'")
        return pd.DataFrame(responses)
    else:
        return pd.DataFrame()


def verify_schedule_names_align(source_schedule_names, destination_schedule_names, func_name=None):
    if source_schedule_names and destination_schedule_names:
        if len(source_schedule_names) != len(destination_schedule_names):
            print("When specifying schedule names, \
            you must provide an equal number of names for both the origin and destination.")
            raise InvalidParameterException(func_name,
                                            (source_schedule_names, destination_schedule_names))
    elif source_schedule_names or destination_schedule_names:
        raise InvalidParameterException(func_name,
                                        (source_schedule_names, destination_schedule_names))


def clone_schedule_state(conn_source,
                         conn_destination,
                         source_schedule_names=None,
                         destination_schedule_names=None) -> pd.DataFrame:
    """
    Clone schedule states from Tableau Server environment 'conn_source' to 'conn_destination'.
    :param TableauServerConnection conn_source: the source Tableau Server connection
    :param TableauServerConnection conn_destination: the destination Tableau Server connection
    :param list source_schedule_names: (optional) define a subset of source schedule names
    :param list destination_schedule_names: (optional) define a subset of destination schedule names
    :return: pd.DataFrame
    """
    verify_schedule_names_align(source_schedule_names, destination_schedule_names, func_name='clone_schedule_state()')
    source_schedule_df = get_schedule_details(conn_source, schedule_names=source_schedule_names)
    destination_schedule_df = get_schedule_details(conn_destination, schedule_names=destination_schedule_names)
    updated_schedules = update_schedule_state(conn_destination, source_schedule_df, destination_schedule_df)
    return updated_schedules


@validate_schedule_state_override
def clone_schedules(conn_source,
                    conn_destination,
                    state_override_value=None,
                    schedule_names=None,
                    prefix=None,
                    suffix=None) -> pd.DataFrame:
    """
    Clone schedules from Tableau Server environment 'conn_source' to 'conn_destination'.
    :param TableauServerConnection conn_source: the source Tableau Server connection
    :param TableauServerConnection conn_destination: the destination Tableau Server connection
    :param str state_override_value: (optional) define a state value to override schedule states from the source
    :param list schedule_names: (optional) define a subset of schedule names to clone
    :param str prefix: (optional) define a prefix to add to the cloned schedule names
    :param str suffix: (optional) define a suffix to add to the cloned schedule names
    :return: pd.DataFrame
    """
    source_schedule_df = get_schedule_details(conn=conn_source, schedule_names=schedule_names)
    base_schedules_responses = create_base_schedules(conn_destination, source_schedule_df, prefix, suffix)
    base_schedule_names = [schedule['name'] for schedule in base_schedules_responses]

    destination_schedule_df = get_schedule_details(conn=conn_destination, schedule_names=base_schedule_names)
    cloned_schedules = update_schedule_state(conn_destination, source_schedule_df, destination_schedule_df)
    if state_override_value:
        cloned_schedules = override_schedule_state(conn_destination, state_override_value, base_schedule_names)
    return cloned_schedules
