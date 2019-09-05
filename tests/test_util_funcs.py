from tableau_api_lib import TableauServerConnection
from .config import tableau_server_config

from tableau_api_lib.utils import extract_pages
from tableau_api_lib.utils import clone_schedules, copy_schedule_state


TEST_SCHEDULE_NAMES = ['estam_test extract schedule', 'estam_test hourly end']
TEST_CLONE_NAME_PREFIX = 'estam_auto_'


conn = TableauServerConnection(tableau_server_config)


def test_sign_in():
    conn.sign_in()


def test_extract_pages_default():
    test_query = conn.query_sites().json()
    total_sites_available = int(test_query['pagination']['totalAvailable'])
    extracted_sites = extract_pages(conn.query_sites)
    print("\nNumber of extracted sites: {}\nNumber of available sites: {}".format(len(extracted_sites),
                                                                                  total_sites_available))
    assert len(extracted_sites) == total_sites_available


def test_extract_pages_limit():
    results_limit = 75
    test_query = conn.query_sites().json()
    total_sites_available = int(test_query['pagination']['totalAvailable'])
    extracted_sites = extract_pages(conn.query_sites, limit=results_limit)
    print("\nNumber of extracted sites: {}\nNumber of available sites: {}".format(len(extracted_sites),
                                                                                  total_sites_available))
    assert len(extracted_sites) == results_limit


def test_clone_schedules():
    cloned_schedules = clone_schedules(origin_conn=conn,
                                       destination_conn=conn,
                                       state_override='suspended',
                                       schedule_names=TEST_SCHEDULE_NAMES,
                                       clone_name_prefix=TEST_CLONE_NAME_PREFIX)
    cloned_schedule_names = [schedule['name'] for schedule in cloned_schedules]
    print("cloned schedule names: ", cloned_schedule_names)
    expected_names = [TEST_CLONE_NAME_PREFIX + name for name in TEST_SCHEDULE_NAMES]
    print("expected names: ", expected_names)
    assert cloned_schedule_names == expected_names


def test_copy_schedule_state():
    updated_schedules = copy_schedule_state(conn,
                                            conn,
                                            ['estam_test extract schedule',
                                             'estam_test hourly end'],
                                            ['estam_auto_estam_test extract schedule',
                                             'estam_auto_estam_test hourly end'])
    assert len(updated_schedules) > 0


def test_delete_cloned_schedules():
    expected_names = [TEST_CLONE_NAME_PREFIX + name for name in TEST_SCHEDULE_NAMES]
    print("expected names: ", expected_names)
    cloned_schedule_ids = [schedule['id'] for schedule in extract_pages(conn.query_schedules)
                           if schedule['name'] in expected_names]
    for schedule_id in cloned_schedule_ids:
        response = conn.delete_schedule(schedule_id=schedule_id)
        print("delete response code: ", response.status_code)
    clones_remaining = [schedule['name'] for schedule in extract_pages(conn.query_schedules)
                        if schedule['name'] in expected_names]
    print("clones remaining (should be none): ", clones_remaining)
    assert not any(clones_remaining)


def test_sign_out():
    conn.sign_out()
