"""
The functions below enable you to clone the sites from Server A to Server B.
This is particularly useful when doing site exports / imports.
Functions to import: clone_sites
"""


import pandas as pd
import copy

from tableau_api_lib.utils import extract_pages
from tableau_api_lib.exceptions import ContentOverwriteDisabled


"""
1) query sites on source server
2) get site names on source server
3) query sites on target server
4) get site names on target server
5) compare site names between source and target
-> overwrite_policy=None: throw exception if any site names overlap
-> overwrite_policy='overwrite': delete overlapping sites on target server
6) create sites on target server, copying the details from the source server
7) query the newly created sites on the target server
8) merge the source sites and the target sites by name, keeping only the 'state' from the source
9) update the newly created target sites to have their source site's 'state' (Active or Suspended)
    -> return a list of HTTP responses
"""


def get_source_site_df(conn_source, site_names=None) -> pd.DataFrame:
    """
    Query details for all sites on the source server, or only the sites listed in 'site_names'.
    :param class conn_source: the Tableau Server connection object
    :param list site_names: a list of the desired site names whose details will be queried
    :return: Pandas DataFrame
    """
    site_df = pd.DataFrame(extract_pages(conn_source.query_sites))
    if site_names:
        site_df = site_df[site_df['name'].isin(site_names)]
    site_df.fillna(value='-1', inplace=True)
    return site_df


def get_target_site_df(conn_target, site_names=None) -> pd.DataFrame:
    """
    Query details for all sites on the target server, or only the sites listed in 'site_names'.
    :param class conn_target: the Tableau Server connection object
    :param list site_names: a list of the desired site names whose details will be queried
    :return: Pandas DataFrame
    """
    site_df = pd.DataFrame(extract_pages(conn_target.query_sites))
    if site_names:
        site_df = site_df[site_df['name'].isin(site_names)]
    site_df.fillna(value='-1', inplace=True)
    return site_df


def merge_source_and_target_df(source_site_df, target_site_df):
    target_site_subset_df = target_site_df[['id', 'name']]
    merged_df = source_site_df.merge(target_site_subset_df, on='name', how='inner', suffixes=(None, "_target"))
    return merged_df


def get_site_names(site_df) -> list:
    """
    Get the site name for the active site on the specified Tableau Server connection.
    :param DataFrame site_df: the site details DataFrame for the server
    :return: site names
    """
    site_names = list(site_df['name'])
    site_names = [site_name for site_name in site_names if site_name.lower() != 'default']
    return site_names


def get_overlapping_site_names(source_site_df, target_site_df) -> list:
    source_site_names = set(get_site_names(source_site_df))
    target_site_names = set(get_site_names(target_site_df))
    overlapping_site_names = source_site_names.intersection(target_site_names)
    return list(overlapping_site_names)


def create_sites(source_site_df, conn_target):
    print("creating target sites...")
    responses = []
    for i, site in source_site_df.iterrows():
        responses.append(conn_target.create_site(
            site_name=site['name'],
            content_url=site['contentUrl']
        ))
    print("target sites created")
    return responses


def update_sites(conn_target, site_df):
    print("updating target sites...")
    responses = []
    original_site_content_url = conn_target.site_url
    for i, site in site_df.iterrows():
        conn_target.switch_site(content_url=site['contentUrl'])
        responses.append(conn_target.update_site(
            site_id=site['id_target'],
            site_name=site['name'],
            content_url=site['contentUrl'],
            admin_mode=site['adminMode'],
            disable_subscriptions_flag=site['disableSubscriptions'],
            state=site['state'],
            revision_history_enabled_flag=site['revisionHistoryEnabled'],
            revision_limit=site['revisionLimit'],
            subscribe_others_enabled_flag=site['subscribeOthersEnabled'],
            allow_subscription_attachments_flag=site['allowSubscriptionAttachments'],
            guest_access_enabled_flag=site['guestAccessEnabled'],
            cache_warmup_enabled_flag=site['cacheWarmupEnabled'],
            commenting_enabled_flag=site['commentingEnabled'],
            flows_enabled_flag=site['flowsEnabled'],
            extract_encryption_mode=site['extractEncryptionMode'],
            user_quota=site['userQuota']
            # derivedPermissions
            # catalogingEnabled
        ))
    conn_target.switch_site(content_url=original_site_content_url)
    print("target sites updated")
    return responses


def delete_sites(conn, site_details_df, site_names):
    print("deleting overlapping target sites...")
    responses = []
    sites_to_delete = site_details_df[site_details_df['name'].isin(site_names)]
    for i, site in sites_to_delete.iterrows():
        conn_original_state = copy.copy(conn)
        conn.switch_site(content_url=site['contentUrl'])
        responses.append(conn.delete_site(site_name=site['name']))
        conn = conn_original_state
        conn.sign_in()
    print("overlapping target sites deleted")
    return conn


def validate_inputs(overwrite_policy):
    valid_overwrite_policies = [
        None,
        'overwrite'
    ]
    if overwrite_policy in valid_overwrite_policies:
        pass
    else:
        raise ValueError("Invalid overwrite policy provided: '{}'".format(overwrite_policy))


def clone_sites(conn_source, conn_target, site_names=None, overwrite_policy=None):
    validate_inputs(overwrite_policy)
    source_site_df = get_source_site_df(conn_source=conn_source, site_names=site_names)
    target_site_df = get_target_site_df(conn_target=conn_target, site_names=site_names)
    overlapping_site_names = get_overlapping_site_names(source_site_df=source_site_df, target_site_df=target_site_df)
    if any(overlapping_site_names) and not overwrite_policy:
        raise ContentOverwriteDisabled('site')
    if any(overlapping_site_names) and overwrite_policy == 'overwrite':
        conn_target = delete_sites(conn=conn_target, site_details_df=target_site_df, site_names=overlapping_site_names)
    create_sites(source_site_df=source_site_df, conn_target=conn_target)
    target_site_df = get_target_site_df(conn_target=conn_target, site_names=site_names)
    merged_site_df = merge_source_and_target_df(source_site_df=source_site_df, target_site_df=target_site_df)
    updated_sites = update_sites(conn_target=conn_target, site_df=merged_site_df)
    return updated_sites
