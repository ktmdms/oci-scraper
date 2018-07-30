""" """
import logging
from time import time
from functools import wraps
from collections import Counter

import pandas as pd
import oci


# The lifecycle_state parameter only allows one at a time (per fetch), python-sdk limitation.
LC_STATE={
        'COMPUTE': 'RUNNING',
        'STORAGE': 'AVAILABLE',
        'NETWORK': 'AVAILABLE',
        }


def timer(func):
    """ Decorator that calculates the time a function takes to run (only logged in DEBUG mode).
        Uses logging.debug to record the execution time, and the logger must be setup in advance.
    """
    @wraps(func)
    def wrapf(*args, **kwargs):
        before = time()
        rv = func(*args, **kwargs)
        after = time()
        logging.debug(f"[[ F({func.__name__}) || EXEC TIME = {(after - before) * 1000:.2f} msec. ]]")
        return rv
    return wrapf


@timer
def get_subscribed_regions():
    """ Queries OCI for the subscribed regions and return a list with them.
        It uses the base tenancy and the identity client to gather the list of
        subscribed regions (allowed) to my user credentials, from the returned
        object it returns the .data attribute which contains the actual payload.
    """
    config = oci.config.from_file()
    identity_client = oci.identity.IdentityClient(config)

    return identity_client.list_region_subscriptions(config['tenancy']).data


def create_compute_dataframe(config, compartments):
    """ """
    df = pd.DataFrame({}, columns=['Region', 'Compartment', 'AD', 'Name', 
                                    'Shape', 'Time created'])
    for results in scrape_compute(config, compartments):
        (compartment, instances) = results
        if instances:
            df_tmp = pd.DataFrame({'Region': config['region'], 
                                    'Compartment': compartment, 
                                    'AD': [x.availability_domain for x in instances],
                                    'Name': [x.display_name for x in instances],
                                    'Shape': [x.shape for x in instances],
                                    'Time created': [x.time_created for x in instances],
                                    })
            df = pd.concat([df, df_tmp], ignore_index=True, sort=False)
    return df

def create_storage_dataframe(config, compartments):
    """ """
    df = pd.DataFrame({}, columns=['Region', 'Compartment', 'AD', 'Name', 
                                    'Size GB', 'Size MB', 'Time created'])
    for results in scrape_storage(config, compartments):
        (compartment, volumes) = results
        if volumes:
            df_tmp = pd.DataFrame({'Region': config['region'],
                                    'Compartment': compartment, 
                                    'AD': [x.availability_domain for x in volumes],
                                    'Name': [x.display_name for x in volumes],
                                    'Size MB': [x.size_in_mbs for x in volumes],
                                    'Size GB': [x.size_in_gbs for x in volumes],
                                    'Time created': [x.time_created for x in volumes],
                                    })
            df = pd.concat([df, df_tmp], ignore_index=True, sort=False)
    return df

def create_network_dataframe(config, compartments):
    """ """
    df = pd.DataFrame({}, columns=['Region', 'Compartment', 'CIDR block', 'Name', 
                                    'DNS label', 'VCN domain name', 'Time created'])
                                            
    for results in scrape_network(config, compartments):
        (compartment, networks) = results
        if networks:
            df_tmp = pd.DataFrame({'Region': config['region'], 
                                    'Compartment': compartment, 
                                    'CIDR block': [x.cidr_block for x in networks],
                                    'Name': [x.display_name for x in networks],
                                    'DNS label': [x.dns_label for x in networks],
                                    'VCN domain name': [x.vcn_domain_name for x in networks],
                                    'Time created': [x.time_created for x in networks],
                                    })
            df = pd.concat([df, df_tmp], ignore_index=True, sort=False)
    return df


@timer
def scrape_compute(config, compartments):
    """ """
    compute = oci.core.ComputeClient(config)
    for c_name, c_id in compartments.items():
        instances_raw = compute.list_instances(c_id, lifecycle_state=LC_STATE['COMPUTE'])
        instances = instances_raw.data
        while instances_raw.has_next_page:
            instances_raw = compute.list_instances(c_id, lifecycle_state=LC_STATE['COMPUTE'],
                                                  page=instances_raw.next_page)
            instances.extend(instances_raw.data)
        yield (c_name, instances)


@timer
def scrape_storage(config, compartments):
    """ """
    volume = oci.core.BlockstorageClient(config)
    for c_name, c_id in compartments.items():
        block_volumes = volume.list_volumes(c_id, lifecycle_state=LC_STATE['STORAGE']).data
        yield (c_name, block_volumes)


@timer
def scrape_network(config, compartments):
    """ """
    network = oci.core.VirtualNetworkClient(config)
    for c_name, c_id in compartments.items():
        vcns = network.list_vcns(c_id, lifecycle_state=LC_STATE['NETWORK']).data
        yield (c_name, vcns)


@timer
def scrape_a_region(region):
    """ """
    config = oci.config.from_file(profile_name=region.region_key)
    ic = oci.identity.IdentityClient(config)
    compartments = {
                    c.name.lower(): c.id 
                    for c in ic.list_compartments(config['tenancy']).data
                    }

    if not compartments:
        raise ValueError('could not find any compartments.')

    # We generate a dataframe per Service, Compute, Storage and Network.
    create_compute_dataframe(config, compartments).to_csv(f'compute_{region.region_name}.csv')
    create_storage_dataframe(config, compartments).to_csv(f'storage_{region.region_name}.csv')
    create_network_dataframe(config, compartments).to_csv(f'network_{region.region_name}.csv')


@timer
def main() :
    """ """
    subscribed_regions = get_subscribed_regions()

    if not subscribed_regions:
        ValueError('No subscribed regions found')

    try:
        logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', \
                            filename="scrapper.log", level=logging.DEBUG)
    except:
        raise

    for region in subscribed_regions:
        print(f'Scrapping {region}')
        scrape_a_region(region)


if __name__ == '__main__':
    try:
        main()
    except ValueError as e:
        print(f'We are having issues due to {e}, cannot continue.')
    except KeyboardInterrupt:
        print("User terminated")
