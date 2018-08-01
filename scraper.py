""" """
import logging
from time import time
from functools import partial, wraps
from collections import Counter

import pandas as pd
import oci

# The lifecycle_state parameter only allows one at a time (per fetch), python-sdk limitation.
LC_STATE={
        'INSTANCE': 'RUNNING',
        'VOLUME': 'AVAILABLE',
        'VCN': 'AVAILABLE',
        'LOAD_BALANCER': 'ACTIVE',
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


def create_instance_dataframe(scrape_regional_results):
    """ """
    df = pd.DataFrame({}, columns=['Compartment', 'AD', 'Name', 'Shape', 
                                   'Defined tags', 'Freeform tags', 'Time created'])

    for compartment_results in scrape_regional_results:
        (compartment, instances) = compartment_results
        if instances:
            df_tmp = pd.DataFrame({'Compartment': compartment, 
                                   'AD': [x.availability_domain for x in instances],
                                   'Name': [x.display_name for x in instances],
                                   'Shape': [x.shape for x in instances],
                                   'Defined tags': [x.defined_tags for x in instances],
                                   'Freeform tags': [x.freeform_tags for x in instances],
                                   'Time created': [x.time_created for x in instances],
                                   })
            df = pd.concat([df, df_tmp], ignore_index=True, sort=False)

    return df


def create_volume_dataframe(scrape_regional_results):
    """ """
    df = pd.DataFrame({}, columns=['Compartment', 'AD', 'Name', 
                                    'Size GB', 'Size MB', 'Defined tags', 
                                    'Freeform tags', 'Time created'])

    for compartment_results in scrape_regional_results:
        (compartment, volumes) = compartment_results
        if volumes:
            df_tmp = pd.DataFrame({'Compartment': compartment, 
                                   'AD': [x.availability_domain for x in volumes],
                                   'Name': [x.display_name for x in volumes],
                                   'Size MB': [x.size_in_mbs for x in volumes],
                                   'Size GB': [x.size_in_gbs for x in volumes],
                                   'Defined tags': [x.defined_tags for x in volumes],
                                   'Freeform tags': [x.freeform_tags for x in volumes],
                                   'Time created': [x.time_created for x in volumes],
                                   })
            df = pd.concat([df, df_tmp], ignore_index=True, sort=False)

    return df


def create_vcn_dataframe(scrape_regional_results):
    """ """
    df = pd.DataFrame({}, columns=['Compartment', 'CIDR block', 'Name', 
                                    'DNS label', 'VCN domain name', 'Defined tags', 
                                    'Freeform tags', 'Time created'])
                                            
    for compartment_results in scrape_regional_results:
        (compartment, networks) = compartment_results
        if networks:
            df_tmp = pd.DataFrame({'Compartment': compartment, 
                                   'CIDR block': [x.cidr_block for x in networks],
                                   'Name': [x.display_name for x in networks],
                                   'DNS label': [x.dns_label for x in networks],
                                   'VCN domain name': [x.vcn_domain_name for x in networks],
                                   'Defined tags': [x.defined_tags for x in networks],
                                   'Freeform tags': [x.freeform_tags for x in networks],
                                   'Time created': [x.time_created for x in networks],
                                   })
            df = pd.concat([df, df_tmp], ignore_index=True, sort=False)

    return df


def create_loadbalancer_dataframe(scrape_f):
    """ """
    df = pd.DataFrame({}, columns=['Compartment', 'Name', 'Shape name', 'Time created'])
                                            
    for results in scrape_f:
        (compartment, loadbalancers) = results
        if loadbalancers:
            df_tmp = pd.DataFrame({'Compartment': compartment, 
                                   'Name': [x.display_name for x in loadbalancers],
                                   'Shape name': [x.shape_name for x in loadbalancers],
                                   'Time created': [x.time_created for x in loadbalancers],
                                   })
            df = pd.concat([df, df_tmp], ignore_index=True, sort=False)

    return df


@timer
def scrape_service_elements(compartments, list_function, lc_state):
    """ This function scrapes different service elements, from each service, 
        Compute, Storage, Network, etc. The elements are the constituents of
        the service such as instances, volumes, vcns, load balancers, etc.

        It receives a list of compartments, a list function which already
        brings the oci object client (e.g. ComputeClient) and the lifecycle state.

        It will yield for each compartment the compartment name and list of elements.
    """
    for c_name, c_id in compartments.items():
        elements_raw = list_function(c_id, lifecycle_state=lc_state)
        elements = elements_raw.data
        while elements_raw.has_next_page:
            elements_raw = list_function(c_id, lifecycle_state=lc_state,
                                                  page=elements_raw.next_page)
            elements.extend(elements_raw.data)
        yield (c_name, elements)


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

    # We generalize in order to have one function to process all elements 
    # and not one per each. Shorter and easier to make changes in the future.
    scrape_elements = partial(scrape_service_elements, compartments)

    instances = scrape_elements(oci.core.ComputeClient(config).list_instances, LC_STATE['INSTANCE'])
    volumes = scrape_elements(oci.core.BlockstorageClient(config).list_volumes, LC_STATE['VOLUME'])
    vcns = scrape_elements(oci.core.VirtualNetworkClient(config).list_vcns, LC_STATE['VCN'])
    loadbalancers = scrape_elements(oci.load_balancer.LoadBalancerClient(config).list_load_balancers, 
                                    LC_STATE['LOAD_BALANCER'])
    
    # We generate a dataframe per SubService, e.g. Compute->instance, Storage->volume
    # Network->vcn, Loadbalancer->loadbalancer.
    create_instance_dataframe(instances).to_csv(f'output/instance_{region.region_name}.csv')
    create_volume_dataframe(volumes).to_csv(f'output/volume_{region.region_name}.csv')
    create_vcn_dataframe(vcns).to_csv(f'output/vcn_{region.region_name}.csv')
    create_loadbalancer_dataframe(loadbalancers).to_csv(f'output/loadbalancer_{region.region_name}.csv')


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
