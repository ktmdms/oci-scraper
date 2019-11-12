""" """
import logging

from time import time
from functools import partial, wraps

import pandas as pd

from oci.config import from_file
from oci.core import ComputeClient as ccli
from oci.core import BlockstorageClient as bscli
from oci.core import VirtualNetworkClient as netcli
from oci.identity import IdentityClient as icli
from oci.load_balancer import LoadBalancerClient as lbcli

# The lifecycle_state parameter only allows one at a time (per fetch).
LC_STATE = {
        'INSTANCE': 'RUNNING',
        'VOLUME': 'AVAILABLE',
        'VCN': 'AVAILABLE',
        'LOAD_BALANCER': 'ACTIVE',
        }

logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename="scraper.log", level=logging.DEBUG)


def timer(func):
    """ Decorator that calculates the time a function takes to run (only logged
        in DEBUG mode).
        Uses logging.info to record the execution time, and the logger must be
        setup in advance.
    """
    @wraps(func)
    def wrapf(*args, **kwargs):
        before = time()
        rv = func(*args, **kwargs)
        after = time()
        etime = (after - before) * 1000
        logging.info(f'[F({func.__name__}) || EXEC TIME = {etime:.2f} msec]')
        return rv
    return wrapf


def create_df(subservice, columns):
    """ This decorator takes the results of the scraper and creates a dataframe
        based on them. It walks the results and generates a list of elements
        which are the results of processing the results, then it calls the
        original function with the new results.
    """
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            df = pd.DataFrame({}, columns=columns)
            for compartment_results in args[0]:
                (compartment, elements) = compartment_results
                logging.info(f'Creating {subservice} dframe for {compartment}')
                if elements:
                    df = pd.concat([df, func(elements, compartment)],
                                   ignore_index=True, sort=False)
            return df
        return wrapper
    return decorate


@timer
def get_subscribed_regions():
    """ Queries OCI for the subscribed regions and return a list with them.
        It uses the base tenancy and the identity client to gather the list of
        subscribed regions (allowed) to my user credentials, from the returned
        object it returns the .data attribute which contains the payload.
    """
    cfg = from_file()
    return icli(cfg).list_region_subscriptions(cfg['tenancy']).data


@create_df('LB', ['Compartment', 'AD', 'Name', 'Shape',
                  'Defined tags', 'Freeform tags', 'Time created'])
def get_instance_df(instances, compartment=None):
    return pd.DataFrame({'Compartment': compartment,
                         'AD': [x.availability_domain for x in instances],
                         'Name': [x.display_name for x in instances],
                         'Shape': [x.shape for x in instances],
                         'Defined tags': [x.defined_tags for x in instances],
                         'Freeform tags': [x.freeform_tags for x in instances],
                         'Time created': [x.time_created for x in instances],
                         })


@create_df('Block Volume', ['Compartment', 'AD', 'Name', 'Size GB', 'Size MB',
                            'Defined tags', 'Freeform tags', 'Time created'])
def get_bv_df(volumes, compartment=None):
    return pd.DataFrame({'Compartment': compartment,
                         'AD': [x.availability_domain for x in volumes],
                         'Name': [x.display_name for x in volumes],
                         'Size MB': [x.size_in_mbs for x in volumes],
                         'Size GB': [x.size_in_gbs for x in volumes],
                         'Defined tags': [x.defined_tags for x in volumes],
                         'Freeform tags': [x.freeform_tags for x in volumes],
                         'Time created': [x.time_created for x in volumes],
                         })


@create_df('VCN', ['Compartment', 'CIDR block', 'Name', 'DNS label',
                   'VCN domain name', 'Defined tags', 'Freeform tags',
                   'Time created'])
def get_vcn_df(vcns, compartment=None):
    return pd.DataFrame({'Compartment': compartment,
                         'CIDR block': [x.cidr_block for x in vcns],
                         'Name': [x.display_name for x in vcns],
                         'DNS label': [x.dns_label for x in vcns],
                         'VCN domain name': [x.vcn_domain_name for x in vcns],
                         'Defined tags': [x.defined_tags for x in vcns],
                         'Freeform tags': [x.freeform_tags for x in vcns],
                         'Time created': [x.time_created for x in vcns],
                         })


@create_df('Load Balancer', ['Compartment', 'Name', 'Shape name',
                             'Time created'])
def get_lb_df(loadbalancers, compartment=None):
    return pd.DataFrame({'Compartment': compartment,
                         'Name': [x.display_name for x in loadbalancers],
                         'Shape name': [x.shape_name for x in loadbalancers],
                         'Time created': [x.time_created for x in
                                          loadbalancers],
                         })


@timer
def scrape_service_elements(compartments, list_function, lc_state):
    """ This function scrapes different service elements, from each service,
        Compute, Storage, Network, etc. The elements are the constituents of
        the service such as instances, volumes, vcns, load balancers, etc.

        It receives a list of compartments, a list function which already
        brings the oci object client (ComputeClient) and the lifecycle state.

        It will yield for each compartment the name and list of elements.
    """
    #Must read the items in id->name since that is how were are colating the compartment info.
    for c_id, c_name in compartments.items():
        logging.info(f'Scraping elements for {c_name}')
        elements_raw = list_function(c_id, lifecycle_state=lc_state)
        elements = elements_raw.data
        while elements_raw.has_next_page:
            logging.info('Processing another page')
            elements_raw = list_function(c_id, lifecycle_state=lc_state,
                                         page=elements_raw.next_page)
            elements.extend(elements_raw.data)
        yield (c_name, elements)


@timer
def scrape_a_region(region):
    """ """
    cfg = from_file(profile_name=region.region_key)
    logging.info(f'Scraping region {region}')

    #In order to accomodate child compartments *and* compartments that my be named the same in different regions, compartment
    #Id comes before compartment Name in compartments and compartment_id_in_subtree is defined in the icli call.
    compartments = {c.id: c.name.lower()
                    for c in icli(cfg).list_compartments(cfg['tenancy'], compartment_id_in_subtree=True).data
                    }

    if not compartments:
        raise LookupError('could not find any compartments.')

    # We generalize in order to have one function to process all elements
    # and not one per each. Shorter and easier to make changes in the future.
    scrape = partial(scrape_service_elements, compartments)

    instances = scrape(ccli(cfg).list_instances, LC_STATE['INSTANCE'])
    volumes = scrape(bscli(cfg).list_volumes, LC_STATE['VOLUME'])
    vcns = scrape(netcli(cfg).list_vcns, LC_STATE['VCN'])
    lbs = scrape(lbcli(cfg).list_load_balancers, LC_STATE['LOAD_BALANCER'])

    # We generate a dataframe per SubService, e.g. Compute->instance,
    # Storage->volume, Network->vcn, Loadbalancer->loadbalancer.
    get_instance_df(instances).to_csv(f'output/instance_{region.region_name}.csv')
    get_bv_df(volumes).to_csv(f'output/volume_{region.region_name}.csv')
    get_vcn_df(vcns).to_csv(f'output/vcn_{region.region_name}.csv')
    get_lb_df(lbs).to_csv(f'output/loadbalancer_{region.region_name}.csv')


@timer
def main():
    """ """
    subscribed_regions = get_subscribed_regions()

    if not subscribed_regions:
        ValueError('No subscribed regions found')

    for region in subscribed_regions:
        scrape_a_region(region)


if __name__ == '__main__':
    try:
        main()
    except ValueError as e:
        print(f'We are having issues due to {e}, cannot continue.')
    except KeyboardInterrupt:
        print("User terminated")
