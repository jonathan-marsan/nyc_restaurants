"""
Dataset Import Job
Import business data from Yelp using a list of phone numbers
Reference: https://www.yelp.com/developers/documentation/v3/business_search_phone
"""

import io
import time

import pandas as pd
from yelpapi import YelpAPI

from modules.utilities import (create_csv_filepath, get_list_delta,
                               zero_if_negative, list_s3_files, unique_elements)
from modules.yelp_utilities import convert_to_yelp_phone_numbers


# Tech debt: investigate solutions to speed up querying (e.g. check graphQL limitations)
def _get_yelp_data_by_phone_nbr(api_key, yelp_phone_numbers, manifest_folder,
                                manifest_filename):
    """
    Query Yelp business data from list of phone numbers and write to disk
    """
    yelp_api = YelpAPI(api_key=api_key)
    yelp_busines_data = []
    manifest = []
    for phone_number in yelp_phone_numbers:
        yelp_busines_data.append(yelp_api.phone_search_query(phone=phone_number))
        manifest.append({'phone_number': phone_number, 'api_called': True})
        time.sleep(0.1) # Limit because of TOO_MANY_REQUESTS_PER_SECOND error message
    manifest_filepath = create_csv_filepath(manifest_folder, manifest_filename)
    pd.DataFrame(manifest).to_csv(manifest_filepath)
    return yelp_busines_data

# Assumption
# One phone per business
def _get_phone_numbers(bucket_name, path, phone_col):
    """
    Return phone numbers from tabular csv files in S3 bucket
    """
    s3_files = list_s3_files(bucket_name=bucket_name, path=path)
    phone_nbr_list = []
    for s3_file in s3_files:
        try:
            obj = s3_file.get()
            data_frame = pd.read_csv(io.BytesIO(obj['Body'].read()))
            phone_nbr_list.extend(data_frame[phone_col].tolist())
        except:
            pass
    return convert_to_yelp_phone_numbers(unique_elements(phone_nbr_list))


def _get_phone_nbrs_to_query(input_file, bucket_name, manifest_folder):
    """
    List phone numbers for which business data has not been retrieved yet
    """
    new_phone_nbrs = get_list_delta(_get_phone_numbers(bucket_name=bucket_name,
                                                       path=input_file,
                                                       phone_col='phone'),
                                    _get_phone_numbers(bucket_name=bucket_name,
                                                       path=manifest_folder,
                                                       phone_col='phone_number'))
    return new_phone_nbrs


def _parse_yelp_info_in_df(business_data):
    """
    Pull following fields from Yelp business dataset:
    'name', 'review_count', 'rating', 'price', 'phone'
    """
    data_list = []
    for data in business_data:
        try:
            data_list.append(dict((k, data['businesses'][0][k]) for k in ('name', 'review_count', 'rating', 'price', 'phone')))
        except:
            pass
    return pd.DataFrame(data_list)


def _write_yelp_data_to_disk(api_key, phone_numbers, output_folder,
                             manifest_folder, current_timestamp):
    """
    Pull business data from listed phone numbers, write to disk, then parse it
    """
    try:
        business_info = _get_yelp_data_by_phone_nbr(api_key=api_key,
                                                    yelp_phone_numbers=phone_numbers,
                                                    manifest_folder=manifest_folder,
                                                    manifest_filename='manifest_'+current_timestamp)
        output_filepath = create_csv_filepath(output_folder, 'output_' + current_timestamp)
        _parse_yelp_info_in_df(business_data=business_info).to_csv(output_filepath)
    except:
        pass


def import_yelp_business_data(input_file, api_key, output_folder, manifest_folder,
                              bucket_name, limit=100):
    """
    Import Yelp business data for inspected NYC restaurants
    """
    phone_numbers = _get_phone_nbrs_to_query(input_file=input_file,
                                             bucket_name=bucket_name,
                                             manifest_folder=manifest_folder)
    _write_yelp_data_to_disk(api_key=api_key, phone_numbers=phone_numbers['new_list'][:limit],
                             output_folder=output_folder,
                             manifest_folder=manifest_folder,
                             current_timestamp=str(time.time())[0:10])
    print("""
          Total restaurants in NYC Inspection dataset: {0}
          Business data already pulled: {1}
          Restaurant data imported this iteration: {2}
          Restaurants left to import from Yelp: {3}
          """.format(phone_numbers['orig_len'],
                     phone_numbers['orig_len'] - len(phone_numbers['new_list']),
                     str(min(limit, len(phone_numbers['new_list']))),
                     zero_if_negative(phone_numbers['new_len']-limit)))
