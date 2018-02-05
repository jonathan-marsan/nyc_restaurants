import os
import time

import pandas as pd

from yelpapi import YelpAPI

API_KEY = os.environ['YELP_API_KEY']
API_CALL_LIMIT = int(os.environ['API_CALL_LIMIT'])
nyc_inspect_csv = 'output/nyc_inspection_data.csv'
yelp_output_folder = 'output/yelp_business_data/'
yelp_manifest_folder = 'output/yelp_business_data/manifest/'


def create_csv_filepath(folder, filename):
    return folder + filename + '.csv'

def _get_phone_numbers(df_filepath, phone_col):
    df = pd.read_csv(df_filepath, sep=',')
    return df[phone_col].unique()

def _convert_to_yelp_phone_numbers(phone_numbers):
    list = []
    for phone_number in phone_numbers:
        list.append('1' + str(phone_number))
    return list

# Tech debt: investigate solutions to speed up querying (e.g. check graphQL limitations)
def _get_yelp_data_by_phone_nbr(api_key, yelp_phone_numbers, manifest_folder,
                                manifest_filename):
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


# Need to create a manifest and write whether queried or not, and query time
def _parse_yelp_info_in_df(business_data):
    data_list = []
    for data in business_data:
        try:
            data_list.append(dict((k, data['businesses'][0][k]) for k in ('name', 'review_count', 'rating', 'price', 'phone')))
        except:
            pass #For those that errorer out, also write to disk so we don't query again
    return pd.DataFrame(data_list)

def _return_files_by_suffix(folder, suffix):
    files = os.listdir(folder)
    return [folder+file for file in files if file.endswith(suffix)]

# Should be reading from s3 bucket
def _get_already_imported_phone_numbers(folder):
    list = []
    for file in _return_files_by_suffix(folder=folder,suffix='.csv'):
        list.append(pd.read_csv(file))
    try:
        return pd.concat(list)['phone_number'].tolist()
    except:
        return []

def _get_list_delta(original_list, list_to_cross_reference):
    list = []
    for element in original_list:
        if element not in list_to_cross_reference:
            list.append(element)
    return list

# Tech debt: only make api call for phone numbers that have not been requested yet
def write_yelp_data_to_disk(input, api_key, output_folder, manifest_folder,
                               limit=100):
    timestamp = str(time.time())[0:10]
    p = _get_phone_numbers(df_filepath=input, phone_col='phone')
    phone_numbers = _get_list_delta(_convert_to_yelp_phone_numbers(p),
                                    str(_get_already_imported_phone_numbers(yelp_manifest_folder)))[0:limit]
    business_info = _get_yelp_data_by_phone_nbr(api_key=api_key,
                                                yelp_phone_numbers=phone_numbers,
                                                manifest_folder=manifest_folder,
                                                manifest_filename='manifest_'+timestamp)
    output_filepath = create_csv_filepath(output_folder, 'output_' + timestamp)
    _parse_yelp_info_in_df(business_data=business_info).to_csv(output_filepath)
    print('Done')

if __name__ == "__main__":
    write_yelp_data_to_disk(input=nyc_inspect_csv, api_key=API_KEY,
                               output_folder=yelp_output_folder,
                               manifest_folder=yelp_manifest_folder, limit=2000)
