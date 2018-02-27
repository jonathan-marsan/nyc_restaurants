"""
General utility functions
"""

import os
import csv

import pandas as pd
import boto3


def get_count(client, dataset):
    """
    Get number of rows
    """
    try:
        count = client.get(dataset, select='count(*)')
    except:
        print('Request errored out')
    return count[0]['count']


def list_to_csv(items, path):
    """
    Write items in list to csv
    """
    with open(path, "w") as output:
        writer = csv.writer(output, delimiter=",")
        for item in items:
            writer.writerow(item)


def create_csv_filepath(filepath, filename):
    """
    Create a CSV filepath
    """
    return filepath + filename + '.csv'


def get_unique_entries_from_csv(csv_filepath, col):
    """
    Ouput unique entries from a table column stored in a csv
    """
    data_frame = pd.read_csv(csv_filepath, sep=',')
    return data_frame[col].unique()


def unique_elements(items):
    """
    Output unique elements of a list
    """
    return list(set(items))


def get_list_delta(original_list, list_to_cross_reference):
    """
    Provide two lists.
    Output dictionary that lists entries from original list
    not in cross-referenced list with counts.
    """
    my_list = []
    for element in original_list:
        if element not in list_to_cross_reference:
            my_list.append(element)
    return {'orig_len': len(original_list), 'new_len': len(my_list),
            'new_list':my_list}


def zero_if_negative(number):
    """
    Replace negative numbers with zero
    """
    if number < 0:
        return 0
    return number


def return_files_by_suffix(folder, suffix):
    """
    List files in a given folder with a particular suffix
    """
    files = os.listdir(folder)
    return [folder+file for file in files if file.endswith(suffix)]


def list_s3_files(bucket_name, path):
    """
    List files in s3 bucket
    """
    resource = boto3.resource('s3')
    my_bucket = resource.Bucket(bucket_name)
    return list(my_bucket.objects.filter(Prefix=path))
