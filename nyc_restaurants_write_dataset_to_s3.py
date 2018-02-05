
import os

import boto3

nyc_inspect_disk_filepath = 'output/nyc_inspection_data.csv'
nyc_inspect_bucket_name = 'nyc-restaurants-20180203'
nyc_inspect_bucket_filepath = 'nyc_inspection_data.csv'


def upload_file_to_s3(filepath, bucket_name, bucket_filepath):
    try:
        s3 = boto3.resource('s3')
        data = open(filepath, 'rb')
        s3.Bucket(bucket_name).put_object(Key=bucket_filepath, Body=data)
        print('Uploaded file')
    except:
        print('Errored out')

if __name__ == "__main__":
    upload_file_to_s3(filepath=nyc_inspect_disk_filepath,
                      bucket_name=nyc_inspect_bucket_name,
                      bucket_filepath=nyc_inspect_bucket_filepath)
