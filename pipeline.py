import os

import luigi
import pandas as pd

from luigi.contrib.simulate import RunAnywayTarget
from yelpapi import YelpAPI

from import_nyc_restaurant_inspection_data import write_nyc_inspection_to_csv
from import_yelp_business_data import import_yelp_business_data
from upload_files_to_s3 import upload_files_in_dir_to_s3


# Constants
nyc_app_token = os.environ['NYC_APP_TOKEN']
nyc_rest_inspections_filename = 'output/nyc_restaurant_inspection_data.csv'
bucket_name = 'nyc-restaurants-20180203'


# Tech Debt: Set up so that csv write to a folder within output
# Then only upload contents of that folder to S3
class nyc_inspection_to_csv(luigi.Task):

    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        write_nyc_inspection_to_csv(token=nyc_app_token,
                                    output_csv=nyc_rest_inspections_filename)
        return self.output().done()


class nyc_inspection_to_s3(luigi.Task):

    directory_with_files = 'output'
    bucket = bucket_name

    def requires(self):
        return [nyc_inspection_to_csv()]
    def output(self):
        return None
    def run(self):
        upload_files_in_dir_to_s3(directory=self.directory_with_files,
                                  bucket_name=self.bucket)


# Tech debt: Make it depend on previous task output csv
class yelp_business_data_to_csv(luigi.Task):

    yelp_output_folder = 'output/yelp_business_data/'
    yelp_manifest_folder = 'output/yelp_business_data/manifest/'
    api_call_limit = 10
    yelp_api_key = os.environ['YELP_API_KEY']

    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        import_yelp_business_data(input_file=nyc_rest_inspections_filename,
                                  api_key=self.yelp_api_key,
                                  output_folder=self.yelp_output_folder,
                                  manifest_folder=self.yelp_manifest_folder,
                                  bucket_name=bucket_name,
                                  limit=self.api_call_limit)
        return self.output().done()


class yelp_business_data_to_s3(luigi.Task):

    directory_with_files = 'output'
    bucket = bucket_name

    def requires(self):
        return [yelp_business_data_to_csv()]
    def output(self):
        return None
    def run(self):
        upload_files_in_dir_to_s3(directory=self.directory_with_files,
                                  bucket_name=self.bucket)


if __name__ == '__main__':
    luigi.run()
