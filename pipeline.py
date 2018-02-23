import os

import pandas as pd
import luigi
from luigi.contrib.simulate import RunAnywayTarget
from yelpapi import YelpAPI

from pipeline_data.import_nyc_restaurant_inspection_data import write_nyc_inspection_to_csv
from pipeline_data.import_yelp_business_data import import_yelp_business_data
from pipeline_data.upload_files_to_s3 import upload_files_in_dir_to_s3


# Constants
nyc_app_token = os.environ['NYC_APP_TOKEN']
nyc_rest_inspections_filename = 'output/nyc_restaurant_inspection_data.csv'
bucket_name = 'nyc-restaurants-20180203'
directory_with_files = 'output'
yelp_output_folder = 'output/yelp_business_data/'
yelp_manifest_folder = 'output/yelp_business_data/manifest/'
yelp_api_key = os.environ['YELP_API_KEY']


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
        upload_files_in_dir_to_s3(directory=directory_with_files,
                                  bucket_name=bucket_name)


class yelp_business_data_to_s3(luigi.Task):

    cycles = luigi.IntParameter(default=1)
    limit = luigi.IntParameter(default=100)

    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        for i in range(0,self.cycles):
            import_yelp_business_data(input_file=nyc_rest_inspections_filename,
                                      api_key=yelp_api_key,
                                      output_folder=yelp_output_folder,
                                      manifest_folder=yelp_manifest_folder,
                                      bucket_name=bucket_name,
                                      limit=self.limit)
            upload_files_in_dir_to_s3(directory=directory_with_files,
                                      bucket_name=bucket_name)
        return self.output().done()


if __name__ == '__main__':
    luigi.run()
