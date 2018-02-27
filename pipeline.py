"""
Workflow management file to run data pipeline scripts
"""

import os

import luigi
from luigi.contrib.simulate import RunAnywayTarget

from data.import_nyc_restaurant_inspection_data import write_nyc_inspection_to_csv
from data.import_yelp_business_data import import_yelp_business_data
from data.upload_files_to_s3 import upload_files_in_dir_to_s3


# Constants
NYC_APP_TOKEN = os.environ['NYC_APP_TOKEN']
NYC_REST_INSPECT_FILEPATH = 'output/nyc_restaurant_inspection_data.csv'
BUCKET_NAME = 'nyc-restaurants-20180203'
FILE_DIRECTORY = 'output'
YELP_OUTPUT_FOLDER = 'output/yelp_business_data/'
YELP_MANIFEST_FOLDER = 'output/yelp_business_data/manifest/'
YELP_API_KEY = os.environ['YELP_API_KEY']


class nyc_inspection_to_csv(luigi.Task):
    """
    Download NYC inspection dataset
    """
    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        write_nyc_inspection_to_csv(token=NYC_APP_TOKEN,
                                    output_csv=NYC_REST_INSPECT_FILEPATH)
        return self.output().done()


class nyc_inspection_to_s3(luigi.Task):
    """
    Upload NYC inspection dataset to S3
    """
    def requires(self):
        return [nyc_inspection_to_csv()]
    def output(self):
        return None
    def run(self):
        upload_files_in_dir_to_s3(directory=FILE_DIRECTORY,
                                  bucket_name=BUCKET_NAME)


class yelp_business_data_to_s3(luigi.Task):
    """
    Download Yelp business data and upload to S3
    """
    cycles = luigi.IntParameter(default=1)
    limit = luigi.IntParameter(default=100)

    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        for i in range(0, self.cycles):
            import_yelp_business_data(input_file=NYC_REST_INSPECT_FILEPATH,
                                      api_key=YELP_API_KEY,
                                      output_folder=YELP_OUTPUT_FOLDER,
                                      manifest_folder=YELP_MANIFEST_FOLDER,
                                      bucket_name=BUCKET_NAME,
                                      limit=self.limit)
            upload_files_in_dir_to_s3(directory=FILE_DIRECTORY,
                                      bucket_name=BUCKET_NAME)
        return self.output().done()

# Use as stand alone method to send files to s3
class output_to_s3(luigi.Task):
    """
    Upload any file in FILE_DIRECTORY to S3
    """
    def requires(self):
        return None
    def output(self):
        return RunAnywayTarget(self)
    def run(self):
        upload_files_in_dir_to_s3(directory=FILE_DIRECTORY,
                                  bucket_name=BUCKET_NAME)


if __name__ == '__main__':
    luigi.run()
