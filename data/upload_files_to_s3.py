"""
Enable upload of all files in a given directory to s3
"""
import os

import boto3

from shutil import move


def move_file(filepath, new_directory):
    new_filepath = new_directory + filepath.rsplit('/',1)[1]
    if not os.path.exists(new_directory):
        os.makedirs(new_directory)
    os.rename(filepath, new_filepath)
    print('Moved file to: ' + new_filepath)


def upload_file_to_s3(filepath, bucket_name, backup_directory="backup/"):
    try:
        s3 = boto3.resource('s3')
        data = open(filepath, 'rb')
        s3.Bucket(bucket_name).put_object(Key=filepath, Body=data)
        print('Uploaded: ' + filepath)
    except:
        print('Error uploading: ' + filepath)
    else:
        move_file(filepath=filepath, new_directory=backup_directory)


def upload_files_in_dir_to_s3(directory, bucket_name):
    for path, subdirs, files in os.walk(directory):
        for name in files:
            if name[0] != '.':
                filename = os.path.join(path, name)
                upload_file_to_s3(filepath=filename,
                                  bucket_name=bucket_name)
                print("""
                      Uploaded: {0}
                      """.format(filename))
