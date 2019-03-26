"""S3 Data Access Object."""

# System
import os
import zipfile
import logging
import datetime
import glob

# Third Party
import boto3
import botocore
import pandas as pd

S3_BUCKET = os.environ.get("S3_UPLOAD_BUCKET", "numerai-production-uploads")
S3_DATASET_BUCKET = "numerai-datasets"
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")


class FileManager():
    def __init__(self, local_dir, log=None):
        self.local_dir = local_dir
        self.s3 = boto3.resource(
            "s3",
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY)
        self.bucket = S3_BUCKET
        self.log = log

    def __hash__(self):
        """
        We want to implement the hash function so we can use this with a lru_cache
        but we don't actually care about hashing it.
        """
        return 90210

    def download(self, files):
        """
        Takes in a list of S3 directories, and tries to download them to a local folder.
        Will skip files that it has trouble downloading.
        Returns a list of the local file locations.
        """

        if not os.path.exists(self.local_dir):
            os.makedirs(self.local_dir)

        local_files = []
        for s3_file in files:
            print(s3_file)
            full_filename = os.path.join(self.local_dir, s3_file)
            local_files.append(full_filename)

            nested_dir_name = os.path.dirname(full_filename)
            if not os.path.exists(nested_dir_name):
                os.makedirs(nested_dir_name)

            if not os.path.isfile(full_filename):
                print("Downloading {} from S3 bucket {}".format(
                    full_filename, self.bucket))
                try:
                    self.s3.meta.client.download_file(self.bucket, s3_file,
                                                      full_filename)
                except botocore.exceptions.EndpointConnectionError:

                    if self.log:
                        logging.getLogger().info(
                            "Could not download {} from S3. Skipping.".format(
                                s3_file))
                    else:
                        print("Could not download {} from S3. Skipping.".
                              format(s3_file))

        return local_files

    def read_csv(self, s3_file):
        res = self.s3.Bucket(self.bucket).Object(s3_file).get()
        return pd.read_csv(res.get('Body'))

    def download_dataset(self, tournament, round_number):
        bucket = S3_DATASET_BUCKET
        s3_path = "t{}/{}/numerai_datasets.zip".format(tournament,
                                                       round_number)
        extract_dir = "t{}/{}/numerai_datasets/".format(
            tournament, round_number)
        local_path = os.path.join(self.local_dir, s3_path)
        local_extract = os.path.join(self.local_dir, extract_dir)

        if not os.path.exists(
                os.path.join(self.local_dir, "t" + str(tournament))):
            os.makedirs(os.path.join(self.local_dir, "t" + str(tournament)))

        if not os.path.exists(
                os.path.join(self.local_dir, "t" + str(tournament),
                             str(round_number))):
            os.makedirs(
                os.path.join(self.local_dir, "t" + str(tournament),
                             str(round_number)))

        if not os.path.isfile(local_path):
            print("Attempting to get file {} from bucket {} to {}".format(
                s3_path, bucket, local_path))
            self.s3.meta.client.download_file(bucket, s3_path, local_path)

            with zipfile.ZipFile(local_path, "r") as zip_ref:
                zip_ref.extractall(local_extract)

        return local_extract

    def clean_up(self):
        """
            Delete all files in local_dir that is older than 1 week
        """
        print(f"cleaning up {self.local_dir}")
        now = datetime.datetime.now()
        min_datetime = now - datetime.timedelta(weeks=1)
        min_timestamp = min_datetime.timestamp()
        files = glob.iglob(
            os.path.join(self.local_dir, "**/*"), recursive=True)
        for f in files:
            last_modifed_timestamp = os.stat(f).st_mtime
            if last_modifed_timestamp < min_timestamp:
                print(f"deleting {f}")
                try:
                    os.remove(f)
                except Exception as e:
                    print(e)
