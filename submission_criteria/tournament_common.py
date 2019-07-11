import os
import botocore
import pandas as pd

S3_INPUT_DATA_BUCKET = "numerai-tournament-data"


def _download_file(s3, s3_bucket, s3_filepath, local_filepath):
    try:
        s3.meta.client.download_file(s3_bucket, s3_filepath, local_filepath)
    except (botocore.exceptions.ClientError,
            botocore.exceptions.EndpointConnectionError) as e:
        raise Exception(f"Error when downloading {s3_filepath}: {str(e)}")


def get_file(s3, s3_bucket, s3_path, filename, local_path, download=True):
    # make dir if it does not exist
    local_path = os.path.join(local_path, s3_path)
    if not os.path.isdir(local_path):
        os.makedirs(local_path)
    s3_filepath = os.path.join(s3_path, filename)
    local_filepath = os.path.join(local_path, filename)
    if download and not os.path.isfile(local_filepath):
        print(
            f"Downloading file: s3://{s3_bucket}/{s3_filepath} to {local_filepath}"
        )
        _download_file(s3, s3_bucket, s3_filepath, local_filepath)
    print(f"Loading file: {local_filepath}")
    return pd.read_csv(local_filepath)


def read_csv(s3, s3_bucket, s3_file):
    res = s3.Bucket(s3_bucket).Object(s3_file).get()
    return pd.read_csv(res.get('Body'))


def get_validation_data(s3, version):
    s3_filepath = os.path.join(version, 'train_test_val', "validation_data.csv")
    print('s3_filepath', s3_filepath)
    return read_csv(s3, S3_INPUT_DATA_BUCKET, s3_filepath)


def get_test_data(s3, version):
    s3_filepath = os.path.join(version, 'train_test_val', "test_data.csv")
    return read_csv(s3, S3_INPUT_DATA_BUCKET, s3_filepath)
