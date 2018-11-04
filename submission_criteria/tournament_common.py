import datetime
import os
import boto3
import botocore


def _get_s3_resource():
    print("get s3 resource", flush=True)
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
    return boto3.resource("s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)


def _download_data_file(bucket, s3_path, s3_file):
    print("download_data_file", flush=True)
    s3 = _get_s3_resource()
    print("get_s3_resource", flush=True)
    local_path = os.path.join(_get_tmp_directory(), s3_path)
    if not os.path.isdir(local_path):
        os.makedirs(local_path)
    s3_pathfile = os.path.join(s3_path, s3_file)
    local_pathfile = os.path.join(local_path, s3_file)
    try:
        s3.meta.client.download_file(bucket, s3_pathfile, local_pathfile)
    except (botocore.exceptions.ClientError,
            botocore.exceptions.EndpointConnectionError) as e:
        print("Error when downloading {}: {}".format(s3_pathfile, str(e)))
        return None
    return local_pathfile


def _get_tmp_directory():
    return os.environ.get("TMPDIR", "/tmp/")


def get_s3_dataset_path(date):
    return date.strftime('%Y%m%d')


def _download_tournament_data_file(s3_path, s3_file):
    print("hi anson")
    print(s3_path)
    print(s3_file)
    print(os.environ)
    bucket = "numerai-tournament-data"
    return _download_data_file(bucket, s3_path, s3_file)


def get_tournament_file(s3_file, folder=None):
    if folder is None:
        date = datetime.date.today()
        folder = get_s3_dataset_path(date)
    return _download_tournament_data_file(folder, s3_file)


def get_live_targets(folder=None):
    return get_tournament_file("live_targets.csv", folder)


def get_live_features(folder=None):
    return get_tournament_file("live_features.csv", folder)


def get_test_features(folder=None):
    return get_tournament_file("test_features.csv", folder)


def get_train_data(folder=None):
    return get_tournament_file("train_data.csv", folder)


def get_train_features(folder=None):
    return get_tournament_file("train_features.csv", folder)


def get_train_targets(folder=None):
    return get_tournament_file("train_targets.csv", folder)


def get_validation_data(folder=None):
    return get_tournament_file("validation_data.csv", folder)


def get_validation_features(folder=None):
    return get_tournament_file("validation_features.csv", folder)


def get_validation_targets(folder=None):
    return get_tournament_file("validation_targets.csv", folder)
