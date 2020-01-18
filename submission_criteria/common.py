"""
Commonly used functions.
"""

# System
import os

# Third Party
import pandas as pd
import numpy as np
from psycopg2 import connect
import boto3
import botocore
from submission_criteria import tournament_common as tc

S3_BUCKET = os.environ.get("S3_UPLOAD_BUCKET", "numerai-production-uploads")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
s3 = boto3.resource("s3",
                    aws_access_key_id=S3_ACCESS_KEY,
                    aws_secret_access_key=S3_SECRET_KEY)

TARGETS = [
    "sentinel",
    "target_bernie",
    "target_elizabeth",
    "target_jordan",
    "target_ken",
    "target_charles",
    "target_frank",
    "target_hillary",
    "target_kazutsugi",
]


def get_secret(key):
    """Return a secret from S3."""
    global s3
    bucket = os.environ.get("S3_SECRETS_BUCKET", "numerai-api-ml-secrets")
    print("bucket", bucket)
    obj = s3.Object(bucket, key)
    secret = obj.get()['Body'].read().decode('utf-8')
    return secret


def get_round(postgres_db, submission_id):
    query = """
        SELECT r.tournament, r.number, r.dataset_path
        FROM submissions s
        INNER JOIN rounds r
          ON s.round_id = r.id
            AND s.id = %s
        """
    cursor = postgres_db.cursor()
    cursor.execute(query, [submission_id])
    tournament, round_number, dataset_path = cursor.fetchone()
    cursor.close()
    return tournament, round_number, dataset_path


def get_filename(postgres_db, submission_id):
    query = "SELECT filename, user_id FROM submissions WHERE id = '{}'".format(
        submission_id)
    cursor = postgres_db.cursor()
    cursor.execute(query)
    results = cursor.fetchone()
    filename = results[0]
    user_id = results[1]
    query = "SELECT username FROM users WHERE id = '{}'".format(user_id)
    cursor.execute(query)
    username = cursor.fetchone()[0]
    cursor.close()
    return "{}/{}".format(username, filename), filename


def read_csv(postgres_db, submission_id):
    global s3
    bucket = S3_BUCKET

    s3_file, _ = get_filename(postgres_db, submission_id)
    return pd.read_csv(f's3://{bucket}/{s3_file}')


def connect_to_postgres():
    """Connect to postgres database."""
    postgres_creds = os.environ.get("POSTGRES_CREDS")
    if not postgres_creds:
        postgres_creds = get_secret("POSTGRES_CREDS")
    print("Using {} Postgres database credentials".format(postgres_creds))
    postgres_url = os.environ.get("POSTGRES")
    if not postgres_url:
        postgres_url = get_secret("POSTGRES")
    return connect(postgres_url)


def calc_correlation(targets, predictions):
    return np.corrcoef(targets, predictions.rank(pct=True, method="first"))[0,
                                                                            1]


# update logloss and auroc
def update_metrics(submission_id):
    """Insert validation scores into the Postgres database."""
    print("Updating loglosses...", submission_id)
    postgres_db = connect_to_postgres()
    cursor = postgres_db.cursor()
    submission = read_csv(postgres_db, submission_id).set_index('id')
    tournament, _round_number, dataset_path = get_round(
        postgres_db, submission_id)

    # Get the truth data
    print("Getting validation data...", submission_id)
    dataset_version = dataset_path.split('/')[0]
    validation_data = tc.get_validation_data(s3,
                                             dataset_version).set_index('id')

    # Sort submission data
    print("Getting validation subset of data...", submission_id)
    submission_validation_data = submission.loc[validation_data.index]

    # Calculate correlation
    print("Calculating validation_correlation...", submission_id)
    validation_correlation = calc_correlation(
        validation_data[f"target_{tournament}"],
        submission_validation_data.probability)

    # Insert values into Postgres
    print("Updating validation_correlation...", submission_id)
    query = "UPDATE submissions SET validation_correlation={} WHERE id = '{}'".format(
        validation_correlation, submission_id)
    print(query)
    cursor.execute(query)
    print("Updated {} with validation_correlation={}'".format(
        submission_id, validation_correlation))
    postgres_db.commit()
    cursor.close()
    postgres_db.close()
