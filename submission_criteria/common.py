"""
Commonly used functions.
"""

# System
import os

# Third Party
import pandas as pd
from psycopg2 import connect
import boto3
import botocore
from sklearn.metrics import log_loss, roc_auc_score
from submission_criteria import tournament_common as tc

S3_BUCKET = os.environ.get("S3_UPLOAD_BUCKET", "numerai-production-uploads")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
s3 = boto3.resource(
    "s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)
S3_INPUT_DATA_BUCKET = "numerai-tournament-data"
INPUT_DATA_PATH = '/tmp/numerai-input-data'

TARGETS = [
    "sentinel", "target_bernie", "target_elizabeth", "target_jordan",
    "target_ken", "target_charles", "target_frank", "target_hillary"
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
    try:
        res = s3.Bucket(bucket).Object(s3_file).get()
    except botocore.exceptions.EndpointConnectionError:
        print("Could not download {} from S3. Skipping.".format(s3_file))
        return None
    return pd.read_csv(res.get('Body'))


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


# update logloss and auroc
def update_metrics(submission_id):
    """Insert validation and test loglosses into the Postgres database."""
    print("Updating loglosses...")
    postgres_db = connect_to_postgres()
    cursor = postgres_db.cursor()
    submission = read_csv(postgres_db, submission_id)
    tournament, _round_number, dataset_path = get_round(
        postgres_db, submission_id)

    # Get the truth data
    validation_data = tc.get_validation_data(s3, S3_INPUT_DATA_BUCKET,
                                             dataset_path)
    test_data = tc.get_test_data(s3, S3_INPUT_DATA_BUCKET, dataset_path)
    validation_data.sort_values("id", inplace=True)
    test_data.sort_values("id", inplace=True)

    # Sort submission data
    submission_validation_data = submission.loc[submission["id"].isin(
        validation_data["id"].as_matrix())].copy()
    submission_validation_data.sort_values("id", inplace=True)
    submission_test_data = submission.loc[submission["id"].isin(
        test_data["id"].as_matrix())].copy()
    submission_test_data.sort_values("id", inplace=True)

    # Calculate logloss
    validation_logloss = log_loss(
        validation_data[f"target_{tournament}"].as_matrix(),
        submission_validation_data["probability"].as_matrix())
    test_logloss = log_loss(test_data[f"target_{tournament}"].as_matrix(),
                            submission_test_data["probability"].as_matrix())

    # Calculate AUROC (https://stats.stackexchange.com/questions/132777/what-does-auc-stand-for-and-what-is-it)
    validation_auroc = roc_auc_score(
        validation_data[f"target_{tournament}"].as_matrix(),
        submission_validation_data["probability"].as_matrix())
    test_auroc = roc_auc_score(
        test_data[f"target_{tournament}"].as_matrix(),
        submission_test_data["probability"].as_matrix())

    # Insert values into Postgres
    query = "UPDATE submissions SET validation_logloss={}, test_logloss={}, validation_auroc={}, test_auroc={} WHERE id = '{}'".format(
        validation_logloss, test_logloss, validation_auroc, test_auroc, submission_id)
    print(query)
    cursor.execute(query)
    print("Updated {} with validation_logloss={}, test_logloss={}, validation_auroc={}, and test_auroc={}".format(
        submission_id, validation_logloss, test_logloss, validation_auroc, test_auroc))
    postgres_db.commit()
    cursor.close()
    postgres_db.close()
