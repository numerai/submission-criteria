"""
Commonly used functions.
"""

# System
import os

# Third Party
import pandas as pd
from psycopg2 import connect
import tournament_common as tc
import boto3
import botocore
from sqlalchemy import create_engine
from sklearn.metrics import log_loss

TARGETS = ["sentinel", "target_bernie", "target_elizabeth", "target_jordan", "target_ken", "target_charles"]
S3_BUCKET = os.environ.get("S3_UPLOAD_BUCKET", "numerai-production-uploads")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY")
s3 = boto3.resource("s3", aws_access_key_id=S3_ACCESS_KEY, aws_secret_access_key=S3_SECRET_KEY)


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
        SELECT r.tournament, r.number, dataset_path
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
    query = "SELECT filename, user_id FROM submissions WHERE id = '{}'".format(submission_id)
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


def download_submission(postgres_db, submission_id):
    global s3
    bucket = S3_BUCKET

    s3_file, filename = get_filename(postgres_db, submission_id)
    path = os.path.join("/tmp/", filename)
    if not os.path.isfile(path):
        try:
            s3.meta.client.download_file(bucket, s3_file, path)
        except botocore.exceptions.EndpointConnectionError:
            print("Could not download {} from S3. Skipping.".format(s3_file))
            return None
    return path


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


def connect_to_public_targets_db():
    """Connect to the public targets database."""
    url = os.environ.get("SQL_URL")
    if not url:
        url = get_secret("SQL_URL")
    if url is None or url == "":
        raise Exception("You must specify SQL_URL")
    db = create_engine(url, echo=False)
    return db


def update_loglosses(submission_id):
    """Insert validation and test loglosses into the Postgres database."""
    print("Updating loglosses...")
    postgres_db = connect_to_postgres()
    cursor = postgres_db.cursor()
    submission_path = download_submission(postgres_db, submission_id)
    submission = pd.read_csv(submission_path)
    tournament, _round_number, dataset_path = get_round(postgres_db, submission_id)

    # Get the truth data
    public_targets_db = connect_to_public_targets_db()
    validation_path = tc.get_validation_data(dataset_path)
    validation_data = pd.read_csv(validation_path)
    validation_data.sort_values("id", inplace=True)
    test_data = tc.get_test_data(dataset_path)
    test_data.sort_values("id", inplace=True)

    # Calculate logloss
    submission_validation_data = submission.loc[submission["id"].isin(validation_data["id"].as_matrix())].copy()
    submission_validation_data.sort_values("id", inplace=True)
    submission_test_data = submission.loc[submission["id"].isin(test_data["id"].as_matrix())].copy()
    submission_test_data.sort_values("id", inplace=True)
    validation_logloss = log_loss(validation_data[TARGETS[tournament]].as_matrix(), submission_validation_data["probability"].as_matrix())
    test_logloss = log_loss(test_data[TARGETS[tournament]].as_matrix(), submission_test_data["probability"].as_matrix())
    
    # Insert values into Postgres
    query = "UPDATE submissions SET validation_logloss={}, test_logloss={} WHERE id = '{}'".format(validation_logloss, test_logloss, submission_id)
    print(query)
    cursor.execute(query)
    print("Updated {} with validation_logloss={} and test_logloss={}".format(submission_id, validation_logloss, test_logloss))
    postgres_db.commit()
    cursor.close()
    postgres_db.close()
