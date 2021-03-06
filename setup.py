from setuptools import setup

setup(
    name="submission_criteria",
    author="Numerai",
    install_requires=[
        "boto3", "botocore", "pandas>=0.25.3", "s3fs", "tqdm", "scipy",
        "sklearn", "statsmodels", "python-dotenv", "bottle", "numpy", "pqueue",
        "randomstate", "psycopg2", "sqlalchemy", "mysqlclient",
        "pylint>=2.1.1", "flake8", "xgboost==0.81", "requests>=2.20.0",
        "schedule"
    ],
)
