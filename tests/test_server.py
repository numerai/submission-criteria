#!/usr/bin/env python
"""Test Server."""

# System
import datetime
import os

# Third Party
import requests

# First Party
from submission_criteria.database_manager import DatabaseManager


def fetch_competition(db):
    now = datetime.datetime.utcnow()
    return db.competitions.find_one({
        "start_date": {
            "$lt": now
        },
        "end_date": {
            "$gt": now
        }
    })


def test_server(db_manager, comp_id):
    submissions = db_manager.get_everyone_elses_recent_submssions(comp_id, '')
    api_key = os.environ.get("API_KEY",
                             "h/52y/E7cm8Ih4F3cVdlBM4ZQxER+Apk6P0L7yR0lFU=")

    for submission in submissions:
        s_id = str(submission["submission_id"])
        print(s_id)
        requests.post(
            "http://localhost:5151/",
            json={
                'submission_id': s_id,
                'api_key': api_key
            })


def main():
    db_manager = DatabaseManager()
    cid = str(fetch_competition(db_manager.db)["_id"])
    print(cid)
    test_server(db_manager, cid)


if __name__ == '__main__':
    main()
