#!/usr/bin/env python
"""Machine Learning Server."""

# System
import threading
import sys
import os
from datetime import datetime
import logging

# Third Party
import time
from pqueue import Queue
import numpy as np
import schedule
from bottle import run, request, route
from file_manager import FileManager

# First Party
from database_manager import DatabaseManager
import concordance
import common

PORT = os.environ.get("PORT", "5151")
API_KEY = os.environ.get("API_KEY")
if not API_KEY:
    API_KEY = common.get_secret("API_KEY")
if not API_KEY:
    API_KEY = "h/52y/E7cm8Ih4F3cVdlBM4ZQxER+Apk6P0L7yR0lFU="
TEMP_DIR, OQ_DIR, CQ_DIR = "queue_temp", "oqueue", "cqueue"
LB_TEMP_DIR, LBQ_DIR = "lb_temp", "lbqueue"

for d in [TEMP_DIR, OQ_DIR, CQ_DIR, LB_TEMP_DIR, LBQ_DIR]:
    if not os.path.exists(d):
        os.makedirs(d)

concordance_queue = Queue(CQ_DIR, tempdir=TEMP_DIR)
leaderboard_queue = Queue(LBQ_DIR, tempdir=LB_TEMP_DIR)


@route('/', method='POST')
def queue_for_scoring():
    """ Recieves a submission and authenticates that the request has a valid API key.

    Once authenticated the submission request is then queued to the leaderboard_queue and later checked for concordance.

    """
    json = request.json
    submission_id = json["submission_id"]
    api_key = json["api_key"]

    if API_KEY is None:
        logging.getLogger().critical("NO API KEY EXITING")
        return
    if api_key != API_KEY:
        logging.getLogger().info(
            "Received invalid post request with incorrect api_key {} and submission_id {}"
            .format(api_key, submission_id))
        return

    logging.getLogger().info(
        "Received request to score {}".format(submission_id))

    data = {
        "submission_id": submission_id,
        "enqueue_time": datetime.now(),
    }

    leaderboard_queue.put(data)

    common.update_metrics(submission_id)


def put_submission_on_lb(db_manager, filemanager):
    """Pulls submissions from leaderboard_queue and pushes submissions to concordance queue for scoring"""
    while True:
        submission = leaderboard_queue.get()
        try:
            db_manager.update_leaderboard(submission["submission_id"],
                                          filemanager)

            for queue in [concordance_queue]:
                queue.put(submission)

            leaderboard_queue.task_done()
        except Exception:
            logging.exception("Exception updating submission.")


def score_concordance(db_manager, filemanager):
    """Pulls submission from concordance_queue for concordance check"""
    while True:
        submission = concordance_queue.get()
        try:
            concordance.submission_concordance(submission, db_manager,
                                               filemanager)
            if 'enqueue_time' in submission:
                time_taken = datetime.now() - submission['enqueue_time']
                logging.getLogger().info(
                    "Submission {} took {} to complete concordance".format(
                        submission['submission_id'], time_taken))

            concordance_queue.task_done()
        except Exception:
            logging.exception("Exception scoring concordance.")


def create_logger():
    """Configure the logger to print process ID."""
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '%(asctime)s - %(process)d - {} - %(message)s'.format(
            "Machine learning Server"))
    ch.setFormatter(formatter)
    root.addHandler(ch)


def schedule_cleanup(filemanager):
    """
    Tell the filemanager to clean up every day
    """
    # schedule a daily cleanup
    schedule.every(1).days.do(filemanager.clean_up)

    # run pending jobs every hour
    while 1:
        schedule.run_pending()
        time.sleep(3600)


def main():
    """
    The threading in this file works like this

    We have a bottle server listening for submissions. When it gets a submission
    it gives it to the put_submission_on_lb. This makes sure that the user is on the
    leaderboard/ the leaderboard reflects their most up to date submission.

    That method then enqueues the submission for concordance check.
    """
    np.random.seed(1337)

    create_logger()
    db_manager = DatabaseManager()
    fm = FileManager('/tmp/', logging)
    logging.getLogger().info("Creating servers")

    threading.Thread(
        target=run, kwargs=dict(host='0.0.0.0', port=int(PORT))).start()
    logging.getLogger().info("Spawning new threads to score concordance")

    threading.Thread(
        target=put_submission_on_lb,
        kwargs=dict(db_manager=db_manager, filemanager=fm)).start()
    threading.Thread(
        target=score_concordance,
        kwargs=dict(db_manager=db_manager, filemanager=fm)).start()

    # clean up the /tmp folder so we don't run out of disk space
    threading.Thread(
        target=schedule_cleanup, kwargs=dict(filemanager=fm)).start()


if __name__ == '__main__':
    main()
