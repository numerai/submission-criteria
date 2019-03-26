# System
"""Data access class"""
import os
import datetime

# Third Party
import pandas as pd
from sklearn.metrics import log_loss
import numpy as np
import psycopg2
import psycopg2.extras

# First Party
from submission_criteria import common

BENCHMARK = 0.693


class DatabaseManager():
    def __init__(self):
        self.postgres_db = common.connect_to_postgres()

    def __hash__(self):
        """
        We want to implement the hash function so we can use this with a lru_cache
        but we don't actually care about hashing it.
        """
        return 314159

    def get_round_number(self, submission_id):
        query = "SELECT round_id FROM submissions WHERE id = '{}'".format(
            submission_id)
        cursor = self.postgres_db.cursor()
        cursor.execute(query)
        round_id = cursor.fetchone()[0]
        cursor.execute(
            "SELECT number FROM rounds WHERE id = '{}'".format(round_id))
        result = cursor.fetchone()[0]
        return result

    def update_leaderboard(self, submission_id, filemanager):
        """Update the leaderboard with a submission

        Parameters:
        ----------
        submission_id : string
            ID of the submission

        filemanager : FileManager
            S3 Bucket data access object for querying competition datasets
        """
        print("Calculating consistency for submission_id {}...".format(
            submission_id))
        tournament, round_number, _dataset_path = common.get_round(
            self.postgres_db, submission_id)

        # Get the tournament data
        print("Getting public dataset for round number {}-{}".format(
            tournament, round_number))
        extract_dir = filemanager.download_dataset(tournament, round_number)
        tournament_data = pd.read_csv(
            os.path.join(extract_dir, "numerai_tournament_data.csv"))
        # Get the user submission
        s3_file, _ = common.get_filename(self.postgres_db, submission_id)
        submission_data = filemanager.read_csv(s3_file)
        validation_data = tournament_data[tournament_data.data_type ==
                                          "validation"]
        validation_submission_data = submission_data[submission_data.id.isin(
            validation_data.id.values)]
        validation_eras = np.unique(validation_data.era.values)
        print(validation_eras)
        num_eras = len(validation_eras)
        assert num_eras == 12

        # Calculate era loglosses
        better_than_random_era_count = 0

        for era in validation_eras:
            era_data = validation_data[validation_data.era == era]
            submission_era_data = validation_submission_data[
                validation_submission_data.id.isin(era_data.id.values)]
            assert len(
                submission_era_data > 0), "There must be data for every era"
            era_data = era_data.sort_values(["id"])
            submission_era_data = submission_era_data.sort_values(["id"])
            logloss = log_loss(era_data[common.TARGETS[tournament]].values,
                               submission_era_data.probability.values)
            if logloss < BENCHMARK:
                better_than_random_era_count += 1

        consistency = better_than_random_era_count / num_eras * 100

        print("Consistency: {}".format(consistency))

        # Update consistency and insert pending concordance into Postgres
        cursor = self.postgres_db.cursor()
        cursor.execute(
            "UPDATE submissions SET consistency={} WHERE id = '{}'".format(
                consistency, submission_id))
        cursor.execute(
            "INSERT INTO concordances(pending, submission_id) VALUES(TRUE, '{}') ON CONFLICT (submission_id) DO NOTHING;"
            .format(submission_id))
        self.postgres_db.commit()
        cursor.close()

    def write_concordance(self, submission_id, concordance):
        """Write to both the submission and leaderboard

        Parameters:
        -----------
        submission_id : string
            ID of the submission

        concordance : bool
            The calculated concordance for a submission
        """
        cursor = self.postgres_db.cursor()
        query = "UPDATE concordances SET pending=FALSE, value={} WHERE submission_id = '{}'".format(
            concordance, submission_id)
        cursor.execute(query)
        self.postgres_db.commit()
        cursor.close()

    def get_everyone_elses_recent_submssions(self,
                                             round_id,
                                             user_id,
                                             end_time=None):
        """ Get all submissions in a round, excluding those submitted by the given user_id.

        Parameters:
        -----------
        round_id : int
            The ID of the competition round

        user_id : string
            The username belonging to the submission

        endtime : time, optional, default: None
            Lookback window for querying recent submissions

        Returns:
        --------
        submissions : list
            List of all recent submissions for the competition round less than end_time
        """
        if end_time is None:
            end_time = datetime.datetime.utcnow()
        cursor = self.postgres_db.cursor(
            cursor_factory=psycopg2.extras.RealDictCursor)
        query = """
        SELECT s.id FROM submissions s
        INNER JOIN originalities o
          ON s.id = o.submission_id
        WHERE s.round_id = %s AND
          s.user_id != %s AND
          s.inserted_at < %s AND
          s.selected = TRUE AND
          (o.value = TRUE OR o.pending = TRUE)
        ORDER BY s.inserted_at DESC"""
        cursor.execute(query, [round_id, user_id, end_time])
        results = cursor.fetchall()
        cursor.close()
        return results

    def get_date_created(self, submission_id):
        """Get the date create for a submission"""
        cursor = self.postgres_db.cursor()
        query = "SELECT inserted_at FROM submissions WHERE id = '{}'".format(
            submission_id)
        cursor.execute(query)
        result = cursor.fetchone()[0]
        cursor.close()
        return result
