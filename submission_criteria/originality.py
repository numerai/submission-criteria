# System
"""Originality Checking."""
import logging
import functools
from threading import Lock

# Third Party
from scipy.stats.stats import pearsonr
import numpy as np
import pandas as pd

# First Party
import submission_criteria.common as common

lock = Lock()


@functools.lru_cache(maxsize=2048)
def get_submission(db_manager, filemanager, submission_id):
    """Gets the submission file from S3

    Parameters:
    -----------
    db_manager: DatabaseManager
        DB data access object that has read and write functions to NoSQL DB

    filemanager: FileManager
        S3 Bucket data access object for querying competition datasets

    submission_id : string
        The ID of the submission

    Returns:
    --------
    submission : ndarray
        2d array of the submission probabilities. First column is sorted by ID
        and second column is sorted by probability.
    """
    if not submission_id:
        return None

    s3_filename, _ = common.get_filename(db_manager.postgres_db, submission_id)
    try:

        local_files = filemanager.download([s3_filename])
        if len(local_files) != 1:
            logging.getLogger().info("Error looking for submission {}, found files {}".format(submission_id, local_files))
            return None

        local_file = local_files[0]
    except Exception:
        logging.getLogger().info("Could not get submission {} at S3 path {}".format(submission_id, s3_filename))
        return None

    df = pd.read_csv(local_file)
    assert "id" in df.columns, "No id column in submission {}".format(s3_filename)
    assert "probability" in df.columns, "No probability column in submission {}".format(s3_filename)

    df.sort_values("id", inplace=True)
    df = df["probability"]
    a = df.as_matrix()
    a_sorted = np.sort(a)

    # make a two-column numpy array: first column is sorted by id; second
    # column is sorted by probability
    a = a.reshape(-1, 1)
    a_sorted = a_sorted.reshape(-1, 1)
    a = np.hstack((a, a_sorted))

    return a


def original(submission1, submission2, threshold=0.05):
    """Determines if two submissions are original

    Paramters:
    ----------
    submission1, submission2 : 1-D ndarrays
        Submission arrays that will be used in the Kolmogorov-Smirnov statistic
    threshold : float, optional, default: 0.05
        threshold in which the originality_score must be greater than to be "original"

    Returns:
    --------
    original : bool
        boolean value that indicates if a submission is original
    """
    score = originality_score(submission1, submission2)
    return score > threshold


# this function is taken from scipy (ks_2samp) and modified and so falls
# under their BSD license
def originality_score(data1, data2):
    """
    Computes the Kolmogorov-Smirnov statistic on 2 samples.

    This is a two-sided test for the null hypothesis that 2 independent samples
    are drawn from the same continuous distribution.

    Warning: data1 and data2 are assumed sorted in ascending order.

    Parameters
    ----------
    data1, data2 : ndarray
        Two arrays of sample observations assumed to be drawn from a
        continuous distribution. Arrays must be of the same size. data1 and
        data2 are assumed sorted in ascending order.

    Returns
    -------
    statistic : float
        KS statistic
    """

    # data1 and date2 are assumed sorted in ascending order
    n1 = data1.shape[0]
    n2 = data2.shape[0]
    if n1 != n2:
        raise ValueError("`data1` and `data2` must have the same length")

    # the following commented out line is slower than the two after it
    # cdf1 = np.searchsorted(data1, data_all, side='right') / (1.0*n1)
    cdf1 = np.searchsorted(data1, data2, side='right')
    cdf1 = np.concatenate((np.arange(n1) + 1, cdf1)) / (1.0 * n1)

    # the following commented out line is slower than the two after it
    # cdf2 = np.searchsorted(data2, data_all, side='right') / (1.0*n2)
    cdf2 = np.searchsorted(data2, data1, side='right')
    cdf2 = np.concatenate((cdf2, np.arange(n1) + 1)) / (1.0 * n2)

    d = np.max(np.absolute(cdf1 - cdf2))

    return d


def is_almost_unique(submission_data, submission, db_manager, filemanager, is_exact_dupe_thresh, is_similar_thresh, max_similar_models):
    """Determines how similar/exact a submission is to all other submission for the competition round

    Paramters:
    ----------
    submission_data : dictionary
        Submission metadata containing the submission_id and the user associated to the submission

    submission : ndarray
        Submission data that contains the probabilities for the competition
        data. The array is 2d. First column is sorted by ID and second column
        is sorted by probability.

    db_manager : DatabaseManager
        DB data access object that has read and write functions to NoSQL DB

    filemanager : FileManager
        S3 Bucket data access object for querying competition datasets

    is_exact_dupe_thresh :
        Threshold for determining if a submission is and exact duplicate to another submission

    is_similar_thresh :
        Similarity threshold that determines if a submission is too similar and counts against the submissions originality

    max_similar_models :
        The max number of models that a submission is allow to be similar to

    Returns:
    --------
    bool
        Whether the submission data is considered to be original or not
    """
    num_similar_models = 0
    is_original = True
    similar_models = []
    is_not_a_constant = np.std(submission[:, 0]) > 0

    date_created = db_manager.get_date_created(submission_data['submission_id'])

    get_others = db_manager.get_everyone_elses_recent_submssions

    # first test correlations
    for user_sub in get_others(submission_data["round_id"],
                               submission_data["user_id"], date_created):

        with lock:
            other_submission = get_submission(db_manager, filemanager,
                                              user_sub["id"])
        if other_submission is None:
            continue

        if is_not_a_constant and np.std(other_submission[:, 0]) > 0:
            correlation = pearsonr(submission[:, 0], other_submission[:, 0])[0]
            if np.abs(correlation) > 0.95:
                msg = "Found a highly correlated submission {} with score {}".format(user_sub["id"], correlation)
                logging.getLogger().info(msg)
                is_original = False
                break

    # only run KS test if correlation test passes
    if is_original:
        for user_sub in get_others(submission_data["round_id"],
                                   submission_data["user_id"], date_created):

            with lock:
                other_submission = get_submission(db_manager, filemanager,
                                                  user_sub["id"])
            if other_submission is None:
                continue

            score = originality_score(submission[:, 1], other_submission[:, 1])
            if score < is_exact_dupe_thresh:
                logging.getLogger().info("Found a duplicate submission {} with score {}".format(user_sub["id"], score))
                is_original = False
                break
            if score <= is_similar_thresh:
                num_similar_models += 1
                similar_models.append(user_sub["id"])
                if num_similar_models >= max_similar_models:
                    logging.getLogger().info("Found too many similar models. Similar models were {}".format(similar_models))
                    is_original = False
                    break

    return is_original


def submission_originality(submission_data, db_manager, filemanager):
    """Pulls submission data from DB and determines the originality score and will update the submissions originality score

    This checks a few things
        1. If the current submission is similar to the previous submission, we give it the same originality score
        2. Otherwise, we check that it is sufficently unique. To check this we see if it is A. Almost identitical to
        any other submission or B. Very similar to a handful of other models.

    Parameters:
    -----------
    submission_data : dictionary
        Metadata about the submission pulled from the queue

    db_manager : DatabaseManager
        DB data access object that has read and write functions to DB

    filemanager : FileManager
        S3 Bucket data access object for querying competition datasets
    """
    query = "SELECT round_id, user_id FROM submissions WHERE id='{}'".format(submission_data["submission_id"])
    cursor = db_manager.postgres_db.cursor()
    cursor.execute(query)
    results = cursor.fetchone()
    cursor.close()
    submission_data["round_id"] = results[0]
    submission_data["user_id"] = results[1]
    logging.getLogger().info("Scoring user_id {} submission_id {}".format(submission_data["user_id"], submission_data['submission_id']))

    with lock:
        submission = get_submission(db_manager, filemanager, submission_data['submission_id'])

    if submission is None:
        logging.getLogger().info("Couldn't find submission {}".format(submission_data['submission_id']))
        return

    is_exact_dupe_thresh = 0.005
    is_similar_thresh = 0.03
    max_similar_models = 1

    is_original = is_almost_unique(submission_data, submission, db_manager, filemanager, is_exact_dupe_thresh, is_similar_thresh, max_similar_models)
    db_manager.write_originality(submission_data['submission_id'], is_original)
