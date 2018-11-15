#!/usr/bin/env python
"""Integration testing."""

# System
import os
import logging
import time
import traceback
import sys
from uuid import uuid4 as uuid

from concurrent import futures
from concurrent.futures import ThreadPoolExecutor

# Third Party
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.neural_network import MLPClassifier
# from sklearn.neighbors import KNeighborsClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier  # , AdaBoostClassifier, GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis
from xgboost.sklearn import XGBClassifier

# needed for module level import
sys.path.insert(0, os.path.dirname(os.path.dirname(sys.argv[0])))

from tests.testing_api import NumerAPI

# the methods we're testing
from submission_criteria.concordance import get_sorted_split
from submission_criteria.concordance import has_concordance
from submission_criteria.concordance import get_competition_variables_from_df

DATA_SET_PATH = 'tests/numerai_datasets'
DATA_SET_FILE = 'numerai_dataset'
TRAIN_FILE = 'numerai_training_data.csv'
TOURN_FILE = 'numerai_tournament_data.csv'
test_csv = "tests/test_csv"
clf_n_jobs = 2

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger('integration_test')
upload_executor = ThreadPoolExecutor(max_workers=10)
clf_executor = ThreadPoolExecutor(max_workers=clf_n_jobs)
submission_executor = ThreadPoolExecutor(max_workers=2)


class NumeraiApiWrapper(NumerAPI):
    """
    Skips uploading to server to check concordance; not an actual integration test
    if this wrapper is used, but the relevant parts are tested. Use the normal NumerAPI instead
    of NumerApiWrapper and this test will be an integration test behaving in the same way, but
    since there's not dedicated test token that can be used, and no zip support on upload, and
    there's roughly 500 MB currently needing to be uploaded every time this test is run, it's
    not always practical.

    Probably we should mock submission-criteria with an in-memory postgresql db, and try to call
    the relevant methods from this wrapper to avoid trying to mock http requests (we're not
    testing the NumerAPI after all).
    """

    def __init__(self, public_id=None, secret_key=None, verbosity="INFO"):
        super(NumeraiApiWrapper, self).__init__(public_id, secret_key,
                                                verbosity)
        self.checked = set()
        self.cluster_ids = dict()
        self.clusters = dict()
        self.futures = dict()

    def set_data(self, tournament_data, training_data):
        self.cluster_ids = {
            'test':
            tournament_data[tournament_data.data_type ==
                            'test'].id.copy().values.ravel(),
            'valid':
            tournament_data[tournament_data.data_type ==
                            'validation'].id.copy().values.ravel(),
            'live':
            tournament_data[tournament_data.data_type == 'live'].id.copy().
            values.ravel(),
        }
        self.clusters = get_competition_variables_from_df(
            '1', training_data, tournament_data, self.cluster_ids['valid'],
            self.cluster_ids['test'], self.cluster_ids['live'])

    def upload_predictions(self, file_path):
        sub_id = str(uuid())
        self.futures[sub_id] = submission_executor.submit(
            self.check_concordance, file_path)
        return sub_id

    def check_concordance(self, submission_file_path):
        submission = pd.read_csv(submission_file_path)
        ids_valid, ids_test, ids_live = self.cluster_ids[
            'valid'], self.cluster_ids['test'], self.cluster_ids['live']
        p1, p2, p3 = get_sorted_split(submission, ids_valid, ids_test,
                                      ids_live)
        c1, c2, c3 = self.clusters['cluster_1'], self.clusters[
            'cluster_2'], self.clusters['cluster_3']
        has_it = has_concordance(p1, p2, p3, c1, c2, c3)
        # logger.info('submission %s has concordance? %s' % (submission_file_path, str(has_it)))
        return has_it

    def submission_status(self, submission_id=None):
        if submission_id not in self.futures:
            raise ValueError('unknown submission id %s' % submission_id)

        f = self.futures.get(submission_id)
        if not f.done():
            pending = True
            value = False
        else:
            pending = False
            value = f.result()

        return {
            'concordance': {
                'pending': pending,
                'value': value
            }
        }


def main():
    # when running on circleci, set the vars in the project settings
    public_id = os.environ.get('NUMERAPI_PUBLIC_ID', '')
    secret_key = os.environ.get('NUMERAPI_SECRET_KEY', '')

    if not os.path.exists(test_csv):
        os.makedirs(test_csv)

    napi = NumeraiApiWrapper(public_id=public_id, secret_key=secret_key)

    if not os.path.exists(DATA_SET_PATH):
        logger.info("Downloading the current dataset...")
        os.makedirs(DATA_SET_PATH)
        napi.download_current_dataset(
            dest_path=DATA_SET_PATH,
            dest_filename=DATA_SET_FILE + '.zip',
            unzip=True)
        import shutil
        shutil.move(
            os.path.join(DATA_SET_PATH, DATA_SET_FILE, TRAIN_FILE),
            os.path.join(DATA_SET_PATH, TRAIN_FILE))
        shutil.move(
            os.path.join(DATA_SET_PATH, DATA_SET_FILE, TOURN_FILE),
            os.path.join(DATA_SET_PATH, TOURN_FILE))
    else:
        logger.info("Found old data to use.")

    training_data = pd.read_csv(
        '%s/%s' % (DATA_SET_PATH, TRAIN_FILE), header=0)
    tournament_data = pd.read_csv(
        '%s/%s' % (DATA_SET_PATH, TOURN_FILE), header=0)

    napi.set_data(tournament_data, training_data)

    features = [f for f in list(training_data) if "feature" in f]
    features = features[:len(features) //
                        2]  # just use half, speed things up a bit
    X, Y = training_data[features], training_data[
        "target_bernie"]  # hardcode to target bernie for now

    x_prediction = tournament_data[features]
    ids = tournament_data["id"]

    clfs = [
        RandomForestClassifier(
            n_estimators=15,
            max_features=1,
            max_depth=2,
            n_jobs=1,
            criterion='entropy',
            random_state=42),
        XGBClassifier(
            learning_rate=0.1,
            subsample=0.4,
            max_depth=2,
            n_estimators=20,
            nthread=1,
            seed=42),
        DecisionTreeClassifier(max_depth=5, random_state=42),
        MLPClassifier(alpha=1, hidden_layer_sizes=(25, 25), random_state=42),
        GaussianNB(),
        QuadraticDiscriminantAnalysis(tol=1.0e-3),
        # last item can have multiple jobs since it may be the last to be processed so we have an extra core
        LogisticRegression(
            n_jobs=2,
            solver='sag',
            C=1,
            tol=1e-2,
            random_state=42,
            max_iter=50)
    ]

    before = time.time()
    fit_all(clfs, X, Y)
    logger.info('all clfs fit() took %.2fs' % (time.time() - before))

    before = time.time()
    uploads_wait_for_legit = predict_and_upload_legit(napi, clfs, x_prediction,
                                                      ids)
    logger.info(
        'all legit clfs predict_proba() took %.2fs' % (time.time() - before))

    before = time.time()
    uploads_wait_for_mix = predict_and_upload_mix(napi, clfs, tournament_data,
                                                  x_prediction, ids)
    logger.info(
        'all mix clfs predict_proba() took %.2fs' % (time.time() - before))

    legit_submission_ids = list()
    mix_submission_ids = list()

    before = time.time()
    for f in futures.as_completed(uploads_wait_for_legit):
        legit_submission_ids.append(f.result())
    logger.info('await legit uploads took %.2fs' % (time.time() - before))

    before = time.time()
    for f in futures.as_completed(uploads_wait_for_mix):
        mix_submission_ids.append(f.result())
    logger.info('await mix uploads took %.2fs' % (time.time() - before))

    n_passed_concordance = get_concordance(
        napi, legit_submission_ids)
    if len(n_passed_concordance) != len(clfs):
        logger.error('legit passed concordance %s/%s' %
                     (len(n_passed_concordance), len(clfs)))
        sys.exit(1)
    else:
        logger.info('all legit tests passed!')

    n_passed_concordance = get_concordance(
        napi, mix_submission_ids)
    if len(n_passed_concordance) > 0:
        logger.error('mix passed concordance %s/%s' %
                     (len(n_passed_concordance), len(clfs)))
        sys.exit(1)
    else:
        logger.info('all mix tests passed!')

    sys.exit(0)


def get_concordance(napi, _submission_ids):
    submission_ids = _submission_ids.copy()
    n_passed_concordance = set()

    while True:
        statuses = list()
        for submission_id in submission_ids:
            statuses.append(
                upload_executor.submit(check_status, napi, submission_id))

        check_later = list()
        for f in futures.as_completed(statuses):
            submission_id = f.result()['id']
            concordance = f.result()['result']['concordance']

            if concordance['pending']:
                check_later.append(f.result()['id'])
            if concordance['value']:
                n_passed_concordance.add(submission_id)

        if len(check_later) == 0:
            break

        submission_ids.clear()
        submission_ids = check_later.copy()

    return n_passed_concordance


def check_status(napi, submission_id):
    try:
        return {
            'id': submission_id,
            'result': napi.submission_status(submission_id)
        }
    except Exception as e:
        logger.exception(traceback.format_exc())
        logger.error('could not check submission status: %s' % str(e))
        sys.exit(1)


def fit_all(clfs: list, X, Y):
    wait_for = list()

    for clf in clfs:
        wait_for.append(clf_executor.submit(fit_clf, X, Y, clf))

    before = time.time()
    for _ in futures.as_completed(wait_for):
        pass
    logger.info('await fitting took %.2fs' % (time.time() - before))


def fit_clf(X, Y, clf):
    before = time.time()
    clf_str = str(clf).split("(")[0]
    clf.fit(X, Y)
    time_taken = '%.2fs' % (time.time() - before)
    logger.info('fit() took %s%s (%s)' % (time_taken, ' ' *
                                          (9 - len(time_taken)), clf_str))
    return clf_str


def predict_and_upload_legit(napi, clfs: list, x_prediction, ids):
    wait_for = list()
    upload_wait_for = list()

    for clf in clfs:
        wait_for.append(
            clf_executor.submit(predict_and_upload_one_legit, upload_wait_for,
                                napi, clf, x_prediction, ids))

    before = time.time()
    for _ in futures.as_completed(wait_for):
        pass
    logger.info('await legit predictions took %.2fs' % (time.time() - before))

    return upload_wait_for


def predict_and_upload_one_legit(upload_wait_for: list, napi, clf,
                                 x_prediction, ids):
    clf_str = str(clf).split("(")[0]

    before = time.time()
    y_prediction = clf.predict_proba(x_prediction)
    after = time.time()

    out = os.path.join(test_csv, "{}-legit.csv".format(clf_str))
    time_taken = '%.2fs' % (after - before)
    logger.info('predict_proba() took %s%s (%s)' %
                (time_taken, ' ' * (9 - len(time_taken)), out))

    upload_wait_for.append(
        upload_executor.submit(upload_one_legit, y_prediction, ids, out, napi))


def upload_one_legit(y_prediction, ids, out: str, napi):
    try:
        results = y_prediction[:, 1]
        results_df = pd.DataFrame(data={'probability': results})
        joined = pd.DataFrame(ids).join(results_df)

        # Save the predictions out to a CSV file
        joined.to_csv(out, index=False)

        return napi.upload_predictions(out)
    except Exception as e:
        logger.exception(traceback.format_exc())
        logger.error('error uploading: %s' % str(e))


def predict_and_upload_mix(napi, clfs: list, tournament_data: pd.DataFrame,
                           x_prediction, ids):
    valid = tournament_data["data_type"] == "validation"
    test = tournament_data["data_type"] != "validation"

    x_pv, ids_v = x_prediction[valid], ids[valid]
    x_pt, ids_t = x_prediction[test], ids[test]

    wait_for = list()
    uploads_wait_for = list()

    checked_combos = set()

    for i, clf1 in enumerate(clfs[:len(clfs) // 2]):
        for j, clf2 in enumerate(clfs[:len(clfs) // 2]):
            if i == j:
                continue

            name_1 = str(clf1).split("(")[0] + '-' + str(clf2).split("(")[0]
            name_2 = str(clf2).split("(")[0] + '-' + str(clf1).split("(")[0]

            if name_1 in checked_combos or name_2 in checked_combos:
                continue

            checked_combos.add(name_1)
            checked_combos.add(name_2)

            wait_for.append(
                clf_executor.submit(predict_and_upload_one_mix, napi,
                                    uploads_wait_for, (clf1, clf2),
                                    (x_pv, x_pt), (ids_v, ids_t)))

    before = time.time()
    for _ in futures.as_completed(wait_for):
        pass
    logger.info('await mix predictions took %.2fs' % (time.time() - before))
    return uploads_wait_for


def predict_and_upload_one_mix(napi, uploads_wait_for: list, clfs: tuple,
                               xs: tuple, ids: tuple) -> None:
    clf1, clf2, = clfs
    x_pv, x_pt = xs
    ids_v, ids_t = ids
    before_one_mix = time.time()
    y_pv = clf1.predict_proba(x_pv)[:, 1]
    y_pt = clf2.predict_proba(x_pt)[:, 1]

    out = os.path.join(
        test_csv, "{}-{}-mix.csv".format(
            str(clf1).split("(")[0],
            str(clf2).split("(")[0]))
    time_taken = '%.2fs' % (time.time() - before_one_mix)
    logger.info('pred mix took  %s%s (%s)' % (time_taken, ' ' *
                                              (10 - len(time_taken)), out))

    uploads_wait_for.append(
        upload_executor.submit(upload_one_mix, napi, out, ids_v, ids_t, y_pt,
                               y_pv))


def upload_one_mix(napi, out, ids_v, ids_t, y_pt, y_pv):
    try:
        valid_df = pd.DataFrame(ids_v).join(
            pd.DataFrame(data={'probability': y_pv}))
        test_df = pd.DataFrame(ids_t).join(
            pd.DataFrame(data={'probability': y_pt}))

        before_csv_write = time.time()
        mix = pd.concat([valid_df, test_df])
        mix.to_csv(out, index=False)
        time_taken = '%.2fs' % (time.time() - before_csv_write)
        logger.info('write csv took %s%s (%s)' % (time_taken, ' ' *
                                                  (10 - len(time_taken)), out))

        return napi.upload_predictions(out)
    except Exception as e:
        logger.exception(traceback.format_exc())
        logger.error('error uploading: %s' % str(e))


if __name__ == '__main__':
    main()
