#!/usr/bin/env python

# System
import tempfile
import bz2
import os
import time
import random
from multiprocessing import Pool

# Third Party
import pandas as pd
import numpy as np
import randomstate as rnd

# First Party
from benchmark_base import Benchmark
from submission_criteria.concordance import get_competition_variables_from_df, has_concordance, get_sorted_split

N_SAMPLES = 100 * 1000
N_RUNS = 250


class BenchmarkConcordance(Benchmark):
    @staticmethod
    def load_data():
        data_frames = dict()
        for sample_type, sample_file in [('train',
                                          'data/sample_training.csv.bz2'),
                                         ('predict',
                                          'data/sample_tournament.csv.bz2'),
                                         ('result',
                                          'data/sample_result.csv.bz2')]:
            with tempfile.NamedTemporaryFile() as temp_file, \
                    open(temp_file.name, 'wb') as uncompressed_file, \
                    bz2.BZ2File(sample_file, 'rb') as compressed_file:

                for data in iter(lambda: compressed_file.read(1000 * 1024),
                                 b''):
                    uncompressed_file.write(data)
                data_frames[sample_type] = pd.read_csv(temp_file)
        return data_frames['train'], data_frames['predict'], data_frames[
            'result']

    def gen_more_data(self, train: pd.DataFrame, predict: pd.DataFrame,
                      result: pd.DataFrame):
        new_train = self.gen_similar_df(train, data_types=['train'])
        new_predict = self.gen_similar_df(
            predict, data_types=['live', 'validation', 'test'])

        sample = result.sample(
            len(new_predict), replace=True).probability.copy().values.ravel()
        new_result = pd.DataFrame.from_dict({
            'id':
            new_predict.id.copy(),
            'probability':
            sample + rnd.normal(
                loc=0.0, scale=0.025, size=(len(new_predict), ))
        })
        return new_train, new_predict, new_result

    @staticmethod
    def gen_similar_df(df: pd.DataFrame, data_types: list) -> pd.DataFrame:
        sample_batch_size = 500
        new_df = pd.DataFrame(data=None, columns=df.columns)
        features = [col for col in df.columns if 'feature' in col]

        for batch_nr in range(N_SAMPLES // sample_batch_size):
            sample = df.sample(sample_batch_size, replace=True)
            sample = sample[features] + rnd.normal(
                loc=0.0, scale=0.1, size=sample[features].shape)
            sample = sample.as_matrix()
            new_ids = np.array([
                batch_nr * sample_batch_size + j
                for j in range(sample_batch_size)
            ])

            data_types = [
                random.choice(data_types) for _ in range(sample_batch_size)
            ]
            new_batch = {
                'id':
                new_ids,
                'era': [
                    'era%s' % random.choice([i + 1 for i in range(99)])
                    for _ in range(sample_batch_size)
                ],
                'data_type':
                data_types,
                'target': [
                    random.choice([0, 1])
                    if data_types[i] != 'live' else np.nan
                    for i in range(sample_batch_size)
                ]
            }

            for f_num, feature in enumerate(features):
                new_batch[feature] = sample[:, f_num]
            new_df = pd.concat((new_df, pd.DataFrame.from_dict(new_batch)),
                               axis=0)
        return new_df

    @staticmethod
    def check_concordance(submission, clusters, ids):
        t0 = time.time()
        ids_valid, ids_test, ids_live = ids['valid'], ids['test'], ids['live']
        p1, p2, p3 = get_sorted_split(submission, ids_valid, ids_test,
                                      ids_live)
        c1, c2, c3 = clusters['cluster_1'], clusters['cluster_2'], clusters[
            'cluster_3']
        has_concordance(p1, p2, p3, c1, c2, c3)
        t1 = time.time()
        return (t1 - t0) * 1000

    def benchmark(self):
        # try to use half the available cores to avoid shaky medians per run caused by cpu usage from other processes
        pool_size = os.cpu_count() or 1
        if pool_size > 1:
            pool_size = pool_size // 2

        source_train_data, source_predict_data, source_submission = self.load_data(
        )
        train_data, predict_data, submission_data = \
            self.gen_more_data(source_train_data, source_predict_data, source_submission)

        ids = {
            'test':
            predict_data[predict_data.data_type ==
                         'test'].id.copy().values.ravel(),
            'valid':
            predict_data[predict_data.data_type ==
                         'validation'].id.copy().values.ravel(),
            'live':
            predict_data[predict_data.data_type == 'live'].id.copy().values.
            ravel(),
        }
        clusters = get_competition_variables_from_df(
            '1', train_data, predict_data, ids['valid'], ids['test'],
            ids['live'])

        with Pool(pool_size) as pool:
            times = pool.starmap(self.check_concordance,
                                 [(submission_data, clusters, ids)
                                  for _ in range(N_RUNS)])

        self.log('benchmark finished in %.2fs' % (sum(times) / 1000))
        self.log('[per iteration] %s' % self.format_stats(times, unit='ms'))


def main():
    benchmark = BenchmarkConcordance(n_runs=N_RUNS)
    benchmark.start('benchmarking %s submissions with %s examples each' %
                    (N_RUNS, N_SAMPLES))


if __name__ == '__main__':
    main()
