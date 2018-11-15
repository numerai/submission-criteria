#!/usr/bin/env python

# System
import time
import numpy as np

# Third Party
import randomstate as rnd

# First Party
from benchmark_base import Benchmark
from submission_criteria.originality import original

N_RUNS = 5
N_EXAMPLES = 45000
N_OTHER_SUBMISSIONS = 1000


class OriginalityBenchmark(Benchmark):
    @staticmethod
    def gen_submission(predictions=N_EXAMPLES,
                       users=N_OTHER_SUBMISSIONS) -> np.ndarray:
        # numpy's RandomSeed don't play well with multiprocessing; randomstate is a drop-in replacement
        return np.array(
            rnd.normal(loc=0.5, scale=0.1, size=(predictions, users)))

    @staticmethod
    def check_original(new_submission, other_submissions):
        n_predictions = new_submission.shape[0]
        t_iter_start = time.time()
        sorted_submission = np.sort(new_submission.reshape(n_predictions, ))

        t_per_submission = list()
        for i in range(other_submissions.shape[1]):
            t_sub_start = time.time()
            original(sorted_submission, other_submissions[:, i])
            t_per_submission.append((time.time() - t_sub_start) * 1000)

        return (time.time() - t_iter_start), t_per_submission

    def checkpoint(self,
                   times_per_iteration: list,
                   times_per_submission: list = None):
        if not self.print_checkpoint:
            return

        if len(times_per_iteration) == 1:
            self.log(
                '[iteration %s/%s] will benchmark after second iteration...' %
                (1, self.n_runs))
            return

        self.log_stats(times_per_iteration, unit='s')

        if len(times_per_iteration) == self.n_runs:
            self.log('benchmark finished in %.2fs' % sum(times_per_iteration))
            self.log('[per other submission] %s' % self.format_stats(
                times_per_submission, unit='ms'))

    def benchmark(self):
        new_submission = OriginalityBenchmark.gen_submission(users=1)
        other_submissions = OriginalityBenchmark.gen_submission(users=1000)

        times_per_iteration = list()
        times_per_submission = list()

        for _ in range(N_RUNS):
            t_iter, t_subs = OriginalityBenchmark.check_original(
                new_submission, other_submissions)
            times_per_iteration.append(t_iter)
            times_per_submission.extend(t_subs)
            self.checkpoint(times_per_iteration, times_per_submission)


def main():
    benchmark = OriginalityBenchmark(n_runs=N_RUNS)
    benchmark.start('%s runs of %s examples against %s other submissions' %
                    (N_RUNS, N_EXAMPLES, N_OTHER_SUBMISSIONS))


if __name__ == '__main__':
    main()
