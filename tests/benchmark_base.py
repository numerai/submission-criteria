import statistics

from datetime import datetime


class Benchmark():
    def __init__(self, n_runs: int = 5, print_checkpoint: bool = True):
        self.n_runs = n_runs
        self.print_checkpoint = print_checkpoint

    @staticmethod
    def log(message: str) -> None:
        print('[%s] - %s' % (datetime.now(), message))

    def log_stats(self, times: list, unit: str = 'ms'):
        self.log(
            '[iteration %s/%s] %s' % (len(times), self.n_runs,
                                      self.format_stats(times, unit=unit)))

    @staticmethod
    def format_stats(times: list, unit: str) -> str:
        return 'median: %.2f%s, mean: %.2f%s, stdev: %.2f, max: %.2f%s, min: %.2f%s' % (
            statistics.median(times), unit, statistics.mean(times), unit,
            statistics.stdev(times), max(times), unit, min(times), unit)

    def start(self, suffix: str = None):
        if suffix is None:
            suffix = '...'
        else:
            suffix = ': ' + suffix

        self.log('starting benchmark%s' % suffix)
        self.benchmark()

    def benchmark(self):
        raise NotImplementedError('method benchmark() not implemented yet')
