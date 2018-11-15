#!/usr/bin/env python
"""Machine Learning Server Unit Testing."""

# System
import tempfile
import warnings

# Third Party
import unittest
from pqueue import Queue


class TestServerFailover(unittest.TestCase):
    fake_requests = ["Not a real request", "Also not a real request"]

    def queue_persistence(self, queue, tdir, qdir):
        self.assertEqual(queue.qsize(), 0)
        for request in self.fake_requests:
            queue.put(request)
        self.assertEqual(queue.qsize(), len(self.fake_requests))

        del queue
        queue = Queue(qdir, tempdir=tdir)

        self.assertEqual(queue.qsize(), len(self.fake_requests))
        for _ in range(len(self.fake_requests)):
            queue.get()
        self.assertEqual(queue.qsize(), 0)

    def test_concordance_queue(self):
        with warnings.catch_warnings():
            warnings.simplefilter('ignore')
            self._test_concordance_queue()

    def _test_concordance_queue(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            with tempfile.TemporaryDirectory() as concordance_dir:
                concordance_queue = Queue(concordance_dir, tempdir=temp_dir)
                self.queue_persistence(concordance_queue, temp_dir,
                                       concordance_dir)


if __name__ == '__main__':
    unittest.main()
