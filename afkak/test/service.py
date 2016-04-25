# -*- coding: utf-8 -*-
# Copyright (C) 2015 Cyan, Inc.

import logging
import re
import select
import subprocess
import threading
import time
import errno

log = logging.getLogger(__name__)
log.addHandler(logging.NullHandler())

__all__ = [
    'ExternalService',
    'SpawnedService',
]


class ExternalService(object):  # pragma: no cover
    def __init__(self, host, port):
        log.info("Using already running service at %s:%d" % (host, port))
        self.host = host
        self.port = port

    def open(self):
        pass

    def close(self):
        pass


class SpawnedService(threading.Thread):
    def __init__(self, args=None, env=None, tag=''):
        threading.Thread.__init__(self)

        self.args = args
        self.env = env
        self.tag = tag if not tag else '{}:'.format(tag)
        self.captured_stdout = []
        self.captured_stderr = []

        self.should_die = threading.Event()

    def run(self):
        self.run_with_handles()

    def run_with_handles(self):
        killing_time = 60  # Wait up to 20 seconds before resorting to kill
        log.debug("self.args:%r self.env:%r", self.args, self.env)
        self.child = subprocess.Popen(
            self.args,
            env=self.env,
            bufsize=1,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
        alive = True

        while True:
            (rds, _, _) = select.select(
                [self.child.stdout, self.child.stderr], [], [], 0.25)

            if self.child.stdout in rds:
                line = self.child.stdout.readline()
                self.captured_stdout.append(line)

            if self.child.stderr in rds:  # pragma: no cover
                line = self.child.stderr.readline()
                self.captured_stderr.append(line)

            if self.should_die.is_set():
                self.child.terminate()

                start_time = time.time()
                while self.child.poll() is None:
                    time.sleep(0.1)
                    if time.time() > start_time + killing_time:
                        import datetime
                        now = datetime.datetime.now().isoformat()
                        log.error(
                            'Child process: %r failed to exit within: %d. '
                            'Resorting to kill at: %s.', self.child,
                            killing_time, now)
                        self.child.kill()
                        self.dump_logs()
                alive = False

            poll_results = self.child.poll()
            if poll_results is not None:
                if not alive:
                    break
                else:  # pragma: no cover
                    raise RuntimeError(
                        "Subprocess has died. Aborting. "
                        "(args=%s)\n"
                        "____________________"
                        "Service stdout output:"
                        "____________________\n%s"
                        "____________________"
                        "Service stderr output:"
                        "____________________\n%s"
                        "____________________"
                        "Service stderr complete:"
                        "__________________\n" % (
                            ' '.join(str(x) for x in self.args),
                            ' '.join(self.captured_stdout),
                            ' '.join(self.captured_stderr)))

    def dump_logs(self):  # pragma: no cover
        log.debug(
            '____________________Service stdout output:____________________')
        for line in self.captured_stdout:
            log.debug(line.rstrip())
        log.debug(
            '____________________Service stderr output:____________________')
        for line in self.captured_stderr:
            log.debug(line.rstrip())
        log.debug(
            '____________________Service stderr complete:__________________')

    def wait_for(self, pattern, timeout=10):
        t1 = time.time()
        while True:
            t2 = time.time()
            if t2 - t1 >= timeout:  # pragma: no cover
                try:
                    self.child.kill()
                except OSError as exc:
                    if exc.errno != errno.ESRCH:
                        log.exception(
                            "Received exception when killing child process")
                self.dump_logs()

                raise RuntimeError(
                    "Waiting for {!r} timed out after {} seconds".format(
                        pattern, timeout))

            if re.search(pattern, '\n'.join(
                    self.captured_stdout), re.IGNORECASE) is not None:
                log.info("Found pattern %r in %d seconds via stdout",
                         pattern, (t2 - t1))
                return
            if re.search(pattern, '\n'.join(
                    self.captured_stderr),
                    re.IGNORECASE) is not None:  # pragma: no cover
                log.info("Found pattern %r in %d seconds via stderr",
                         pattern, (t2 - t1))
                return
            time.sleep(0.05)

    def start(self):
        threading.Thread.start(self)

    def stop(self):
        self.should_die.set()
        self.join()
