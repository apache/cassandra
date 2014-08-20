# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from threading import Event, Condition
from . import meter
import sys

class _CountDownLatch(object):
    def __init__(self, counter=1):
        self._count = counter
        self._lock = Condition()

    def count_down(self):
        with self._lock:
            self._count -= 1
            if self._count <= 0:
                self._lock.notifyAll()

    def await(self):
        with self._lock:
            while self._count > 0:
                # use a timeout so that the main thread wakes up occasionally
                # so it can see keyboard interrupts (CASSANDRA-7815)
                self._lock.wait(0.5)


class _ChainedWriter(object):

    CONCURRENCY = 100

    def __init__(self, session, enumerated_reader, statement_func):
        self._sentinel = object()
        self._session = session
        self._cancellation_event = Event()
        self._first_error = None
        self._task_counter = _CountDownLatch(self.CONCURRENCY)
        self._enumerated_reader = enumerated_reader
        self._statement_func = statement_func
        self._meter = meter.Meter()

    def insert(self):
        if not self._enumerated_reader:
            return 0, None

        for i in xrange(self.CONCURRENCY):
            self._execute_next(self._sentinel, 0)

        try:
            self._task_counter.await()
        except KeyboardInterrupt:
            self._cancellation_event.set()
            sys.stdout.write('Aborting due to keyboard interrupt\n')
            self._task_counter.await()
        self._meter.done()
        return self._meter.num_finished(), self._first_error


    def _abort(self, error, failed_record):
        if not self._first_error:
            self._first_error = error, failed_record
        self._task_counter.count_down()
        self._cancellation_event.set()

    def _handle_error(self, error, failed_record):
        self._abort(error, failed_record)

    def _execute_next(self, result, last_completed_record):
        if self._cancellation_event.is_set():
            self._task_counter.count_down()
            return

        if result is not self._sentinel:
            self._meter.mark_written()

        try:
            (current_record, row) = next(self._enumerated_reader)
        except StopIteration:
            self._task_counter.count_down()
            return
        except Exception as exc:
            self._abort(exc, last_completed_record)
            return

        if self._cancellation_event.is_set():
            self._task_counter.count_down()
            return

        try:
            statement = self._statement_func(row)
            future = self._session.execute_async(statement)
            future.add_callbacks(callback=self._execute_next,
                                 callback_args=(current_record,),
                                 errback=self._handle_error,
                                 errback_args=(current_record,))
        except Exception as exc:
            self._abort(exc, current_record)
            return


def insert_concurrent(session, enumerated_reader, statement_func):
    return _ChainedWriter(session, enumerated_reader, statement_func).insert()

