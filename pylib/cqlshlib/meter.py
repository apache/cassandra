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

from time import time
import sys
from threading import RLock


class Meter(object):

    def __init__(self):
        self._num_finished = 0
        self._last_checkpoint_time = None
        self._current_rate = 0.0
        self._lock = RLock()

    def mark_written(self):
        with self._lock:
            if not self._last_checkpoint_time:
                self._last_checkpoint_time = time()
            self._num_finished += 1

            if self._num_finished % 10000 == 0:
                previous_checkpoint_time = self._last_checkpoint_time
                self._last_checkpoint_time = time()
                new_rate = 10000.0 / (self._last_checkpoint_time - previous_checkpoint_time)
                if self._current_rate == 0.0:
                    self._current_rate = new_rate
                else:
                    self._current_rate = (self._current_rate + new_rate) / 2.0

            if self._num_finished % 1000 != 0:
                return
            output = 'Processed %s rows; Write: %.2f rows/s\r' % \
                     (self._num_finished, self._current_rate)
            sys.stdout.write(output)
            sys.stdout.flush()

    def num_finished(self):
        with self._lock:
            return self._num_finished

    def done(self):
        print ""


