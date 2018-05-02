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

from threading import Thread
from six import StringIO
from six.moves.queue import Queue, Empty


class WinPty(object):

    def __init__(self, stdin):
        self._s = stdin
        self._q = Queue()

        def _read_next_char(stdin, queue):
            while True:
                char = stdin.read(1)  # potentially blocking read
                if char:
                    queue.put(char)
                else:
                    break

        self._t = Thread(target=_read_next_char, args=(self._s, self._q))
        self._t.daemon = True
        self._t.start()  # read characters asynchronously from stdin

    def read(self, blksize=-1, timeout=1):
        buf = StringIO()
        count = 0
        try:
            while count < blksize or blksize == -1:
                next = self._q.get(block=timeout is not None, timeout=timeout)
                buf.write(next)
                count = count + 1
        except Empty:
            pass
        return buf.getvalue()