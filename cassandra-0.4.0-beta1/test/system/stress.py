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

# PYTHONPATH=test nosetests --tests=system.stress:Stress.ten_million_inserts

from hashlib import md5
from threading import Thread
from thread import get_ident
import time

from . import get_client, root, CassandraTester
from ttypes import *

class Inserter(Thread):
    def run(self):
        id = get_ident()
        self.count = 0
        client = get_client(port=9160)
        client.transport.open()
        for i in xrange(0, 200):
            data = md5(str(i)).hexdigest()
            for j in xrange(0, 1000):
                key = '%s.%s.%s' % (time.time(), id, j)
                client.insert('Keyspace1', key, ColumnPath('Standard1', column='A'), data, i, 1)
                client.insert('Keyspace1', key, ColumnPath('Standard1', column='B'), data, i, 1)
                self.count += 1

class Stress(CassandraTester):
    runserver = False

    def ten_million_inserts(self):
        threads = []
        for i in xrange(0, 50):
            th = Inserter()
            threads.append(th)
            th.start()

        total = old_total = 0
        while True:
            time.sleep(10)
            old_total = total
            total = sum(th.count for th in threads)
            delta = total - old_total
            file('/tmp/progress', 'w').write('%d at %d/s\n' % (total, delta / 10))
            if not [th for th in threads if th.isAlive()]:
                file('/tmp/progress', 'w').write('done -- %s\n' % str(total))
                break
