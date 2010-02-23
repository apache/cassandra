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

# PYTHONPATH=test nosetests --tests=system.stress:Stress.[insert|read]
# expects a Cassandra server to be running and listening on port 9160.
# (read tests expect insert tests to have run first too.)

try:
    from multiprocessing import Array as array, Process as Thread
    from uuid import uuid1 as get_ident
    Thread.isAlive = Thread.is_alive
except ImportError:
    from threading import Thread
    from thread import get_ident
    from array import array
from hashlib import md5
import time, random
from random import randint, gauss

from . import get_client, root, CassandraTester
from ttypes import *


TOTAL_KEYS = 1000**2
N_THREADS = 50
KEYS_PER_THREAD = TOTAL_KEYS / N_THREADS
COLUMNS_PER_KEY = 5
# this allows client to round robin requests directly for
# simple request load-balancing
NODES = ["localhost"]

# a generator that generates all keys according to a bell curve centered
# around the middle of the keys generated (0..TOTAL_KEYS).  Remember that
# about 68% of keys will be within STDEV away from the mean and 
# about 95% within 2*STDEV.
STDEV = TOTAL_KEYS * 0.3
MEAN = TOTAL_KEYS / 2
def key_generator():
    while True:
        guess = gauss(MEAN, STDEV)
        if 0 <= guess < TOTAL_KEYS:
            return int(guess)
    
# a generator that will generate all keys w/ equal probability.  this is the
# worst case for caching.
# key_generator = lambda: randint(0, TOTAL_KEYS - 1)

class Operation(Thread):
    def __init__(self, i, counts):
        Thread.__init__(self)
        self.range = xrange(KEYS_PER_THREAD * i, KEYS_PER_THREAD * (i + 1))
        self.idx = i
        self.counts = counts
        [hostname] = random.sample(NODES, 1)
        self.cclient = get_client(host=hostname,port=9160)
        self.cclient.transport.open()

class Inserter(Operation):
    def run(self):
        data = md5(str(get_ident())).hexdigest()
        columns = [Column(chr(ord('A') + j), data, 0) for j in xrange(COLUMNS_PER_KEY)]
        for i in self.range:
            key = str(i)
            cfmap = {'Standard1': [ColumnOrSuperColumn(column=c) for c in columns]}
            self.cclient.batch_insert('Keyspace1', key, cfmap, ConsistencyLevel.ONE)
            self.counts[self.idx]=self.counts[self.idx]+1

class Reader(Operation):
    def run(self):
        parent = ColumnParent('Standard1')
        p = SlicePredicate(slice_range=SliceRange('', '', False, COLUMNS_PER_KEY))
        for i in xrange(KEYS_PER_THREAD):
            key = str(key_generator())
            self.cclient.get_slice('Keyspace1', key, parent, p, ConsistencyLevel.ONE)
            self.counts[self.idx]=self.counts[self.idx]+1

class OperationFactory:
    @staticmethod
    def create(type,i,counts):
      if type == 'read':
        return Reader(i, counts)
      elif type == 'insert':
        return Inserter(i, counts)
      else:
        raise RuntimeError, 'Unsupported op!'

class Stress(CassandraTester):
    runserver = False
    counts = array('i', [0]*N_THREADS)

    def create_threads(self,type):
        threads = []
        for i in xrange(N_THREADS):
            th = OperationFactory.create(type,i, self.counts)
            threads.append(th)
            th.start()
        return threads

    def run_test(self,filename,threads):
        start_t = time.time()
        file(filename,'w').write('total,interval_op_rate,elapsed_time\n')
        total = old_total = 0
        while True:
            interval = 10
            time.sleep(interval)
            old_total = total
            total = sum(self.counts[th.idx] for th in threads)
            delta = total - old_total
            elapsed_t = int(time.time()-start_t)
            file(filename, 'a').write('%d,%d,%d\n' % (total, delta / interval,elapsed_t))
            if not [th for th in threads if th.isAlive()]:
                break

    def insert(self):
        threads = self.create_threads('insert')
        self.run_test("/tmp/progress_insert",threads);

    def read(self):
        threads = self.create_threads('read')
        self.run_test("/tmp/progress_reads",threads);
        
