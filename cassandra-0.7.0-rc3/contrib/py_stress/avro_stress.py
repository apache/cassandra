#!/usr/bin/python
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

# expects a Cassandra server to be running and listening on port 9160.
# (read tests expect insert tests to have run first too.)

have_multiproc = False
try:
    from multiprocessing import Array as array, Process as Thread
    from uuid import uuid1 as get_ident
    Thread.isAlive = Thread.is_alive
    have_multiproc = True
except ImportError:
    from threading import Thread
    from thread import get_ident
    from array import array
from hashlib import md5
import time, random, sys, os
from random import randint, gauss
from optparse import OptionParser

import avro.ipc as ipc
import avro.protocol as protocol
from avro.ipc import AvroRemoteException

L = os.path.abspath(__file__).split(os.path.sep)[:-3]
root = os.path.sep.join(L)


parser = OptionParser()
parser.add_option('-n', '--num-keys', type="int", dest="numkeys",
                  help="Number of keys", default=1000**2)
parser.add_option('-t', '--threads', type="int", dest="threads",
                  help="Number of threads/procs to use", default=50)
parser.add_option('-c', '--columns', type="int", dest="columns",
                  help="Number of columns per key", default=5)
parser.add_option('-d', '--nodes', type="string", dest="nodes",
                  help="Host nodes (comma separated)", default="localhost")
parser.add_option('-s', '--stdev', type="float", dest="stdev", default=0.1,
                  help="standard deviation factor")
parser.add_option('-r', '--random', action="store_true", dest="random",
                  help="use random key generator (stdev will have no effect)")
parser.add_option('-f', '--file', type="string", dest="file", 
                  help="write output to file")
parser.add_option('-p', '--port', type="int", default=9160, dest="port",
                  help="thrift port")
parser.add_option('-m', '--unframed', action="store_true", dest="unframed",
                  help="use unframed transport")
parser.add_option('-o', '--operation', type="choice", dest="operation",
                  default="insert", choices=('insert', 'read', 'rangeslice'),
                  help="operation to perform")
parser.add_option('-u', '--supercolumns', type="int", dest="supers", default=1,
                  help="number of super columns per key")
parser.add_option('-y', '--family-type', type="choice", dest="cftype",
                  choices=('regular','super'), default='regular',
                  help="column family type")
parser.add_option('-k', '--keep-going', action="store_true", dest="ignore",
                  help="ignore errors inserting or reading")
parser.add_option('-i', '--progress-interval', type="int", default=10,
                  dest="interval", help="progress report interval (seconds)")
parser.add_option('-g', '--get-range-slice-count', type="int", default=1000,
                  dest="rangecount",
                  help="amount of keys to get_range_slices per call")
parser.add_option('-l', '--replication-factor', type="int", default=1,
                  dest="replication",
                  help="replication factor to use when creating needed column families")
parser.add_option('-e', '--consistency-level', type="str", default='ONE',
                  dest="consistency", help="consistency level to use")

(options, args) = parser.parse_args()
 
total_keys = options.numkeys
n_threads = options.threads
keys_per_thread = total_keys / n_threads
columns_per_key = options.columns
supers_per_key = options.supers
# this allows client to round robin requests directly for
# simple request load-balancing
nodes = options.nodes.split(',')

# a generator that generates all keys according to a bell curve centered
# around the middle of the keys generated (0..total_keys).  Remember that
# about 68% of keys will be within stdev away from the mean and 
# about 95% within 2*stdev.
stdev = total_keys * options.stdev
mean = total_keys / 2

c_levels = ['ZERO', 'ANY', 'ONE', 'QUORUM', 'DCQUORUM', 'DCQUORUMSYNC', 'ALL']
consistency = options.consistency
if not consistency in c_levels:
    print "%s is not a valid consistency level" % options.consistency
    sys.exit(3)

def key_generator_gauss():
    fmt = '%0' + str(len(str(total_keys))) + 'd'
    while True:
        guess = gauss(mean, stdev)
        if 0 <= guess < total_keys:
            return fmt % int(guess)
    
# a generator that will generate all keys w/ equal probability.  this is the
# worst case for caching.
def key_generator_random():
    fmt = '%0' + str(len(str(total_keys))) + 'd'
    return fmt % randint(0, total_keys - 1)

key_generator = key_generator_gauss
if options.random:
    key_generator = key_generator_random

def get_client(host='127.0.0.1', port=9170):
    schema = os.path.join(root, 'interface/avro', 'cassandra.avpr')
    proto = protocol.parse(open(schema).read())
    client = ipc.HTTPTransceiver(host, port)
    return ipc.Requestor(proto, client)

def make_keyspaces():
    keyspace1 = dict()
    keyspace1['name'] = 'Keyspace1'
    keyspace1['replication_factor'] = options.replication
    keyspace1['strategy_class'] = 'org.apache.cassandra.locator.SimpleStrategy'

    keyspace1['cf_defs'] = [{
        'keyspace': 'Keyspace1',
        'name': 'Standard1',
    }]

    keyspace1['cf_defs'].append({
        'keyspace': 'Keyspace1',
        'name': 'Super1',
        'column_type': 'Super',
        'comparator_type': 'BytesType',
        'subcomparator_type': 'BytesType',
    })
    client = get_client(nodes[0], options.port)
    try:
        client.request('system_add_keyspace', {'ks_def': keyspace1})
    except AvroRemoteException, e:
        print e
    client.transceiver.conn.close()

class Operation(Thread):
    def __init__(self, i, opcounts, keycounts, latencies):
        Thread.__init__(self)
        # generator of the keys to be used
        self.range = xrange(keys_per_thread * i, keys_per_thread * (i + 1))
        # we can't use a local counter, since that won't be visible to the parent
        # under multiprocessing.  instead, the parent passes a "opcounts" array
        # and an index that is our assigned counter.
        self.idx = i
        self.opcounts = opcounts
        # similarly, a shared array for latency and key totals
        self.latencies = latencies
        self.keycounts = keycounts
        # random host for pseudo-load-balancing
        [hostname] = random.sample(nodes, 1)
        # open client
        self.cclient = get_client(hostname, options.port)
        self.cclient.request('set_keyspace', {'keyspace': 'Keyspace1'})

class Inserter(Operation):
    def run(self):
        data = md5(str(get_ident())).hexdigest()
        columns = [{'name': 'C' + str(j), 'value': data, 'timestamp': int(time.time() * 1000000)} for j in xrange(columns_per_key)]
        fmt = '%0' + str(len(str(total_keys))) + 'd'
        if 'super' == options.cftype:
            supers = [{'name': 'S' + str(j), 'columns': columns} for j in xrange(supers_per_key)]
        for i in self.range:
            key = fmt % i
            if 'super' == options.cftype:
                cfmap= {'key': key, 'mutations': {'Super1' : [{'column_or_supercolumn': {'super_column': s}} for s in supers]}}
            else:
                cfmap = {'key': key, 'mutations': {'Standard1': [{'column_or_supercolumn': {'column': c}} for c in columns]}}
            start = time.time()
            try:
                self.cclient.request('batch_mutate', {'mutation_map': [cfmap], 'consistency_level': consistency})
            except KeyboardInterrupt:
                raise
            except Exception, e:
                if options.ignore:
                    print e
                else:
                    raise
            self.latencies[self.idx] += time.time() - start
            self.opcounts[self.idx] += 1
            self.keycounts[self.idx] += 1


class Reader(Operation):
    def run(self):
        p = {'slice_range': {'start': '', 'finish': '', 'reversed': False, 'count': columns_per_key}}
        if 'super' == options.cftype:
            for i in xrange(keys_per_thread):
                key = key_generator()
                for j in xrange(supers_per_key):
                    parent = {'column_family': 'Super1', 'super_column': 'S' + str(j)}
                    start = time.time()
                    try:
                        r = self.cclient.request('get_slice', {'key': key, 'column_parent': parent, 'predicate': p, 'consistency_level': consistency})
                        if not r: raise RuntimeError("Key %s not found" % key)
                    except KeyboardInterrupt:
                        raise
                    except Exception, e:
                        if options.ignore:
                            print e
                        else:
                            raise
                    self.latencies[self.idx] += time.time() - start
                    self.opcounts[self.idx] += 1
                    self.keycounts[self.idx] += 1
        else:
            parent = {'column_family': 'Standard1'}
            for i in xrange(keys_per_thread):
                key = key_generator()
                start = time.time()
                try:
                    r = self.cclient.request('get_slice', {'key': key, 'column_parent': parent, 'predicate': p, 'consistency_level': consistency})
                    if not r: raise RuntimeError("Key %s not found" % key)
                except KeyboardInterrupt:
                    raise
                except Exception, e:
                    if options.ignore:
                        print e
                    else:
                        raise
                self.latencies[self.idx] += time.time() - start
                self.opcounts[self.idx] += 1
                self.keycounts[self.idx] += 1

class RangeSlicer(Operation):
    def run(self):
        begin = self.range[0]
        end = self.range[-1]
        current = begin
        last = current + options.rangecount
        fmt = '%0' + str(len(str(total_keys))) + 'd'
        p = {'slice_range': {'start': '', 'finish': '', 'reversed': False, 'count': columns_per_key}}
        if 'super' == options.cftype:
            while current < end:
                keyrange = {'start_key': fmt % current, 'end_key': fmt % last, 'count': options.rangecount}
                res = []
                for j in xrange(supers_per_key):
                    parent = {'column_family': 'Super1', 'super_column': 'S' + str(j)} 
                    begin = time.time()
                    try:
                        res = self.cclient.request('get_range_slices', {'column_parent': parent, 'predicate': p, 'range': keyrange, 'consistency_level': consistency})
                        if not res: raise RuntimeError("Key %s not found" % key)
                    except KeyboardInterrupt:
                        raise
                    except Exception, e:
                        if options.ignore:
                            print e
                        else:
                            raise
                    self.latencies[self.idx] += time.time() - begin
                    self.opcounts[self.idx] += 1
                current += len(r) + 1
                last = current + len(r) + 1
                self.keycounts[self.idx] += len(r)
        else:
            parent = {'column_family': 'Standard1'}
            while current < end:
                start = fmt % current 
                finish = fmt % last
                keyrange = {'start_key': start, 'end_key': finish, 'count': options.rangecount}
                begin = time.time()
                try:
                    r = self.cclient.request('get_range_slices', {'column_parent': parent, 'predicate': p, 'range': keyrange, 'consistency_level': consistency})
                    if not r: raise RuntimeError("Range not found:", start, finish)
                except KeyboardInterrupt:
                    raise
                except Exception, e:
                    if options.ignore:
                        print e
                    else:
                        print start, finish
                        raise
                current += len(r) + 1
                last = current + len(r)  + 1
                self.latencies[self.idx] += time.time() - begin
                self.opcounts[self.idx] += 1
                self.keycounts[self.idx] += len(r)


class OperationFactory:
    @staticmethod
    def create(type, i, opcounts, keycounts, latencies):
        if type == 'read':
            return Reader(i, opcounts, keycounts, latencies)
        elif type == 'insert':
            return Inserter(i, opcounts, keycounts, latencies)
        elif type == 'rangeslice':
            return RangeSlicer(i, opcounts, keycounts, latencies)
        else:
            raise RuntimeError, 'Unsupported op!'


class Stress(object):
    opcounts = array('i', [0] * n_threads)
    latencies = array('d', [0] * n_threads)
    keycounts = array('i', [0] * n_threads)

    def create_threads(self,type):
        threads = []
        for i in xrange(n_threads):
            th = OperationFactory.create(type, i, self.opcounts, self.keycounts, self.latencies)
            threads.append(th)
            th.start()
        return threads

    def run_test(self,filename,threads):
        start_t = time.time()
        if filename:
            outf = open(filename,'w')
        else:
            outf = sys.stdout
        outf.write('total,interval_op_rate,interval_key_rate,avg_latency,elapsed_time\n')
        epoch = total = old_total = latency = keycount = old_keycount = old_latency = 0
        epoch_intervals = (options.interval * 10) # 1 epoch = 1 tenth of a second
        terminate = False
        while not terminate:
            time.sleep(0.1)
            if not [th for th in threads if th.isAlive()]:
                terminate = True
            epoch = epoch + 1
            if terminate or epoch > epoch_intervals:
                epoch = 0
                old_total, old_latency, old_keycount = total, latency, keycount
                total = sum(self.opcounts[th.idx] for th in threads)
                latency = sum(self.latencies[th.idx] for th in threads)
                keycount = sum(self.keycounts[th.idx] for th in threads)
                opdelta = total - old_total
                keydelta = keycount - old_keycount
                delta_latency = latency - old_latency
                if opdelta > 0:
                    delta_formatted = (delta_latency / opdelta)
                else:
                    delta_formatted = 'NaN'
                elapsed_t = int(time.time() - start_t)
                outf.write('%d,%d,%d,%s,%d\n' 
                           % (total, opdelta / options.interval, keydelta / options.interval, delta_formatted, elapsed_t))

    def insert(self):
        threads = self.create_threads('insert')
        self.run_test(options.file,threads);

    def read(self):
        threads = self.create_threads('read')
        self.run_test(options.file,threads);
        
    def rangeslice(self):
        threads = self.create_threads('rangeslice')
        self.run_test(options.file,threads);

stresser = Stress()
benchmark = getattr(stresser, options.operation, None)
if not have_multiproc:
    print """WARNING: multiprocessing not present, threading will be used.
        Benchmark may not be accurate!"""
if options.operation == 'insert':
    make_keyspaces()
benchmark()
