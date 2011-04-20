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

from __future__ import with_statement

have_multiproc = False
try:
    from multiprocessing import Array as array, Process as Thread
    from uuid import uuid1 as get_ident
    array('i', 1) # catch "This platform lacks a functioning sem_open implementation"
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

from thrift.transport import TTransport
from thrift.transport import TSocket
from thrift.transport import THttpClient
from thrift.protocol import TBinaryProtocol

try:
    from cassandra import Cassandra
    from cassandra.ttypes import *
except ImportError:
    # add cassandra directory to sys.path
    L = os.path.abspath(__file__).split(os.path.sep)[:-3]
    root = os.path.sep.join(L)
    _ipath = os.path.join(root, 'interface', 'thrift', 'gen-py')
    sys.path.append(os.path.join(_ipath, 'cassandra'))
    import Cassandra
    from ttypes import *
except ImportError:
    print "Cassandra thrift bindings not found, please run 'ant gen-thrift-py'"
    sys.exit(2)

try:
    from thrift.protocol import fastbinary
except ImportError:
    print "WARNING: thrift binary extension not found, benchmark will not be accurate!"

parser = OptionParser()
parser.add_option('-n', '--num-keys', type="int", dest="numkeys",
                  help="Number of keys", default=1000**2)
parser.add_option('-N', '--skip-keys', type="float", dest="skipkeys",
                  help="Fraction of keys to skip initially", default=0)
parser.add_option('-t', '--threads', type="int", dest="threads",
                  help="Number of threads/procs to use", default=50)
parser.add_option('-c', '--columns', type="int", dest="columns",
                  help="Number of columns per key", default=5)
parser.add_option('-S', '--column-size', type="int", dest="column_size",
                  help="Size of column values in bytes", default=34)
parser.add_option('-C', '--cardinality', type="int", dest="cardinality",
                  help="Number of unique values stored in columns", default=50)
parser.add_option('-d', '--nodes', type="string", dest="nodes",
                  help="Host nodes (comma separated)", default="localhost")
parser.add_option('-D', '--nodefile', type="string", dest="nodefile",
                  help="File containing list of nodes (one per line)", default=None)
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
                  default="insert", choices=('insert', 'read', 'rangeslice',
                  'indexedrangeslice', 'multiget'),
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
parser.add_option('-g', '--keys-per-call', type="int", default=1000,
                  dest="rangecount",
                  help="amount of keys to get_range_slices or multiget per call")
parser.add_option('-l', '--replication-factor', type="int", default=1,
                  dest="replication",
                  help="replication factor to use when creating needed column families")
parser.add_option('-e', '--consistency-level', type="str", default='ONE',
                  dest="consistency", help="consistency level to use")
parser.add_option('-x', '--create-index', type="choice",
                  choices=('keys','keys_bitmap', 'none'), default='none',
                  dest="index", help="type of index to create on needed column families")

(options, args) = parser.parse_args()
 
total_keys = options.numkeys
n_threads = options.threads
keys_per_thread = total_keys / n_threads
columns_per_key = options.columns
supers_per_key = options.supers
# this allows client to round robin requests directly for
# simple request load-balancing
nodes = options.nodes.split(',')
if options.nodefile != None:
    with open(options.nodefile) as f:
        nodes = [n.strip() for n in f.readlines() if len(n.strip()) > 0]

#format string for keys
fmt = '%0' + str(len(str(total_keys))) + 'd'

# a generator that generates all keys according to a bell curve centered
# around the middle of the keys generated (0..total_keys).  Remember that
# about 68% of keys will be within stdev away from the mean and 
# about 95% within 2*stdev.
stdev = total_keys * options.stdev
mean = total_keys / 2

consistency = getattr(ConsistencyLevel, options.consistency, None)
if consistency is None:
    print "%s is not a valid consistency level" % options.consistency
    sys.exit(3)

# generates a list of unique, deterministic values
def generate_values():
    values = []
    for i in xrange(0, options.cardinality):
        h = md5(str(i)).hexdigest()
        values.append(h * int(options.column_size/len(h)) + h[:options.column_size % len(h)])
    return values

def key_generator_gauss():
    while True:
        guess = gauss(mean, stdev)
        if 0 <= guess < total_keys:
            return fmt % int(guess)
    
# a generator that will generate all keys w/ equal probability.  this is the
# worst case for caching.
def key_generator_random():
    return fmt % randint(0, total_keys - 1)

key_generator = key_generator_gauss
if options.random:
    key_generator = key_generator_random


def get_client(host='127.0.0.1', port=9160):
    socket = TSocket.TSocket(host, port)
    if options.unframed:
        transport = TTransport.TBufferedTransport(socket)
    else:
        transport = TTransport.TFramedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
    client = Cassandra.Client(protocol)
    client.transport = transport
    return client

def make_keyspaces():
    colms = []
    if options.index == 'keys':
        colms = [ColumnDef(name='C1', validation_class='UTF8Type', index_type=IndexType.KEYS)]
    elif options.index == 'keys_bitmap':
        colms = [ColumnDef(name='C1', validation_class='UTF8Type', index_type=IndexType.KEYS_BITMAP)]
    cfams = [CfDef(keyspace='Keyspace1', name='Standard1', column_metadata=colms),
             CfDef(keyspace='Keyspace1', name='Super1', column_type='Super')]
    keyspace = KsDef(name='Keyspace1',
                     strategy_class='org.apache.cassandra.locator.SimpleStrategy',
                     strategy_options={'replication_factor': str(options.replication)}, 
                     cf_defs=cfams)
    client = get_client(nodes[0], options.port)
    client.transport.open()
    try:
        client.system_add_keyspace(keyspace)
        print "Created keyspaces.  Sleeping %ss for propagation." % len(nodes)
        time.sleep(len(nodes))
    except InvalidRequestException, e:
        print e.why
    client.transport.close()

class Operation(Thread):
    def __init__(self, i, opcounts, keycounts, latencies):
        Thread.__init__(self)
        # generator of the keys to be used
        self.range = xrange(int(keys_per_thread * (i + options.skipkeys)), 
                            keys_per_thread * (i + 1))
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
        self.cclient.transport.open()
        self.cclient.set_keyspace('Keyspace1')

class Inserter(Operation):
    def run(self):
        values = generate_values()
        columns = [Column('C' + str(j), 'unset', time.time() * 1000000) for j in xrange(columns_per_key)]
        if 'super' == options.cftype:
            supers = [SuperColumn('S' + str(j), columns) for j in xrange(supers_per_key)]
        for i in self.range:
            key = fmt % i
            if 'super' == options.cftype:
                cfmap= {key: {'Super1' : [Mutation(ColumnOrSuperColumn(super_column=s)) for s in supers]}}
            else:
                cfmap = {key: {'Standard1': [Mutation(ColumnOrSuperColumn(column=c)) for c in columns]}}
            # set the correct column values for this row
            value = values[i % len(values)]
            for column in columns:
                column.value = value
            start = time.time()
            try:
                self.cclient.batch_mutate(cfmap, consistency)
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
        p = SlicePredicate(slice_range=SliceRange('', '', False, columns_per_key))
        if 'super' == options.cftype:
            for i in xrange(keys_per_thread):
                key = key_generator()
                for j in xrange(supers_per_key):
                    parent = ColumnParent('Super1', 'S' + str(j))
                    start = time.time()
                    try:
                        r = self.cclient.get_slice(key, parent, p, consistency)
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
            parent = ColumnParent('Standard1')
            for i in xrange(keys_per_thread):
                key = key_generator()
                start = time.time()
                try:
                    r = self.cclient.get_slice(key, parent, p, consistency)
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
        p = SlicePredicate(slice_range=SliceRange('', '', False, columns_per_key))
        if 'super' == options.cftype:
            while current < end:
                keyrange = KeyRange(fmt % current, fmt % last, count = options.rangecount)
                res = []
                for j in xrange(supers_per_key):
                    parent = ColumnParent('Super1', 'S' + str(j)) 
                    begin = time.time()
                    try:
                        res = self.cclient.get_range_slices(parent, p, keyrange, consistency)
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
            parent = ColumnParent('Standard1')
            while current < end:
                start = fmt % current 
                finish = fmt % last
                keyrange = KeyRange(start, finish, count = options.rangecount)
                begin = time.time()
                try:
                    r = self.cclient.get_range_slices(parent, p, keyrange, consistency)
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

# Each thread queries for a portion of the unique values
# TODO: all threads start at the same key: implement wrapping, and start
# from the thread's appointed range
class IndexedRangeSlicer(Operation):
    def run(self):
        p = SlicePredicate(slice_range=SliceRange('', '', False, columns_per_key))
        values = generate_values()
        parent = ColumnParent('Standard1')
        # the number of rows with a particular value and the number of values we should query for
        expected_per_value = total_keys // len(values)
        valuebegin = self.range[0] // expected_per_value
        valuecount = len(self.range) // expected_per_value
        for valueidx in xrange(valuebegin, valuebegin + valuecount):
            received = 0
            start = fmt % 0
            value = values[valueidx % len(values)]
            expressions = [IndexExpression(column_name='C1', op=IndexOperator.EQ, value=value)]
            while received < expected_per_value:
                clause = IndexClause(start_key=start, count=options.rangecount, expressions=expressions)
                begin = time.time()
                try:
                    r = self.cclient.get_indexed_slices(parent, clause, p, consistency)
                    if not r: raise RuntimeError("No indexed values from offset received:", start)
                except KeyboardInterrupt:
                    raise
                except Exception, e:
                    if options.ignore:
                        print e
                        continue
                    else:
                        raise
                received += len(r)
                # convert max key found back to an integer, and increment it
                start = fmt % (1 + max([int(keyslice.key) for keyslice in r]))
                self.latencies[self.idx] += time.time() - begin
                self.opcounts[self.idx] += 1
                self.keycounts[self.idx] += len(r)


class MultiGetter(Operation):
    def run(self):
        p = SlicePredicate(slice_range=SliceRange('', '', False, columns_per_key))
        offset = self.idx * keys_per_thread
        count = (((self.idx+1) * keys_per_thread) - offset) / options.rangecount
        if 'super' == options.cftype:
            for x in xrange(count):
                keys = [key_generator() for i in xrange(offset, offset + options.rangecount)]
                for j in xrange(supers_per_key):
                    parent = ColumnParent('Super1', 'S' + str(j))
                    start = time.time()
                    try:
                        r = self.cclient.multiget_slice(keys, parent, p, consistency)
                        if not r: raise RuntimeError("Keys %s not found" % keys)
                    except KeyboardInterrupt:
                        raise
                    except Exception, e:
                        if options.ignore:
                            print e
                        else:
                            raise
                    self.latencies[self.idx] += time.time() - start
                    self.opcounts[self.idx] += 1
                    self.keycounts[self.idx] += len(keys)
                    offset += options.rangecount
        else:
            parent = ColumnParent('Standard1')
            for x in xrange(count):
                keys = [key_generator() for i in xrange(offset, offset + options.rangecount)]
                start = time.time()
                try:
                    r = self.cclient.multiget_slice(keys, parent, p, consistency)
                    if not r: raise RuntimeError("Keys %s not found" % keys)
                except KeyboardInterrupt:
                    raise
                except Exception, e:
                    if options.ignore:
                        print e
                    else:
                        raise
                self.latencies[self.idx] += time.time() - start
                self.opcounts[self.idx] += 1
                self.keycounts[self.idx] += len(keys)
                offset += options.rangecount


class OperationFactory:
    @staticmethod
    def create(type, i, opcounts, keycounts, latencies):
        if type == 'read':
            return Reader(i, opcounts, keycounts, latencies)
        elif type == 'insert':
            return Inserter(i, opcounts, keycounts, latencies)
        elif type == 'rangeslice':
            return RangeSlicer(i, opcounts, keycounts, latencies)
        elif type == 'indexedrangeslice':
            return IndexedRangeSlicer(i, opcounts, keycounts, latencies)
        elif type == 'multiget':
            return MultiGetter(i, opcounts, keycounts, latencies)
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

    def indexedrangeslice(self):
        threads = self.create_threads('indexedrangeslice')
        self.run_test(options.file,threads);

    def multiget(self):
        threads = self.create_threads('multiget')
        self.run_test(options.file,threads);

stresser = Stress()
benchmark = getattr(stresser, options.operation, None)
if not have_multiproc:
    print """WARNING: multiprocessing not present, threading will be used.
        Benchmark may not be accurate!"""
if options.operation == 'insert':
    make_keyspaces()
benchmark()
