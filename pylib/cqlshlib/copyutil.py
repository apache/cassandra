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

import csv
import json
import multiprocessing as mp
import os
import Queue
import random
import re
import struct
import sys
import time
import traceback

from calendar import timegm
from collections import defaultdict, deque, namedtuple
from decimal import Decimal
from random import randrange
from StringIO import StringIO
from threading import Lock
from uuid import UUID

from cassandra.cluster import Cluster
from cassandra.cqltypes import ReversedType, UserType
from cassandra.metadata import protect_name, protect_names
from cassandra.policies import RetryPolicy, WhiteListRoundRobinPolicy, TokenAwarePolicy, DCAwareRoundRobinPolicy
from cassandra.query import BatchStatement, BatchType, SimpleStatement, tuple_factory
from cassandra.util import Date, Time

from cql3handling import CqlRuleSet
from displaying import NO_COLOR_MAP
from formatting import format_value_default, EMPTY, get_formatter
from sslhandling import ssl_settings


def parse_options(shell, opts):
    """
    Parse options for import (COPY FROM) and export (COPY TO) operations.
    Extract from opts csv and dialect options.

    :return: 3 dictionaries: the csv options, the dialect options, any unrecognized options.
    """
    dialect_options = shell.csv_dialect_defaults.copy()
    if 'quote' in opts:
        dialect_options['quotechar'] = opts.pop('quote')
    if 'escape' in opts:
        dialect_options['escapechar'] = opts.pop('escape')
    if 'delimiter' in opts:
        dialect_options['delimiter'] = opts.pop('delimiter')
    if dialect_options['quotechar'] == dialect_options['escapechar']:
        dialect_options['doublequote'] = True
        del dialect_options['escapechar']

    csv_options = dict()
    csv_options['nullval'] = opts.pop('null', '')
    csv_options['header'] = bool(opts.pop('header', '').lower() == 'true')
    csv_options['encoding'] = opts.pop('encoding', 'utf8')
    csv_options['maxrequests'] = int(opts.pop('maxrequests', 6))
    csv_options['pagesize'] = int(opts.pop('pagesize', 1000))
    # by default the page timeout is 10 seconds per 1000 entries in the page size or 10 seconds if pagesize is smaller
    csv_options['pagetimeout'] = int(opts.pop('pagetimeout', max(10, 10 * (csv_options['pagesize'] / 1000))))
    csv_options['maxattempts'] = int(opts.pop('maxattempts', 5))
    csv_options['dtformats'] = opts.pop('timeformat', shell.display_time_format)
    csv_options['float_precision'] = shell.display_float_precision
    csv_options['chunksize'] = int(opts.pop('chunksize', 1000))
    csv_options['ingestrate'] = int(opts.pop('ingestrate', 100000))
    csv_options['maxbatchsize'] = int(opts.pop('maxbatchsize', 20))
    csv_options['minbatchsize'] = int(opts.pop('minbatchsize', 2))
    csv_options['reportfrequency'] = float(opts.pop('reportfrequency', 0.25))

    return csv_options, dialect_options, opts


def get_num_processes(cap):
    """
    Pick a reasonable number of child processes. We need to leave at
    least one core for the parent process.  This doesn't necessarily
    need to be capped, but 4 is currently enough to keep
    a single local Cassandra node busy so we use this for import, whilst
    for export we use 16 since we can connect to multiple Cassandra nodes.
    Eventually this parameter will become an option.
    """
    try:
        return max(1, min(cap, mp.cpu_count() - 1))
    except NotImplementedError:
        return 1


class CopyTask(object):
    """
    A base class for ImportTask and ExportTask
    """
    def __init__(self, shell, ks, cf, columns, fname, csv_options, dialect_options, protocol_version, config_file):
        self.shell = shell
        self.csv_options = csv_options
        self.dialect_options = dialect_options
        self.ks = ks
        self.cf = cf
        self.columns = shell.get_column_names(ks, cf) if columns is None else columns
        self.fname = fname
        self.protocol_version = protocol_version
        self.config_file = config_file

        self.processes = []
        self.inmsg = mp.Queue()
        self.outmsg = mp.Queue()

    def close(self):
        for process in self.processes:
            process.terminate()

        self.inmsg.close()
        self.outmsg.close()

    def num_live_processes(self):
        return sum(1 for p in self.processes if p.is_alive())

    def make_params(self):
        """
        Return a dictionary of parameters to be used by the worker processes.
        On Windows this dictionary must be pickle-able.

        inmsg is the message queue flowing from parent to child process, so outmsg from the parent point
        of view and, vice-versa,  outmsg is the message queue flowing from child to parent, so inmsg
        from the parent point of view, hence the two are swapped below.
        """
        shell = self.shell
        return dict(inmsg=self.outmsg,  # see comment above
                    outmsg=self.inmsg,  # see comment above
                    ks=self.ks,
                    cf=self.cf,
                    columns=self.columns,
                    csv_options=self.csv_options,
                    dialect_options=self.dialect_options,
                    consistency_level=shell.consistency_level,
                    connect_timeout=shell.conn.connect_timeout,
                    hostname=shell.hostname,
                    port=shell.port,
                    ssl=shell.ssl,
                    auth_provider=shell.auth_provider,
                    cql_version=shell.conn.cql_version,
                    config_file=self.config_file,
                    protocol_version=self.protocol_version,
                    debug=shell.debug
                    )


class ExportTask(CopyTask):
    """
    A class that exports data to .csv by instantiating one or more processes that work in parallel (ExportProcess).
    """

    def run(self):
        """
        Initiates the export by creating the processes.
        """
        shell = self.shell
        fname = self.fname

        if fname is None:
            do_close = False
            csvdest = sys.stdout
        else:
            do_close = True
            try:
                csvdest = open(fname, 'wb')
            except IOError, e:
                shell.printerr("Can't open %r for writing: %s" % (fname, e))
                return 0

        if self.csv_options['header']:
            writer = csv.writer(csvdest, **self.dialect_options)
            writer.writerow(self.columns)

        ranges = self.get_ranges()
        num_processes = get_num_processes(cap=min(16, len(ranges)))
        params = self.make_params()

        for i in xrange(num_processes):
            self.processes.append(ExportProcess(params))

        for process in self.processes:
            process.start()

        try:
            return self.check_processes(csvdest, ranges)
        finally:
            self.close()
            if do_close:
                csvdest.close()

    def get_ranges(self):
        """
        return a queue of tuples, where the first tuple entry is a token range (from, to]
        and the second entry is a list of hosts that own that range. Each host is responsible
        for all the tokens in the rage (from, to].

        The ring information comes from the driver metadata token map, which is built by
        querying System.PEERS.

        We only consider replicas that are in the local datacenter. If there are no local replicas
        we use the cqlsh session host.
        """
        shell = self.shell
        hostname = shell.hostname
        ranges = dict()

        def make_range(hosts):
            return {'hosts': tuple(hosts), 'attempts': 0, 'rows': 0}

        min_token = self.get_min_token()
        if shell.conn.metadata.token_map is None or min_token is None:
            ranges[(None, None)] = make_range([hostname])
            return ranges

        local_dc = shell.conn.metadata.get_host(hostname).datacenter
        ring = shell.get_ring(self.ks).items()
        ring.sort()

        previous_previous = None
        previous = None
        for token, replicas in ring:
            if previous is None and token.value == min_token:
                continue  # avoids looping entire ring

            hosts = []
            for host in replicas:
                if host.is_up and host.datacenter == local_dc:
                    hosts.append(host.address)
            if not hosts:
                hosts.append(hostname)  # fallback to default host if no replicas in current dc
            ranges[(previous, token.value)] = make_range(hosts)
            previous_previous = previous
            previous = token.value

        #  If the ring is empty we get the entire ring from the
        #  host we are currently connected to, otherwise for the last ring interval
        #  we query the same replicas that hold the last token in the ring
        if not ranges:
            ranges[(None, None)] = make_range([hostname])
        else:
            ranges[(previous, None)] = ranges[(previous_previous, previous)].copy()

        return ranges

    def get_min_token(self):
        """
        :return the minimum token, which depends on the partitioner.
        For partitioners that do not support tokens we return None, in
        this cases we will not work in parallel, we'll just send all requests
        to the cqlsh session host.
        """
        partitioner = self.shell.conn.metadata.partitioner

        if partitioner.endswith('RandomPartitioner'):
            return -1
        elif partitioner.endswith('Murmur3Partitioner'):
            return -(2 ** 63)   # Long.MIN_VALUE in Java
        else:
            return None

    def send_work(self, ranges, tokens_to_send):
        for token_range in tokens_to_send:
            self.outmsg.put((token_range, ranges[token_range]))
            ranges[token_range]['attempts'] += 1

    def check_processes(self, csvdest, ranges):
        """
        Here we monitor all child processes by collecting their results
        or any errors. We terminate when we have processed all the ranges or when there
        are no more processes.
        """
        shell = self.shell
        processes = self.processes
        meter = RateMeter(update_interval=self.csv_options['reportfrequency'])
        total_requests = len(ranges)
        max_attempts = self.csv_options['maxattempts']

        self.send_work(ranges, ranges.keys())

        num_processes = len(processes)
        succeeded = 0
        failed = 0
        while (failed + succeeded) < total_requests and self.num_live_processes() == num_processes:
            try:
                token_range, result = self.inmsg.get(timeout=1.0)
                if token_range is None and result is None:  # a request has finished
                    succeeded += 1
                elif isinstance(result, Exception):  # an error occurred
                    if token_range is None:  # the entire process failed
                        shell.printerr('Error from worker process: %s' % (result))
                    else:   # only this token_range failed, retry up to max_attempts if no rows received yet,
                            # if rows are receive we risk duplicating data, there is a back-off policy in place
                            # in the worker process as well, see ExpBackoffRetryPolicy
                        if ranges[token_range]['attempts'] < max_attempts and ranges[token_range]['rows'] == 0:
                            shell.printerr('Error for %s: %s (will try again later attempt %d of %d)'
                                           % (token_range, result, ranges[token_range]['attempts'], max_attempts))
                            self.send_work(ranges, [token_range])
                        else:
                            shell.printerr('Error for %s: %s (permanently given up after %d rows and %d attempts)'
                                           % (token_range, result, ranges[token_range]['rows'],
                                              ranges[token_range]['attempts']))
                            failed += 1
                else:  # partial result received
                    data, num = result
                    csvdest.write(data)
                    meter.increment(n=num)
                    ranges[token_range]['rows'] += num
            except Queue.Empty:
                pass

        if self.num_live_processes() < len(processes):
            for process in processes:
                if not process.is_alive():
                    shell.printerr('Child process %d died with exit code %d' % (process.pid, process.exitcode))

        if succeeded < total_requests:
            shell.printerr('Exported %d ranges out of %d total ranges, some records might be missing'
                           % (succeeded, total_requests))

        return meter.get_total_records()


class ImportReader(object):
    """
    A wrapper around a csv reader to keep track of when we have
    exhausted reading input records.
    """
    def __init__(self, linesource, chunksize, dialect_options):
        self.linesource = linesource
        self.chunksize = chunksize
        self.reader = csv.reader(linesource, **dialect_options)
        self.exhausted = False

    def read_rows(self):
        if self.exhausted:
            return []

        rows = list(next(self.reader) for _ in xrange(self.chunksize))
        self.exhausted = len(rows) < self.chunksize
        return rows


class ImportTask(CopyTask):
    """
    A class to import data from .csv by instantiating one or more processes
    that work in parallel (ImportProcess).
    """
    def __init__(self, shell, ks, cf, columns, fname, csv_options, dialect_options, protocol_version, config_file):
        CopyTask.__init__(self, shell, ks, cf, columns, fname,
                          csv_options, dialect_options, protocol_version, config_file)

        self.num_processes = get_num_processes(cap=4)
        self.chunk_size = csv_options['chunksize']
        self.ingest_rate = csv_options['ingestrate']
        self.max_attempts = csv_options['maxattempts']
        self.header = self.csv_options['header']
        self.table_meta = self.shell.get_table_meta(self.ks, self.cf)
        self.batch_id = 0
        self.receive_meter = RateMeter(update_interval=csv_options['reportfrequency'])
        self.send_meter = RateMeter(update_interval=1, log=False)
        self.retries = deque([])
        self.failed = 0
        self.succeeded = 0
        self.sent = 0

    def run(self):
        shell = self.shell

        if self.fname is None:
            do_close = False
            print "[Use \. on a line by itself to end input]"
            linesource = shell.use_stdin_reader(prompt='[copy] ', until=r'\.')
        else:
            do_close = True
            try:
                linesource = open(self.fname, 'rb')
            except IOError, e:
                shell.printerr("Can't open %r for reading: %s" % (self.fname, e))
                return 0

        try:
            if self.header:
                linesource.next()

            reader = ImportReader(linesource, self.chunk_size, self.dialect_options)
            params = self.make_params()

            for i in range(self.num_processes):
                self.processes.append(ImportProcess(params))

            for process in self.processes:
                process.start()

            return self.process_records(reader)

        except Exception, exc:
            shell.printerr(str(exc))
            if shell.debug:
                traceback.print_exc()
            return 0
        finally:
            self.close()
            if do_close:
                linesource.close()
            elif shell.tty:
                print

    def process_records(self, reader):
        """
        Keep on running until we have stuff to receive or send and until all processes are running.
        Send data (batches or retries) up to the max ingest rate. If we are waiting for stuff to
        receive check the incoming queue.
        """
        while (self.has_more_to_send(reader) or self.has_more_to_receive()) and self.all_processes_running():
            if self.has_more_to_send(reader):
                if self.send_meter.current_record <= self.ingest_rate:
                    self.send_batches(reader)
                else:
                    self.send_meter.maybe_update()

            if self.has_more_to_receive():
                self.receive()

        if self.succeeded < self.sent:
            self.shell.printerr("Failed to process %d batches" % (self.sent - self.succeeded))

        return self.receive_meter.get_total_records()

    def has_more_to_receive(self):
        return (self.succeeded + self.failed) < self.sent

    def has_more_to_send(self, reader):
        return (not reader.exhausted) or self.retries

    def all_processes_running(self):
        return self.num_live_processes() == self.num_processes

    def receive(self):
        shell = self.shell
        start_time = time.time()

        while time.time() - start_time < 0.01:  # 10 millis
            try:
                batch, err = self.inmsg.get(timeout=0.001)  # 1 millisecond

                if err is None:
                    self.succeeded += batch['imported']
                    self.receive_meter.increment(batch['imported'])
                else:
                    err = str(err)

                    if err.startswith('ValueError') or err.startswith('TypeError') or err.startswith('IndexError') \
                            or batch['attempts'] >= self.max_attempts:
                        shell.printerr("Failed to import %d rows: %s -  given up after %d attempts"
                                       % (len(batch['rows']), err, batch['attempts']))
                        self.failed += len(batch['rows'])
                    else:
                        shell.printerr("Failed to import %d rows: %s -  will retry later, attempt %d of %d"
                                       % (len(batch['rows']), err, batch['attempts'],
                                          self.max_attempts))
                        self.retries.append(self.reset_batch(batch))
            except Queue.Empty:
                break

    def send_batches(self, reader):
        """
        Send batches to the queue until we have exceeded the ingest rate. In the export case we queue
        everything and let the worker processes throttle using max_requests, here we throttle
        in the parent process because of memory usage concerns.

        When we have finished reading the csv file, then send any retries.
        """
        while self.send_meter.current_record <= self.ingest_rate:
            if not reader.exhausted:
                rows = reader.read_rows()
                if rows:
                    self.sent += self.send_batch(self.new_batch(rows))
            elif self.retries:
                batch = self.retries.popleft()
                self.send_batch(batch)
            else:
                break

    def send_batch(self, batch):
        batch['attempts'] += 1
        num_rows = len(batch['rows'])
        self.send_meter.increment(num_rows)
        self.outmsg.put(batch)
        return num_rows

    def new_batch(self, rows):
        self.batch_id += 1
        return self.make_batch(self.batch_id, rows, 0)

    @staticmethod
    def reset_batch(batch):
        batch['imported'] = 0
        return batch

    @staticmethod
    def make_batch(batch_id, rows, attempts):
        return {'id': batch_id, 'rows': rows, 'attempts': attempts, 'imported': 0}


class ChildProcess(mp.Process):
    """
    An child worker process, this is for common functionality between ImportProcess and ExportProcess.
    """

    def __init__(self, params, target):
        mp.Process.__init__(self, target=target)
        self.inmsg = params['inmsg']
        self.outmsg = params['outmsg']
        self.ks = params['ks']
        self.cf = params['cf']
        self.columns = params['columns']
        self.debug = params['debug']
        self.port = params['port']
        self.hostname = params['hostname']
        self.consistency_level = params['consistency_level']
        self.connect_timeout = params['connect_timeout']
        self.cql_version = params['cql_version']
        self.auth_provider = params['auth_provider']
        self.ssl = params['ssl']
        self.protocol_version = params['protocol_version']
        self.config_file = params['config_file']

        # Here we inject some failures for testing purposes, only if this environment variable is set
        if os.environ.get('CQLSH_COPY_TEST_FAILURES', ''):
            self.test_failures = json.loads(os.environ.get('CQLSH_COPY_TEST_FAILURES', ''))
        else:
            self.test_failures = None

    def printmsg(self, text):
        if self.debug:
            sys.stderr.write(text + os.linesep)

    def close(self):
        self.printmsg("Closing queues...")
        self.inmsg.close()
        self.outmsg.close()


class ExpBackoffRetryPolicy(RetryPolicy):
    """
    A retry policy with exponential back-off for read timeouts and write timeouts
    """
    def __init__(self, parent_process):
        RetryPolicy.__init__(self)
        self.max_attempts = parent_process.max_attempts
        self.printmsg = parent_process.printmsg

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        return self._handle_timeout(consistency, retry_num)

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        return self._handle_timeout(consistency, retry_num)

    def _handle_timeout(self, consistency, retry_num):
        delay = self.backoff(retry_num)
        if delay > 0:
            self.printmsg("Timeout received, retrying after %d seconds" % (delay))
            time.sleep(delay)
            return self.RETRY, consistency
        elif delay == 0:
            self.printmsg("Timeout received, retrying immediately")
            return self.RETRY, consistency
        else:
            self.printmsg("Timeout received, giving up after %d attempts" % (retry_num + 1))
            return self.RETHROW, None

    def backoff(self, retry_num):
        """
        Perform exponential back-off up to a maximum number of times, where
        this maximum is per query.
        To back-off we should wait a random number of seconds
        between 0 and 2^c - 1, where c is the number of total failures.
        randrange() excludes the last value, so we drop the -1.

        :return : the number of seconds to wait for, -1 if we should not retry
        """
        if retry_num >= self.max_attempts:
            return -1

        delay = randrange(0, pow(2, retry_num + 1))
        return delay


class ExportSession(object):
    """
    A class for connecting to a cluster and storing the number
    of requests that this connection is processing. It wraps the methods
    for executing a query asynchronously and for shutting down the
    connection to the cluster.
    """
    def __init__(self, cluster, export_process):
        session = cluster.connect(export_process.ks)
        session.row_factory = tuple_factory
        session.default_fetch_size = export_process.csv_options['pagesize']
        session.default_timeout = export_process.csv_options['pagetimeout']

        export_process.printmsg("Created connection to %s with page size %d and timeout %d seconds per page"
                                % (session.hosts, session.default_fetch_size, session.default_timeout))

        self.cluster = cluster
        self.session = session
        self.requests = 1
        self.lock = Lock()

    def add_request(self):
        with self.lock:
            self.requests += 1

    def complete_request(self):
        with self.lock:
            self.requests -= 1

    def num_requests(self):
        with self.lock:
            return self.requests

    def execute_async(self, query):
        return self.session.execute_async(query)

    def shutdown(self):
        self.cluster.shutdown()


class ExportProcess(ChildProcess):
    """
    An child worker process for the export task, ExportTask.
    """

    def __init__(self, params):
        ChildProcess.__init__(self, params=params, target=self.run)
        self.dialect_options = params['dialect_options']
        self.hosts_to_sessions = dict()

        csv_options = params['csv_options']
        self.encoding = csv_options['encoding']
        self.time_format = csv_options['dtformats']
        self.float_precision = csv_options['float_precision']
        self.nullval = csv_options['nullval']
        self.max_attempts = csv_options['maxattempts']
        self.max_requests = csv_options['maxrequests']
        self.csv_options = csv_options
        self.formatters = dict()

    def run(self):
        try:
            self.inner_run()
        finally:
            self.close()

    def inner_run(self):
        """
        The parent sends us (range, info) on the inbound queue (inmsg)
        in order to request us to process a range, for which we can
        select any of the hosts in info, which also contains other information for this
        range such as the number of attempts already performed. We can signal errors
        on the outbound queue (outmsg) by sending (range, error) or
        we can signal a global error by sending (None, error).
        We terminate when the inbound queue is closed.
        """
        while True:
            if self.num_requests() > self.max_requests:
                time.sleep(0.001)  # 1 millisecond
                continue

            token_range, info = self.inmsg.get()
            self.start_request(token_range, info)

    def report_error(self, err, token_range=None):
        if isinstance(err, str):
            msg = err
        elif isinstance(err, BaseException):
            msg = "%s - %s" % (err.__class__.__name__, err)
            if self.debug:
                traceback.print_exc(err)
        else:
            msg = str(err)

        self.printmsg(msg)
        self.outmsg.put((token_range, Exception(msg)))

    def start_request(self, token_range, info):
        """
        Begin querying a range by executing an async query that
        will later on invoke the callbacks attached in attach_callbacks.
        """
        session = self.get_session(info['hosts'])
        metadata = session.cluster.metadata.keyspaces[self.ks].tables[self.cf]
        query = self.prepare_query(metadata.partition_key, token_range, info['attempts'])
        future = session.execute_async(query)
        self.attach_callbacks(token_range, future, session)

    def num_requests(self):
        return sum(session.num_requests() for session in self.hosts_to_sessions.values())

    def get_session(self, hosts):
        """
        We select a host to connect to. If we have no connections to one of the hosts
        yet then we select this host, else we pick the one with the smallest number
        of requests.

        :return: An ExportSession connected to the chosen host.
        """
        new_hosts = [h for h in hosts if h not in self.hosts_to_sessions]
        if new_hosts:
            host = new_hosts[0]
            new_cluster = Cluster(
                contact_points=(host,),
                port=self.port,
                cql_version=self.cql_version,
                protocol_version=self.protocol_version,
                auth_provider=self.auth_provider,
                ssl_options=ssl_settings(host, self.config_file) if self.ssl else None,
                load_balancing_policy=TokenAwarePolicy(WhiteListRoundRobinPolicy(hosts)),
                default_retry_policy=ExpBackoffRetryPolicy(self),
                compression=None)

            session = ExportSession(new_cluster, self)
            self.hosts_to_sessions[host] = session
            return session
        else:
            host = min(hosts, key=lambda h: self.hosts_to_sessions[h].requests)
            session = self.hosts_to_sessions[host]
            session.add_request()
            return session

    def attach_callbacks(self, token_range, future, session):
        def result_callback(rows):
            if future.has_more_pages:
                future.start_fetching_next_page()
                self.write_rows_to_csv(token_range, rows)
            else:
                self.write_rows_to_csv(token_range, rows)
                self.outmsg.put((None, None))
                session.complete_request()

        def err_callback(err):
            self.report_error(err, token_range)
            session.complete_request()

        future.add_callbacks(callback=result_callback, errback=err_callback)

    def write_rows_to_csv(self, token_range, rows):
        if not rows:
            return  # no rows in this range

        try:
            output = StringIO()
            writer = csv.writer(output, **self.dialect_options)

            for row in rows:
                writer.writerow(map(self.format_value, row))

            data = (output.getvalue(), len(rows))
            self.outmsg.put((token_range, data))
            output.close()

        except Exception, e:
            self.report_error(e, token_range)

    def format_value(self, val):
        if val is None or val == EMPTY:
            return format_value_default(self.nullval, colormap=NO_COLOR_MAP)

        ctype = type(val)
        formatter = self.formatters.get(ctype, None)
        if not formatter:
            formatter = get_formatter(ctype)
            self.formatters[ctype] = formatter

        return formatter(val, encoding=self.encoding, colormap=NO_COLOR_MAP, time_format=self.time_format,
                         float_precision=self.float_precision, nullval=self.nullval, quote=False)

    def close(self):
        ChildProcess.close(self)
        for session in self.hosts_to_sessions.values():
            session.shutdown()

    def prepare_query(self, partition_key, token_range, attempts):
        """
        Return the export query or a fake query with some failure injected.
        """
        if self.test_failures:
            return self.maybe_inject_failures(partition_key, token_range, attempts)
        else:
            return self.prepare_export_query(partition_key, token_range)

    def maybe_inject_failures(self, partition_key, token_range, attempts):
        """
        Examine self.test_failures and see if token_range is either a token range
        supposed to cause a failure (failing_range) or to terminate the worker process
        (exit_range). If not then call prepare_export_query(), which implements the
        normal behavior.
        """
        start_token, end_token = token_range

        if not start_token or not end_token:
            # exclude first and last ranges to make things simpler
            return self.prepare_export_query(partition_key, token_range)

        if 'failing_range' in self.test_failures:
            failing_range = self.test_failures['failing_range']
            if start_token >= failing_range['start'] and end_token <= failing_range['end']:
                if attempts < failing_range['num_failures']:
                    return 'SELECT * from bad_table'

        if 'exit_range' in self.test_failures:
            exit_range = self.test_failures['exit_range']
            if start_token >= exit_range['start'] and end_token <= exit_range['end']:
                sys.exit(1)

        return self.prepare_export_query(partition_key, token_range)

    def prepare_export_query(self, partition_key, token_range):
        """
        Return a query where we select all the data for this token range
        """
        pk_cols = ", ".join(protect_names(col.name for col in partition_key))
        columnlist = ', '.join(protect_names(self.columns))
        start_token, end_token = token_range
        query = 'SELECT %s FROM %s.%s' % (columnlist, protect_name(self.ks), protect_name(self.cf))
        if start_token is not None or end_token is not None:
            query += ' WHERE'
        if start_token is not None:
            query += ' token(%s) > %s' % (pk_cols, start_token)
        if start_token is not None and end_token is not None:
            query += ' AND'
        if end_token is not None:
            query += ' token(%s) <= %s' % (pk_cols, end_token)
        return query


class ImportConversion(object):
    """
    A class for converting strings to values when importing from csv, used by ImportProcess,
    the parent.
    """
    def __init__(self, parent, table_meta, statement):
        self.ks = parent.ks
        self.cf = parent.cf
        self.columns = parent.columns
        self.nullval = parent.nullval
        self.printmsg = parent.printmsg
        self.table_meta = table_meta
        self.primary_key_indexes = [self.columns.index(col.name) for col in self.table_meta.primary_key]
        self.partition_key_indexes = [self.columns.index(col.name) for col in self.table_meta.partition_key]

        self.proto_version = statement.protocol_version
        self.cqltypes = dict([(c.name, c.type) for c in statement.column_metadata])
        self.converters = dict([(c.name, self._get_converter(c.type)) for c in statement.column_metadata])

    def _get_converter(self, cql_type):
        """
        Return a function that converts a string into a value the can be passed
        into BoundStatement.bind() for the given cql type. See cassandra.cqltypes
        for more details.
        """
        def unprotect(v):
            if v is not None:
                return CqlRuleSet.dequote_value(v)

        def convert(t, v):
            return converters.get(t.typename, convert_unknown)(unprotect(v), ct=t)

        def split(val, sep=','):
            """
            Split into a list of values whenever we encounter a separator but
            ignore separators inside parentheses or single quotes, except for the two
            outermost parentheses, which will be ignored. We expect val to be at least
            2 characters long (the two outer parentheses).
            """
            ret = []
            last = 1
            level = 0
            quote = False
            for i, c in enumerate(val):
                if c == '{' or c == '[' or c == '(':
                    level += 1
                elif c == '}' or c == ']' or c == ')':
                    level -= 1
                elif c == '\'':
                    quote = not quote
                elif c == sep and level == 1 and not quote:
                    ret.append(val[last:i])
                    last = i + 1
            else:
                if last < len(val) - 1:
                    ret.append(val[last:-1])

            return ret

        # this should match all possible CQL datetime formats
        p = re.compile("(\d{4})\-(\d{2})\-(\d{2})\s?(?:'T')?" +  # YYYY-MM-DD[( |'T')]
                       "(?:(\d{2}):(\d{2})(?::(\d{2}))?)?" +  # [HH:MM[:SS]]
                       "(?:([+\-])(\d{2}):?(\d{2}))?")  # [(+|-)HH[:]MM]]

        def convert_date(val, **_):
            m = p.match(val)
            if not m:
                raise ValueError("can't interpret %r as a date" % (val,))

            # https://docs.python.org/2/library/time.html#time.struct_time
            tval = time.struct_time((int(m.group(1)), int(m.group(2)), int(m.group(3)),  # year, month, day
                                     int(m.group(4)) if m.group(4) else 0,  # hour
                                     int(m.group(5)) if m.group(5) else 0,  # minute
                                     int(m.group(6)) if m.group(6) else 0,  # second
                                     0, 1, -1))  # day of week, day of year, dst-flag

            if m.group(7):
                offset = (int(m.group(8)) * 3600 + int(m.group(9)) * 60) * int(m.group(7) + '1')
            else:
                offset = -time.timezone

            # scale seconds to millis for the raw value
            return (timegm(tval) + offset) * 1e3

        def convert_tuple(val, ct=cql_type):
            return tuple(convert(t, v) for t, v in zip(ct.subtypes, split(val)))

        def convert_list(val, ct=cql_type):
            return list(convert(ct.subtypes[0], v) for v in split(val))

        def convert_set(val, ct=cql_type):
            return frozenset(convert(ct.subtypes[0], v) for v in split(val))

        def convert_map(val, ct=cql_type):
            """
            We need to pass to BoundStatement.bind() a dict() because it calls iteritems(),
            except we can't create a dict with another dict as the key, hence we use a class
            that adds iteritems to a frozen set of tuples (which is how dict are normally made
            immutable in python).
            """
            class ImmutableDict(frozenset):
                iteritems = frozenset.__iter__

            return ImmutableDict(frozenset((convert(ct.subtypes[0], v[0]), convert(ct.subtypes[1], v[1]))
                                 for v in [split('{%s}' % vv, sep=':') for vv in split(val)]))

        def convert_user_type(val, ct=cql_type):
            """
            A user type is a dictionary except that we must convert each key into
            an attribute, so we are using named tuples. It must also be hashable,
            so we cannot use dictionaries. Maybe there is a way to instantiate ct
            directly but I could not work it out.
            """
            vals = [v for v in [split('{%s}' % vv, sep=':') for vv in split(val)]]
            ret_type = namedtuple(ct.typename, [unprotect(v[0]) for v in vals])
            return ret_type(*tuple(convert(t, v[1]) for t, v in zip(ct.subtypes, vals)))

        def convert_single_subtype(val, ct=cql_type):
            return converters.get(ct.subtypes[0].typename, convert_unknown)(val, ct=ct.subtypes[0])

        def convert_unknown(val, ct=cql_type):
            if issubclass(ct, UserType):
                return convert_user_type(val, ct=ct)
            elif issubclass(ct, ReversedType):
                return convert_single_subtype(val, ct=ct)

            self.printmsg("Unknown type %s (%s) for val %s" % (ct, ct.typename, val))
            return val

        converters = {
            'blob': (lambda v, ct=cql_type: bytearray.fromhex(v[2:])),
            'decimal': (lambda v, ct=cql_type: Decimal(v)),
            'uuid': (lambda v, ct=cql_type: UUID(v)),
            'boolean': (lambda v, ct=cql_type: bool(v)),
            'tinyint': (lambda v, ct=cql_type: int(v)),
            'ascii': (lambda v, ct=cql_type: v),
            'float': (lambda v, ct=cql_type: float(v)),
            'double': (lambda v, ct=cql_type: float(v)),
            'bigint': (lambda v, ct=cql_type: long(v)),
            'int': (lambda v, ct=cql_type: int(v)),
            'varint': (lambda v, ct=cql_type: int(v)),
            'inet': (lambda v, ct=cql_type: v),
            'counter': (lambda v, ct=cql_type: long(v)),
            'timestamp': convert_date,
            'timeuuid': (lambda v, ct=cql_type: UUID(v)),
            'date': (lambda v, ct=cql_type: Date(v)),
            'smallint': (lambda v, ct=cql_type: int(v)),
            'time': (lambda v, ct=cql_type: Time(v)),
            'text': (lambda v, ct=cql_type: v),
            'varchar': (lambda v, ct=cql_type: v),
            'list': convert_list,
            'set': convert_set,
            'map': convert_map,
            'tuple': convert_tuple,
            'frozen': convert_single_subtype,
        }

        return converters.get(cql_type.typename, convert_unknown)

    def get_row_values(self, row):
        """
        Parse the row into a list of row values to be returned
        """
        ret = [None] * len(row)
        for i, val in enumerate(row):
            if val != self.nullval:
                ret[i] = self.converters[self.columns[i]](val)
            else:
                if i in self.primary_key_indexes:
                    raise ValueError(self.get_null_primary_key_message(i))

                ret[i] = None

        return ret

    def get_null_primary_key_message(self, idx):
        message = "Cannot insert null value for primary key column '%s'." % (self.columns[idx],)
        if self.nullval == '':
            message += " If you want to insert empty strings, consider using" \
                       " the WITH NULL=<marker> option for COPY."
        return message

    def get_row_partition_key_values(self, row):
        """
        Return a string composed of the partition key values, serialized and binary packed -
        as expected by metadata.get_replicas(), see also BoundStatement.routing_key.
        """
        def serialize(n):
            c, v = self.columns[n], row[n]
            if v == self.nullval:
                raise ValueError(self.get_null_primary_key_message(n))
            return self.cqltypes[c].serialize(self.converters[c](v), self.proto_version)

        partition_key_indexes = self.partition_key_indexes
        if len(partition_key_indexes) == 1:
            return serialize(partition_key_indexes[0])
        else:
            pk_values = []
            for i in partition_key_indexes:
                val = serialize(i)
                l = len(val)
                pk_values.append(struct.pack(">H%dsB" % l, l, val, 0))
            return b"".join(pk_values)


class ImportProcess(ChildProcess):

    def __init__(self, params):
        ChildProcess.__init__(self, params=params, target=self.run)

        csv_options = params['csv_options']
        self.nullval = csv_options['nullval']
        self.max_attempts = csv_options['maxattempts']
        self.min_batch_size = csv_options['minbatchsize']
        self.max_batch_size = csv_options['maxbatchsize']
        self._session = None

    @property
    def session(self):
        if not self._session:
            cluster = Cluster(
                contact_points=(self.hostname,),
                port=self.port,
                cql_version=self.cql_version,
                protocol_version=self.protocol_version,
                auth_provider=self.auth_provider,
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                ssl_options=ssl_settings(self.hostname, self.config_file) if self.ssl else None,
                default_retry_policy=ExpBackoffRetryPolicy(self),
                compression=None,
                connect_timeout=self.connect_timeout)

            self._session = cluster.connect(self.ks)
            self._session.default_timeout = None
        return self._session

    def run(self):
        try:
            table_meta = self.session.cluster.metadata.keyspaces[self.ks].tables[self.cf]
            is_counter = ("counter" in [table_meta.columns[name].typestring for name in self.columns])

            if is_counter:
                self.run_counter(table_meta)
            else:
                self.run_normal(table_meta)

        except Exception, exc:
            if self.debug:
                traceback.print_exc(exc)

        finally:
            self.close()

    def close(self):
        if self._session:
            self._session.cluster.shutdown()
        ChildProcess.close(self)

    def run_counter(self, table_meta):
        """
        Main run method for tables that contain counter columns.
        """
        query = 'UPDATE %s.%s SET %%s WHERE %%s' % (protect_name(self.ks), protect_name(self.cf))

        # We prepare a query statement to find out the types of the partition key columns so we can
        # route the update query to the correct replicas. As far as I understood this is the easiest
        # way to find out the types of the partition columns, we will never use this prepared statement
        where_clause = ' AND '.join(['%s = ?' % (protect_name(c.name)) for c in table_meta.partition_key])
        select_query = 'SELECT * FROM %s.%s WHERE %s' % (protect_name(self.ks), protect_name(self.cf), where_clause)
        conv = ImportConversion(self, table_meta, self.session.prepare(select_query))

        while True:
            try:
                batch = self.inmsg.get()

                for batches in self.split_batches(batch, conv):
                    for b in batches:
                        self.send_counter_batch(query, conv, b)

            except Exception, exc:
                self.outmsg.put((batch, '%s - %s' % (exc.__class__.__name__, exc.message)))
                if self.debug:
                    traceback.print_exc(exc)

    def run_normal(self, table_meta):
        """
        Main run method for normal tables, i.e. tables that do not contain counter columns.
        """
        query = 'INSERT INTO %s.%s (%s) VALUES (%s)' % (protect_name(self.ks),
                                                        protect_name(self.cf),
                                                        ', '.join(protect_names(self.columns),),
                                                        ', '.join(['?' for _ in self.columns]))
        query_statement = self.session.prepare(query)
        conv = ImportConversion(self, table_meta, query_statement)

        while True:
            try:
                batch = self.inmsg.get()

                for batches in self.split_batches(batch, conv):
                    for b in batches:
                        self.send_normal_batch(conv, query_statement, b)

            except Exception, exc:
                self.outmsg.put((batch, '%s - %s' % (exc.__class__.__name__, exc.message)))
                if self.debug:
                    traceback.print_exc(exc)

    def send_counter_batch(self, query_text, conv, batch):
        if self.test_failures and self.maybe_inject_failures(batch):
            return

        columns = self.columns
        batch_statement = BatchStatement(batch_type=BatchType.COUNTER, consistency_level=self.consistency_level)
        for row in batch['rows']:
            where_clause = []
            set_clause = []
            for i, value in enumerate(row):
                if i in conv.primary_key_indexes:
                    where_clause.append("%s=%s" % (columns[i], value))
                else:
                    set_clause.append("%s=%s+%s" % (columns[i], columns[i], value))

            full_query_text = query_text % (','.join(set_clause), ' AND '.join(where_clause))
            batch_statement.add(full_query_text)

        self.execute_statement(batch_statement, batch)

    def send_normal_batch(self, conv, query_statement, batch):
        try:
            if self.test_failures and self.maybe_inject_failures(batch):
                return

            batch_statement = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=self.consistency_level)
            for row in batch['rows']:
                batch_statement.add(query_statement, conv.get_row_values(row))

            self.execute_statement(batch_statement, batch)

        except Exception, exc:
            self.err_callback(exc, batch)

    def maybe_inject_failures(self, batch):
        """
        Examine self.test_failures and see if token_range is either a token range
        supposed to cause a failure (failing_range) or to terminate the worker process
        (exit_range). If not then call prepare_export_query(), which implements the
        normal behavior.
        """
        if 'failing_batch' in self.test_failures:
            failing_batch = self.test_failures['failing_batch']
            if failing_batch['id'] == batch['id']:
                if batch['attempts'] < failing_batch['failures']:
                    statement = SimpleStatement("INSERT INTO badtable (a, b) VALUES (1, 2)",
                                                consistency_level=self.consistency_level)
                    self.execute_statement(statement, batch)
                    return True

        if 'exit_batch' in self.test_failures:
            exit_batch = self.test_failures['exit_batch']
            if exit_batch['id'] == batch['id']:
                sys.exit(1)

        return False  # carry on as normal

    def execute_statement(self, statement, batch):
        future = self.session.execute_async(statement)
        future.add_callbacks(callback=self.result_callback, callback_args=(batch, ),
                             errback=self.err_callback, errback_args=(batch, ))

    def split_batches(self, batch, conv):
        """
        Split a batch into sub-batches with the same
        partition key, if possible. If there are at least
        batch_size rows with the same partition key value then
        create a sub-batch with that partition key value, else
        aggregate all remaining rows in a single 'left-overs' batch
        """
        rows_by_pk = defaultdict(list)

        for row in batch['rows']:
            pk = conv.get_row_partition_key_values(row)
            rows_by_pk[pk].append(row)

        ret = dict()
        remaining_rows = []

        for pk, rows in rows_by_pk.items():
            if len(rows) >= self.min_batch_size:
                ret[pk] = self.batches(rows, batch)
            else:
                remaining_rows.extend(rows)

        if remaining_rows:
            ret[self.hostname] = self.batches(remaining_rows, batch)

        return ret.itervalues()

    def batches(self, rows, batch):
        for i in xrange(0, len(rows), self.max_batch_size):
            yield ImportTask.make_batch(batch['id'], rows[i:i + self.max_batch_size], batch['attempts'])

    def result_callback(self, result, batch):
        batch['imported'] = len(batch['rows'])
        batch['rows'] = []  # no need to resend these
        self.outmsg.put((batch, None))

    def err_callback(self, response, batch):
        batch['imported'] = len(batch['rows'])
        self.outmsg.put((batch, '%s - %s' % (response.__class__.__name__, response.message)))
        if self.debug:
            traceback.print_exc(response)


class RateMeter(object):

    def __init__(self, update_interval=0.25, log=True):
        self.log = log  # true if we should log
        self.update_interval = update_interval  # how often we update in seconds
        self.start_time = time.time()  # the start time
        self.last_checkpoint_time = self.start_time  # last time we logged
        self.current_rate = 0.0  # rows per second
        self.current_record = 0  # number of records since we last updated
        self.total_records = 0   # total number of records

    def increment(self, n=1):
        self.current_record += n
        self.maybe_update()

    def maybe_update(self):
        new_checkpoint_time = time.time()
        if new_checkpoint_time - self.last_checkpoint_time >= self.update_interval:
            self.update(new_checkpoint_time)
            self.log_message()

    def update(self, new_checkpoint_time):
        time_difference = new_checkpoint_time - self.last_checkpoint_time
        if time_difference >= 1e-09:
            self.current_rate = self.get_new_rate(self.current_record / time_difference)

        self.last_checkpoint_time = new_checkpoint_time
        self.total_records += self.current_record
        self.current_record = 0

    def get_new_rate(self, new_rate):
        """
         return the rate of the last period: this is the new rate but
         averaged with the last rate to smooth a bit
        """
        if self.current_rate == 0.0:
            return new_rate
        else:
            return (self.current_rate + new_rate) / 2.0

    def get_avg_rate(self):
        """
         return the average rate since we started measuring
        """
        time_difference = time.time() - self.start_time
        return self.total_records / time_difference if time_difference >= 1e-09 else 0

    def log_message(self):
        if self.log:
            output = 'Processed: %d rows; Rate: %7.0f rows/s; Avg. rage: %7.0f rows/s\r' % \
                     (self.total_records, self.current_rate, self.get_avg_rate())
            sys.stdout.write(output)
            sys.stdout.flush()

    def get_total_records(self):
        self.update(time.time())
        self.log_message()
        return self.total_records
