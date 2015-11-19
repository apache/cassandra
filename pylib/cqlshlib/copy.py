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
import sys
import time
import traceback

from StringIO import StringIO
from random import randrange
from threading import Lock

from cassandra.cluster import Cluster
from cassandra.metadata import protect_name, protect_names
from cassandra.policies import RetryPolicy, WhiteListRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import tuple_factory


import sslhandling
from displaying import NO_COLOR_MAP
from formatting import format_value_default, DateTimeFormat, EMPTY, get_formatter


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
    csv_options['jobs'] = int(opts.pop('jobs', 12))
    csv_options['pagesize'] = int(opts.pop('pagesize', 1000))
    # by default the page timeout is 10 seconds per 1000 entries in the page size or 10 seconds if pagesize is smaller
    csv_options['pagetimeout'] = int(opts.pop('pagetimeout', max(10, 10 * (csv_options['pagesize'] / 1000))))
    csv_options['maxattempts'] = int(opts.pop('maxattempts', 5))
    csv_options['float_precision'] = shell.display_float_precision
    csv_options['dtformats'] = DateTimeFormat(opts.pop('timeformat', shell.display_timestamp_format),
                                              shell.display_date_format,
                                              shell.display_nanotime_format)

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


class ExportTask(object):
    """
    A class that exports data to .csv by instantiating one or more processes that work in parallel (ExportProcess).
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

        inmsg = mp.Queue()
        outmsg = mp.Queue()
        processes = []
        for i in xrange(num_processes):
            process = ExportProcess(outmsg, inmsg, self.ks, self.cf, self.columns, self.dialect_options,
                                    self.csv_options, shell.debug, shell.port, shell.conn.cql_version,
                                    shell.auth_provider, shell.ssl, self.protocol_version, self.config_file)
            process.start()
            processes.append(process)

        try:
            return self.check_processes(csvdest, ranges, inmsg, outmsg, processes)
        finally:
            for process in processes:
                process.terminate()

            inmsg.close()
            outmsg.close()
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
                if host.datacenter == local_dc:
                    hosts.append(host.address)
            if len(hosts) == 0:
                hosts.append(hostname)  # fallback to default host if no replicas in current dc
            ranges[(previous, token.value)] = make_range(hosts)
            previous_previous = previous
            previous = token.value

        #  If the ring is empty we get the entire ring from the
        #  host we are currently connected to, otherwise for the last ring interval
        #  we query the same replicas that hold the last token in the ring
        if len(ranges) == 0:
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

    @staticmethod
    def send_work(ranges, tokens_to_send, queue):
        for token_range in tokens_to_send:
            queue.put((token_range, ranges[token_range]))
            ranges[token_range]['attempts'] += 1

    def check_processes(self, csvdest, ranges, inmsg, outmsg, processes):
        """
        Here we monitor all child processes by collecting their results
        or any errors. We terminate when we have processed all the ranges or when there
        are no more processes.
        """
        shell = self.shell
        meter = RateMeter(10000)
        total_jobs = len(ranges)
        max_attempts = self.csv_options['maxattempts']

        self.send_work(ranges, ranges.keys(), outmsg)

        num_processes = len(processes)
        succeeded = 0
        failed = 0
        while (failed + succeeded) < total_jobs and self.num_live_processes(processes) == num_processes:
            try:
                token_range, result = inmsg.get(timeout=1.0)
                if token_range is None and result is None:  # a job has finished
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
                            self.send_work(ranges, [token_range], outmsg)
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

        if self.num_live_processes(processes) < len(processes):
            for process in processes:
                if not process.is_alive():
                    shell.printerr('Child process %d died with exit code %d' % (process.pid, process.exitcode))

        if succeeded < total_jobs:
            shell.printerr('Exported %d ranges out of %d total ranges, some records might be missing'
                           % (succeeded, total_jobs))

        return meter.get_total_records()

    @staticmethod
    def num_live_processes(processes):
        return sum(1 for p in processes if p.is_alive())


class ExpBackoffRetryPolicy(RetryPolicy):
    """
    A retry policy with exponential back-off for read timeouts,
    see ExportProcess.
    """
    def __init__(self, export_process):
        RetryPolicy.__init__(self)
        self.max_attempts = export_process.csv_options['maxattempts']
        self.printmsg = lambda txt: export_process.printmsg(txt)

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
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
    of jobs that this connection is processing. It wraps the methods
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
        self.jobs = 1
        self.lock = Lock()

    def add_job(self):
        with self.lock:
            self.jobs += 1

    def complete_job(self):
        with self.lock:
            self.jobs -= 1

    def num_jobs(self):
        with self.lock:
            return self.jobs

    def execute_async(self, query):
        return self.session.execute_async(query)

    def shutdown(self):
        self.cluster.shutdown()


class ExportProcess(mp.Process):
    """
    An child worker process for the export task, ExportTask.
    """

    def __init__(self, inmsg, outmsg, ks, cf, columns, dialect_options, csv_options,
                 debug, port, cql_version, auth_provider, ssl, protocol_version, config_file):
        mp.Process.__init__(self, target=self.run)
        self.inmsg = inmsg
        self.outmsg = outmsg
        self.ks = ks
        self.cf = cf
        self.columns = columns
        self.dialect_options = dialect_options
        self.hosts_to_sessions = dict()

        self.debug = debug
        self.port = port
        self.cql_version = cql_version
        self.auth_provider = auth_provider
        self.ssl = ssl
        self.protocol_version = protocol_version
        self.config_file = config_file

        self.encoding = csv_options['encoding']
        self.date_time_format = csv_options['dtformats']
        self.float_precision = csv_options['float_precision']
        self.nullval = csv_options['nullval']
        self.maxjobs = csv_options['jobs']
        self.csv_options = csv_options
        self.formatters = dict()

        # Here we inject some failures for testing purposes, only if this environment variable is set
        if os.environ.get('CQLSH_COPY_TEST_FAILURES', ''):
            self.test_failures = json.loads(os.environ.get('CQLSH_COPY_TEST_FAILURES', ''))
        else:
            self.test_failures = None

    def printmsg(self, text):
        if self.debug:
            sys.stderr.write(text + os.linesep)

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
            if self.num_jobs() > self.maxjobs:
                time.sleep(0.001)  # 1 millisecond
                continue

            token_range, info = self.inmsg.get()
            self.start_job(token_range, info)

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

    def start_job(self, token_range, info):
        """
        Begin querying a range by executing an async query that
        will later on invoke the callbacks attached in attach_callbacks.
        """
        session = self.get_session(info['hosts'])
        metadata = session.cluster.metadata.keyspaces[self.ks].tables[self.cf]
        query = self.prepare_query(metadata.partition_key, token_range, info['attempts'])
        future = session.execute_async(query)
        self.attach_callbacks(token_range, future, session)

    def num_jobs(self):
        return sum(session.num_jobs() for session in self.hosts_to_sessions.values())

    def get_session(self, hosts):
        """
        We select a host to connect to. If we have no connections to one of the hosts
        yet then we select this host, else we pick the one with the smallest number
        of jobs.

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
                ssl_options=sslhandling.ssl_settings(host, self.config_file) if self.ssl else None,
                load_balancing_policy=TokenAwarePolicy(WhiteListRoundRobinPolicy(hosts)),
                default_retry_policy=ExpBackoffRetryPolicy(self),
                compression=None,
                executor_threads=max(2, self.csv_options['jobs'] / 2))

            session = ExportSession(new_cluster, self)
            self.hosts_to_sessions[host] = session
            return session
        else:
            host = min(hosts, key=lambda h: self.hosts_to_sessions[h].jobs)
            session = self.hosts_to_sessions[host]
            session.add_job()
            return session

    def attach_callbacks(self, token_range, future, session):
        def result_callback(rows):
            if future.has_more_pages:
                future.start_fetching_next_page()
                self.write_rows_to_csv(token_range, rows)
            else:
                self.write_rows_to_csv(token_range, rows)
                self.outmsg.put((None, None))
                session.complete_job()

        def err_callback(err):
            self.report_error(err, token_range)
            session.complete_job()

        future.add_callbacks(callback=result_callback, errback=err_callback)

    def write_rows_to_csv(self, token_range, rows):
        if len(rows) == 0:
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

        return formatter(val, encoding=self.encoding, colormap=NO_COLOR_MAP, date_time_format=self.date_time_format,
                         float_precision=self.float_precision, nullval=self.nullval, quote=False)

    def close(self):
        self.printmsg("Export process terminating...")
        self.inmsg.close()
        self.outmsg.close()
        for session in self.hosts_to_sessions.values():
            session.shutdown()
        self.printmsg("Export process terminated")

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


class RateMeter(object):

    def __init__(self, log_threshold):
        self.log_threshold = log_threshold  # number of records after which we log
        self.last_checkpoint_time = time.time()  # last time we logged
        self.current_rate = 0.0  # rows per second
        self.current_record = 0  # number of records since we last logged
        self.total_records = 0   # total number of records

    def increment(self, n=1):
        self.current_record += n

        if self.current_record >= self.log_threshold:
            self.update()
            self.log()

    def update(self):
        new_checkpoint_time = time.time()
        time_difference = new_checkpoint_time - self.last_checkpoint_time
        if time_difference != 0.0:
            self.current_rate = self.get_new_rate(self.current_record / time_difference)

        self.last_checkpoint_time = new_checkpoint_time
        self.total_records += self.current_record
        self.current_record = 0

    def get_new_rate(self, new_rate):
        """
         return the previous rate averaged with the new rate to smooth a bit
        """
        if self.current_rate == 0.0:
            return new_rate
        else:
            return (self.current_rate + new_rate) / 2.0

    def log(self):
        output = 'Processed %d rows; Written: %f rows/s\r' % (self.total_records, self.current_rate,)
        sys.stdout.write(output)
        sys.stdout.flush()

    def get_total_records(self):
        self.update()
        self.log()
        return self.total_records
