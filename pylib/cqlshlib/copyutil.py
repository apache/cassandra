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

import ConfigParser
import csv
import datetime
import json
import glob
import multiprocessing as mp
import os
import Queue
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
from formatting import format_value_default, DateTimeFormat, EMPTY, get_formatter
from sslhandling import ssl_settings

CopyOptions = namedtuple('CopyOptions', 'copy dialect unrecognized')


def safe_normpath(fname):
    """
    :return the normalized path but only if there is a filename, we don't want to convert
    an empty string (which means no file name) to a dot. Also expand any user variables such as ~ to the full path
    """
    return os.path.normpath(os.path.expanduser(fname)) if fname else fname


class CopyTask(object):
    """
    A base class for ImportTask and ExportTask
    """
    def __init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file, direction):
        self.shell = shell
        self.ks = ks
        self.table = table
        self.local_dc = shell.conn.metadata.get_host(shell.hostname).datacenter
        self.fname = safe_normpath(fname)
        self.protocol_version = protocol_version
        self.config_file = config_file
        # do not display messages when exporting to STDOUT
        self.printmsg = self._printmsg if self.fname is not None or direction == 'in' else lambda _, eol='\n': None
        self.options = self.parse_options(opts, direction)

        self.num_processes = self.options.copy['numprocesses']
        self.printmsg('Using %d child processes' % (self.num_processes,))

        self.processes = []
        self.inmsg = mp.Queue()
        self.outmsg = mp.Queue()

        self.columns = CopyTask.get_columns(shell, ks, table, columns)
        self.time_start = time.time()

    @staticmethod
    def _printmsg(msg, eol='\n'):
        sys.stdout.write(msg + eol)
        sys.stdout.flush()

    def maybe_read_config_file(self, opts, direction):
        """
        Read optional sections from a configuration file that  was specified in the command options or from the default
        cqlshrc configuration file if none was specified.
        """
        config_file = opts.pop('configfile', '')
        if not config_file:
            config_file = self.config_file

        if not os.path.isfile(config_file):
            return opts

        configs = ConfigParser.RawConfigParser()
        configs.readfp(open(config_file))

        ret = dict()
        config_sections = list(['copy', 'copy-%s' % (direction,),
                                'copy:%s.%s' % (self.ks, self.table),
                                'copy-%s:%s.%s' % (direction, self.ks, self.table)])

        for section in config_sections:
            if configs.has_section(section):
                options = dict(configs.items(section))
                self.printmsg("Reading options from %s:[%s]: %s" % (config_file, section, options))
                ret.update(options)

        # Update this last so the command line options take precedence over the configuration file options
        if opts:
            self.printmsg("Reading options from the command line: %s" % (opts,))
            ret.update(opts)

        if self.shell.debug:  # this is important for testing, do not remove
            self.printmsg("Using options: '%s'" % (ret,))

        return ret

    @staticmethod
    def clean_options(opts):
        """
        Convert all option values to valid string literals unless they are path names
        """
        return dict([(k, v.decode('string_escape') if k not in ['errfile', 'ratefile'] else v)
                     for k, v, in opts.iteritems()])

    def parse_options(self, opts, direction):
        """
        Parse options for import (COPY FROM) and export (COPY TO) operations.
        Extract from opts csv and dialect options.

        :return: 3 dictionaries: the csv options, the dialect options, any unrecognized options.
        """
        shell = self.shell
        opts = self.clean_options(self.maybe_read_config_file(opts, direction))

        dialect_options = dict()
        dialect_options['quotechar'] = opts.pop('quote', '"')
        dialect_options['escapechar'] = opts.pop('escape', '\\')
        dialect_options['delimiter'] = opts.pop('delimiter', ',')
        if dialect_options['quotechar'] == dialect_options['escapechar']:
            dialect_options['doublequote'] = True
            del dialect_options['escapechar']
        else:
            dialect_options['doublequote'] = False

        copy_options = dict()
        copy_options['nullval'] = opts.pop('null', '')
        copy_options['header'] = bool(opts.pop('header', '').lower() == 'true')
        copy_options['encoding'] = opts.pop('encoding', 'utf8')
        copy_options['maxrequests'] = int(opts.pop('maxrequests', 6))
        copy_options['pagesize'] = int(opts.pop('pagesize', 1000))
        # by default the page timeout is 10 seconds per 1000 entries
        # in the page size or 10 seconds if pagesize is smaller
        copy_options['pagetimeout'] = int(opts.pop('pagetimeout', max(10, 10 * (copy_options['pagesize'] / 1000))))
        copy_options['maxattempts'] = int(opts.pop('maxattempts', 5))
        copy_options['dtformats'] = DateTimeFormat(opts.pop('datetimeformat', shell.display_timestamp_format),
                                                   shell.display_date_format, shell.display_nanotime_format)
        copy_options['float_precision'] = shell.display_float_precision
        copy_options['chunksize'] = int(opts.pop('chunksize', 1000))
        copy_options['ingestrate'] = int(opts.pop('ingestrate', 100000))
        copy_options['maxbatchsize'] = int(opts.pop('maxbatchsize', 20))
        copy_options['minbatchsize'] = int(opts.pop('minbatchsize', 2))
        copy_options['reportfrequency'] = float(opts.pop('reportfrequency', 0.25))
        copy_options['consistencylevel'] = shell.consistency_level
        copy_options['decimalsep'] = opts.pop('decimalsep', '.')
        copy_options['thousandssep'] = opts.pop('thousandssep', '')
        copy_options['boolstyle'] = [s.strip() for s in opts.pop('boolstyle', 'True, False').split(',')]
        copy_options['numprocesses'] = int(opts.pop('numprocesses', self.get_num_processes(cap=16)))
        copy_options['begintoken'] = opts.pop('begintoken', '')
        copy_options['endtoken'] = opts.pop('endtoken', '')
        copy_options['maxrows'] = int(opts.pop('maxrows', '-1'))
        copy_options['skiprows'] = int(opts.pop('skiprows', '0'))
        copy_options['skipcols'] = opts.pop('skipcols', '')
        copy_options['maxparseerrors'] = int(opts.pop('maxparseerrors', '-1'))
        copy_options['maxinserterrors'] = int(opts.pop('maxinserterrors', '-1'))
        copy_options['errfile'] = safe_normpath(opts.pop('errfile', 'import_%s_%s.err' % (self.ks, self.table,)))
        copy_options['ratefile'] = safe_normpath(opts.pop('ratefile', ''))
        copy_options['maxoutputsize'] = int(opts.pop('maxoutputsize', '-1'))
        copy_options['ttl'] = int(opts.pop('ttl', -1))

        self.check_options(copy_options)
        return CopyOptions(copy=copy_options, dialect=dialect_options, unrecognized=opts)

    @staticmethod
    def check_options(copy_options):
        """
        Check any options that require a sanity check beyond a simple type conversion and if required
        raise a value error:

        - boolean styles must be exactly 2, they must be different and they cannot be empty
        """
        bool_styles = copy_options['boolstyle']
        if len(bool_styles) != 2 or bool_styles[0] == bool_styles[1] or not bool_styles[0] or not bool_styles[1]:
            raise ValueError("Invalid boolean styles %s" % copy_options['boolstyle'])

    @staticmethod
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

    @staticmethod
    def describe_interval(seconds):
        desc = []
        for length, unit in ((86400, 'day'), (3600, 'hour'), (60, 'minute')):
            num = int(seconds) / length
            if num > 0:
                desc.append('%d %s' % (num, unit))
                if num > 1:
                    desc[-1] += 's'
            seconds %= length
        words = '%.03f seconds' % seconds
        if len(desc) > 1:
            words = ', '.join(desc) + ', and ' + words
        elif len(desc) == 1:
            words = desc[0] + ' and ' + words
        return words

    @staticmethod
    def get_columns(shell, ks, table, columns):
        """
        Return all columns if none were specified or only the columns specified.
        Possible enhancement: introduce a regex like syntax (^) to allow users
        to specify all columns except a few.
        """
        return shell.get_column_names(ks, table) if not columns else columns

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
                    table=self.table,
                    local_dc=self.local_dc,
                    columns=self.columns,
                    options=self.options,
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


class ExportWriter(object):
    """
    A class that writes to one or more csv files, or STDOUT
    """

    def __init__(self, fname, shell, columns, options):
        self.fname = fname
        self.shell = shell
        self.columns = columns
        self.options = options
        self.header = options.copy['header']
        self.max_output_size = long(options.copy['maxoutputsize'])
        self.current_dest = None
        self.num_files = 0

        if self.max_output_size > 0:
            if fname is not None:
                self.write = self._write_with_split
                self.num_written = 0
            else:
                shell.printerr("WARNING: maxoutputsize {} ignored when writing to STDOUT".format(self.max_output_size))
                self.write = self._write_without_split
        else:
            self.write = self._write_without_split

    def open(self):
        self.current_dest = self._get_dest(self.fname)
        if self.current_dest is None:
            return False

        if self.header:
            writer = csv.writer(self.current_dest.output, **self.options.dialect)
            writer.writerow(self.columns)

        return True

    def close(self):
        self._close_current_dest()

    def _next_dest(self):
        self._close_current_dest()
        self.current_dest = self._get_dest(self.fname + '.%d' % (self.num_files,))

    def _get_dest(self, source_name):
        """
        Open the output file if any or else use stdout. Return a namedtuple
        containing the out and a boolean indicating if the output should be closed.
        """
        CsvDest = namedtuple('CsvDest', 'output close')

        if self.fname is None:
            return CsvDest(output=sys.stdout, close=False)
        else:
            try:
                ret = CsvDest(output=open(source_name, 'wb'), close=True)
                self.num_files += 1
                return ret
            except IOError, e:
                self.shell.printerr("Can't open %r for writing: %s" % (source_name, e))
                return None

    def _close_current_dest(self):
        if self.current_dest and self.current_dest.close:
            self.current_dest.output.close()
            self.current_dest = None

    def _write_without_split(self, data, _):
        """
         Write the data to the current destination output.
        """
        self.current_dest.output.write(data)

    def _write_with_split(self, data, num):
        """
         Write the data to the current destination output if we still
         haven't reached the maximum number of rows. Otherwise split
         the rows between the current destination and the next.
        """
        if (self.num_written + num) > self.max_output_size:
            num_remaining = self.max_output_size - self.num_written
            last_switch = 0
            for i, row in enumerate(filter(None, data.split(os.linesep))):
                if i == num_remaining:
                    self._next_dest()
                    last_switch = i
                    num_remaining += self.max_output_size
                self.current_dest.output.write(row + '\n')

            self.num_written = num - last_switch
        else:
            self.num_written += num
            self.current_dest.output.write(data)


class ExportTask(CopyTask):
    """
    A class that exports data to .csv by instantiating one or more processes that work in parallel (ExportProcess).
    """
    def __init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file):
        CopyTask.__init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file, 'to')

        options = self.options
        self.begin_token = long(options.copy['begintoken']) if options.copy['begintoken'] else None
        self.end_token = long(options.copy['endtoken']) if options.copy['endtoken'] else None
        self.writer = ExportWriter(fname, shell, columns, options)

    def run(self):
        """
        Initiates the export by starting the worker processes.
        Then hand over control to export_records.
        """
        shell = self.shell

        if self.options.unrecognized:
            shell.printerr('Unrecognized COPY TO options: %s' % ', '.join(self.options.unrecognized.keys()))
            return

        if not self.columns:
            shell.printerr("No column specified")
            return 0

        ranges = self.get_ranges()
        if not ranges:
            return 0

        if not self.writer.open():
            return 0

        self.printmsg("\nStarting copy of %s.%s with columns %s." % (self.ks, self.table, self.columns))

        params = self.make_params()
        for i in xrange(self.num_processes):
            self.processes.append(ExportProcess(params))

        for process in self.processes:
            process.start()

        try:
            self.export_records(ranges)
        finally:
            self.close()

    def close(self):
        CopyTask.close(self)
        self.writer.close()

    def get_ranges(self):
        """
        return a queue of tuples, where the first tuple entry is a token range (from, to]
        and the second entry is a list of hosts that own that range. Each host is responsible
        for all the tokens in the range (from, to].

        The ring information comes from the driver metadata token map, which is built by
        querying System.PEERS.

        We only consider replicas that are in the local datacenter. If there are no local replicas
        we use the cqlsh session host.
        """
        shell = self.shell
        hostname = shell.hostname
        local_dc = self.local_dc
        ranges = dict()
        min_token = self.get_min_token()
        begin_token = self.begin_token
        end_token = self.end_token

        def make_range(prev, curr):
            """
            Return the intersection of (prev, curr) and (begin_token, end_token),
            return None if the intersection is empty
            """
            ret = (prev, curr)
            if begin_token:
                if ret[1] < begin_token:
                    return None
                elif ret[0] < begin_token:
                    ret = (begin_token, ret[1])

            if end_token:
                if ret[0] > end_token:
                    return None
                elif ret[1] > end_token:
                    ret = (ret[0], end_token)

            return ret

        def make_range_data(replicas=[]):
            hosts = []
            for r in replicas:
                if r.is_up and r.datacenter == local_dc:
                    hosts.append(r.address)
            if not hosts:
                hosts.append(hostname)  # fallback to default host if no replicas in current dc
            return {'hosts': tuple(hosts), 'attempts': 0, 'rows': 0}

        if begin_token and begin_token < min_token:
            shell.printerr('Begin token %d must be bigger or equal to min token %d' % (begin_token, min_token))
            return ranges

        if begin_token and end_token and begin_token > end_token:
            shell.printerr('Begin token %d must be smaller than end token %d' % (begin_token, end_token))
            return ranges

        if shell.conn.metadata.token_map is None or min_token is None:
            ranges[(begin_token, end_token)] = make_range_data()
            return ranges

        ring = shell.get_ring(self.ks).items()
        ring.sort()

        if not ring:
            #  If the ring is empty we get the entire ring from the host we are currently connected to
            ranges[(begin_token, end_token)] = make_range_data()
        elif len(ring) == 1:
            #  If there is only one token we get the entire ring from the replicas for that token
            ranges[(begin_token, end_token)] = make_range_data(ring[0][1])
        else:
            # else we loop on the ring
            first_range_data = None
            previous = None
            for token, replicas in ring:
                if not first_range_data:
                    first_range_data = make_range_data(replicas)  # we use it at the end when wrapping around

                if token.value == min_token:
                    continue  # avoids looping entire ring

                current_range = make_range(previous, token.value)
                if not current_range:
                    continue

                ranges[current_range] = make_range_data(replicas)
                previous = token.value

            #  For the last ring interval we query the same replicas that hold the first token in the ring
            if previous is not None and (not end_token or previous < end_token):
                ranges[(previous, end_token)] = first_range_data

        if not ranges:
            shell.printerr('Found no ranges to query, check begin and end tokens: %s - %s' % (begin_token, end_token))

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

    def export_records(self, ranges):
        """
        Send records to child processes and monitor them by collecting their results
        or any errors. We terminate when we have processed all the ranges or when one child
        process has died (since in this case we will never get any ACK for the ranges
        processed by it and at the moment we don't keep track of which ranges a
        process is handling).
        """
        shell = self.shell
        processes = self.processes
        meter = RateMeter(log_fcn=self.printmsg,
                          update_interval=self.options.copy['reportfrequency'],
                          log_file=self.options.copy['ratefile'])
        total_requests = len(ranges)
        max_attempts = self.options.copy['maxattempts']

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
                            # If rows were already received we'd risk duplicating data.
                            # Note that there is still a slight risk of duplicating data, even if we have
                            # an error with no rows received yet, it's just less likely. To avoid retrying on
                            # all timeouts would however mean we could risk not exporting some rows.
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
                    self.writer.write(data, num)
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

        self.printmsg("\n%d rows exported to %d files in %s." %
                      (meter.get_total_records(),
                       self.writer.num_files,
                       self.describe_interval(time.time() - self.time_start)))


class ImportReader(object):
    """
    A wrapper around a csv reader to keep track of when we have
    exhausted reading input files. We are passed a comma separated
    list of paths, where each path is a valid glob expression.
    We generate a source generator and we read each source one
    by one.
    """
    def __init__(self, task):
        self.shell = task.shell
        self.options = task.options
        self.printmsg = task.printmsg
        self.chunk_size = self.options.copy['chunksize']
        self.header = self.options.copy['header']
        self.max_rows = self.options.copy['maxrows']
        self.skip_rows = self.options.copy['skiprows']
        self.sources = self.get_source(task.fname)
        self.num_sources = 0
        self.current_source = None
        self.current_reader = None
        self.num_read = 0

    def get_source(self, paths):
        """
         Return a source generator. Each source is a named tuple
         wrapping the source input, file name and a boolean indicating
         if it requires closing.
        """
        shell = self.shell
        LineSource = namedtuple('LineSource', 'input close fname')

        def make_source(fname):
            try:
                ret = LineSource(input=open(fname, 'rb'), close=True, fname=fname)
                return ret
            except IOError, e:
                shell.printerr("Can't open %r for reading: %s" % (fname, e))
                return None

        if paths is None:
            self.printmsg("[Use \. on a line by itself to end input]")
            yield LineSource(input=shell.use_stdin_reader(prompt='[copy] ', until=r'\.'), close=False, fname='')
        else:
            for path in paths.split(','):
                path = path.strip()
                if os.path.isfile(path):
                    yield make_source(path)
                else:
                    for f in glob.glob(path):
                        yield (make_source(f))

    def start(self):
        self.next_source()

    @property
    def exhausted(self):
        return not self.current_reader

    def next_source(self):
        """
         Close the current source, if any, and open the next one. Return true
         if there is another source, false otherwise.
        """
        self.close_current_source()
        while self.current_source is None:
            try:
                self.current_source = self.sources.next()
                if self.current_source and self.current_source.fname:
                    self.num_sources += 1
            except StopIteration:
                return False

        if self.header:
            self.current_source.input.next()

        self.current_reader = csv.reader(self.current_source.input, **self.options.dialect)
        return True

    def close_current_source(self):
        if not self.current_source:
            return

        if self.current_source.close:
            self.current_source.input.close()
        elif self.shell.tty:
            print

        self.current_source = None
        self.current_reader = None

    def close(self):
        self.close_current_source()

    def read_rows(self, max_rows):
        if not self.current_reader:
            return []

        rows = []
        for i in xrange(min(max_rows, self.chunk_size)):
            try:
                row = self.current_reader.next()
                self.num_read += 1

                if 0 <= self.max_rows < self.num_read:
                    self.next_source()
                    break

                if self.num_read > self.skip_rows:
                    rows.append(row)

            except StopIteration:
                self.next_source()
                break

        return filter(None, rows)


class ImportErrors(object):
    """
    A small class for managing import errors
    """
    def __init__(self, task):
        self.shell = task.shell
        self.reader = task.reader
        self.options = task.options
        self.printmsg = task.printmsg
        self.max_attempts = self.options.copy['maxattempts']
        self.max_parse_errors = self.options.copy['maxparseerrors']
        self.max_insert_errors = self.options.copy['maxinserterrors']
        self.err_file = self.options.copy['errfile']
        self.parse_errors = 0
        self.insert_errors = 0
        self.num_rows_failed = 0

        if os.path.isfile(self.err_file):
            now = datetime.datetime.now()
            old_err_file = self.err_file + now.strftime('.%Y%m%d_%H%M%S')
            self.printmsg("Renaming existing %s to %s\n" % (self.err_file, old_err_file))
            os.rename(self.err_file, old_err_file)

    def max_exceeded(self):
        if self.insert_errors > self.max_insert_errors >= 0:
            self.shell.printerr("Exceeded maximum number of insert errors %d" % self.max_insert_errors)
            return True

        if self.parse_errors > self.max_parse_errors >= 0:
            self.shell.printerr("Exceeded maximum number of parse errors %d" % self.max_parse_errors)
            return True

        return False

    def add_failed_rows(self, rows):
        self.num_rows_failed += len(rows)

        with open(self.err_file, "a") as f:
            writer = csv.writer(f, **self.options.dialect)
            for row in rows:
                writer.writerow(row)

    def handle_error(self, err, batch):
        """
        Handle an error by printing the appropriate error message and incrementing the correct counter.
        Return true if we should retry this batch, false if the error is non-recoverable
        """
        shell = self.shell
        err = str(err)

        if self.is_parse_error(err):
            self.parse_errors += len(batch['rows'])
            self.add_failed_rows(batch['rows'])
            shell.printerr("Failed to import %d rows: %s -  given up without retries"
                           % (len(batch['rows']), err))
            return False
        else:
            self.insert_errors += len(batch['rows'])
            if batch['attempts'] < self.max_attempts:
                shell.printerr("Failed to import %d rows: %s -  will retry later, attempt %d of %d"
                               % (len(batch['rows']), err, batch['attempts'],
                                  self.max_attempts))
                return True
            else:
                self.add_failed_rows(batch['rows'])
                shell.printerr("Failed to import %d rows: %s -  given up after %d attempts"
                               % (len(batch['rows']), err, batch['attempts']))
                return False

    @staticmethod
    def is_parse_error(err):
        """
        We treat parse errors as unrecoverable and we have different global counters for giving up when
        a maximum has been reached. We consider value and type errors as parse errors as well since they
        are typically non recoverable.
        """
        return err.startswith('ValueError') or err.startswith('TypeError') or \
            err.startswith('ParseError') or err.startswith('IndexError')


class ImportTask(CopyTask):
    """
    A class to import data from .csv by instantiating one or more processes
    that work in parallel (ImportProcess).
    """
    def __init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file):
        CopyTask.__init__(self, shell, ks, table, columns, fname, opts, protocol_version, config_file, 'from')

        options = self.options
        self.ingest_rate = options.copy['ingestrate']
        self.max_attempts = options.copy['maxattempts']
        self.header = options.copy['header']
        self.skip_columns = [c.strip() for c in self.options.copy['skipcols'].split(',')]
        self.valid_columns = [c for c in self.columns if c not in self.skip_columns]
        self.table_meta = self.shell.get_table_meta(self.ks, self.table)
        self.batch_id = 0
        self.receive_meter = RateMeter(log_fcn=self.printmsg,
                                       update_interval=options.copy['reportfrequency'],
                                       log_file=options.copy['ratefile'])
        self.send_meter = RateMeter(log_fcn=None, update_interval=1)
        self.reader = ImportReader(self)
        self.import_errors = ImportErrors(self)
        self.retries = deque([])
        self.failed = 0
        self.succeeded = 0
        self.sent = 0

    def make_params(self):
        ret = CopyTask.make_params(self)
        ret['skip_columns'] = self.skip_columns
        ret['valid_columns'] = self.valid_columns
        return ret

    def run(self):
        shell = self.shell

        if self.options.unrecognized:
            shell.printerr('Unrecognized COPY FROM options: %s' % ', '.join(self.options.unrecognized.keys()))
            return

        if not self.valid_columns:
            shell.printerr("No column specified")
            return 0

        for c in self.table_meta.primary_key:
            if c.name not in self.valid_columns:
                shell.printerr("Primary key column '%s' missing or skipped" % (c.name,))
                return 0

        self.printmsg("\nStarting copy of %s.%s with columns %s." % (self.ks, self.table, self.valid_columns))

        try:
            self.reader.start()
            params = self.make_params()

            for i in range(self.num_processes):
                self.processes.append(ImportProcess(params))

            for process in self.processes:
                process.start()

            self.import_records()

        except Exception, exc:
            shell.printerr(str(exc))
            if shell.debug:
                traceback.print_exc()
            return 0
        finally:
            self.close()

    def close(self):
        CopyTask.close(self)
        self.reader.close()

    def import_records(self):
        """
        Keep on running until we have stuff to receive or send and until all processes are running.
        Send data (batches or retries) up to the max ingest rate. If we are waiting for stuff to
        receive check the incoming queue.
        """
        reader = self.reader

        while self.has_more_to_send(reader) or self.has_more_to_receive():
            if self.has_more_to_send(reader):
                self.send_batches(reader)

            if self.has_more_to_receive():
                self.receive()

            if self.import_errors.max_exceeded() or not self.all_processes_running():
                break

        if self.import_errors.num_rows_failed:
            self.shell.printerr("Failed to process %d rows; failed rows written to %s" %
                                (self.import_errors.num_rows_failed,
                                 self.import_errors.err_file))

        if not self.all_processes_running():
            self.shell.printerr("{} child process(es) died unexpectedly, aborting"
                                .format(self.num_processes - self.num_live_processes()))

        self.printmsg("\n%d rows imported from %d files in %s (%d skipped)." %
                      (self.receive_meter.get_total_records(),
                       self.reader.num_sources,
                       self.describe_interval(time.time() - self.time_start),
                       self.reader.skip_rows))

    def has_more_to_receive(self):
        return (self.succeeded + self.failed) < self.sent

    def has_more_to_send(self, reader):
        return (not reader.exhausted) or self.retries

    def all_processes_running(self):
        return self.num_live_processes() == self.num_processes

    def receive(self):
        start_time = time.time()

        while time.time() - start_time < 0.001:
            try:
                batch, err = self.inmsg.get(timeout=0.00001)

                if err is None:
                    self.succeeded += batch['imported']
                    self.receive_meter.increment(batch['imported'])
                else:
                    err = str(err)

                    if self.import_errors.handle_error(err, batch):
                        self.retries.append(self.reset_batch(batch))
                    else:
                        self.failed += len(batch['rows'])

            except Queue.Empty:
                pass

    def send_batches(self, reader):
        """
        Send one batch per worker process to the queue unless we have exceeded the ingest rate.
        In the export case we queue everything and let the worker processes throttle using max_requests,
        here we throttle using the ingest rate in the parent process because of memory usage concerns.

        When we have finished reading the csv file, then send any retries.
        """
        for _ in xrange(self.num_processes):
            max_rows = self.ingest_rate - self.send_meter.current_record
            if max_rows <= 0:
                self.send_meter.maybe_update()
                break

            if not reader.exhausted:
                rows = reader.read_rows(max_rows)
                if rows:
                    self.sent += self.send_batch(self.new_batch(rows))
            elif self.retries:
                batch = self.retries.popleft()
                if len(batch['rows']) <= max_rows:
                    self.send_batch(batch)
                else:
                    self.send_batch(self.split_batch(batch, batch['rows'][:max_rows]))
                    self.retries.append(self.split_batch(batch, batch['rows'][max_rows:]))
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
    def split_batch(batch, rows):
        return ImportTask.make_batch(batch['id'], rows, batch['attempts'])

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
        self.table = params['table']
        self.local_dc = params['local_dc']
        self.columns = params['columns']
        self.debug = params['debug']
        self.port = params['port']
        self.hostname = params['hostname']
        self.connect_timeout = params['connect_timeout']
        self.cql_version = params['cql_version']
        self.auth_provider = params['auth_provider']
        self.ssl = params['ssl']
        self.protocol_version = params['protocol_version']
        self.config_file = params['config_file']

        options = params['options']
        self.date_time_format = options.copy['dtformats']
        self.consistency_level = options.copy['consistencylevel']
        self.decimal_sep = options.copy['decimalsep']
        self.thousands_sep = options.copy['thousandssep']
        self.boolean_styles = options.copy['boolstyle']
        # Here we inject some failures for testing purposes, only if this environment variable is set
        if os.environ.get('CQLSH_COPY_TEST_FAILURES', ''):
            self.test_failures = json.loads(os.environ.get('CQLSH_COPY_TEST_FAILURES', ''))
        else:
            self.test_failures = None

    def printdebugmsg(self, text):
        if self.debug:
            sys.stdout.write(text + '\n')

    def close(self):
        self.printdebugmsg("Closing queues...")
        self.inmsg.close()
        self.outmsg.close()


class ExpBackoffRetryPolicy(RetryPolicy):
    """
    A retry policy with exponential back-off for read timeouts and write timeouts
    """
    def __init__(self, parent_process):
        RetryPolicy.__init__(self)
        self.max_attempts = parent_process.max_attempts
        self.printdebugmsg = parent_process.printdebugmsg

    def on_read_timeout(self, query, consistency, required_responses,
                        received_responses, data_retrieved, retry_num):
        return self._handle_timeout(consistency, retry_num)

    def on_write_timeout(self, query, consistency, write_type,
                         required_responses, received_responses, retry_num):
        return self._handle_timeout(consistency, retry_num)

    def _handle_timeout(self, consistency, retry_num):
        delay = self.backoff(retry_num)
        if delay > 0:
            self.printdebugmsg("Timeout received, retrying after %d seconds" % (delay,))
            time.sleep(delay)
            return self.RETRY, consistency
        elif delay == 0:
            self.printdebugmsg("Timeout received, retrying immediately")
            return self.RETRY, consistency
        else:
            self.printdebugmsg("Timeout received, giving up after %d attempts" % (retry_num + 1))
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
        session.default_fetch_size = export_process.options.copy['pagesize']
        session.default_timeout = export_process.options.copy['pagetimeout']

        export_process.printdebugmsg("Created connection to %s with page size %d and timeout %d seconds per page"
                                     % (cluster.contact_points, session.default_fetch_size, session.default_timeout))

        self.cluster = cluster
        self.session = session
        self.requests = 1
        self.lock = Lock()
        self.consistency_level = export_process.consistency_level

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
        return self.session.execute_async(SimpleStatement(query, consistency_level=self.consistency_level))

    def shutdown(self):
        self.cluster.shutdown()


class ExportProcess(ChildProcess):
    """
    An child worker process for the export task, ExportTask.
    """

    def __init__(self, params):
        ChildProcess.__init__(self, params=params, target=self.run)
        options = params['options']
        self.encoding = options.copy['encoding']
        self.float_precision = options.copy['float_precision']
        self.nullval = options.copy['nullval']
        self.max_attempts = options.copy['maxattempts']
        self.max_requests = options.copy['maxrequests']

        self.hosts_to_sessions = dict()
        self.formatters = dict()
        self.options = options

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

    @staticmethod
    def get_error_message(err, print_traceback=False):
        if isinstance(err, str):
            msg = err
        elif isinstance(err, BaseException):
            msg = "%s - %s" % (err.__class__.__name__, err)
            if print_traceback:
                traceback.print_exc(err)
        else:
            msg = str(err)
        return msg

    def report_error(self, err, token_range=None):
        msg = self.get_error_message(err, print_traceback=self.debug)
        self.printdebugmsg(msg)
        self.outmsg.put((token_range, Exception(msg)))

    def start_request(self, token_range, info):
        """
        Begin querying a range by executing an async query that
        will later on invoke the callbacks attached in attach_callbacks.
        """
        session = self.get_session(info['hosts'], token_range)
        if session:
            metadata = session.cluster.metadata.keyspaces[self.ks].tables[self.table]
            query = self.prepare_query(metadata.partition_key, token_range, info['attempts'])
            future = session.execute_async(query)
            self.attach_callbacks(token_range, future, session)

    def num_requests(self):
        return sum(session.num_requests() for session in self.hosts_to_sessions.values())

    def get_session(self, hosts, token_range):
        """
        We return a session connected to one of the hosts passed in, which are valid replicas for
        the token range. We sort replicas by favouring those without any active requests yet or with the
        smallest number of requests. If we fail to connect we report an error so that the token will
        be retried again later.

        :return: An ExportSession connected to the chosen host.
        """
        # sorted replicas favouring those with no connections yet
        hosts = sorted(hosts,
                       key=lambda hh: 0 if hh not in self.hosts_to_sessions else self.hosts_to_sessions[hh].requests)

        errors = []
        ret = None
        for host in hosts:
            try:
                ret = self.connect(host)
            except Exception, e:
                errors.append(self.get_error_message(e))

            if ret:
                if errors:
                    self.printdebugmsg("Warning: failed to connect to some replicas: %s" % (errors,))
                return ret

        self.report_error("Failed to connect to all replicas %s for %s, errors: %s" % (hosts, token_range, errors))
        return None

    def connect(self, host):
        if host in self.hosts_to_sessions.keys():
            session = self.hosts_to_sessions[host]
            session.add_request()
            return session

        new_cluster = Cluster(
            contact_points=(host,),
            port=self.port,
            cql_version=self.cql_version,
            protocol_version=self.protocol_version,
            auth_provider=self.auth_provider,
            ssl_options=ssl_settings(host, self.config_file) if self.ssl else None,
            load_balancing_policy=WhiteListRoundRobinPolicy([host]),
            default_retry_policy=ExpBackoffRetryPolicy(self),
            compression=None,
            control_connection_timeout=self.connect_timeout,
            connect_timeout=self.connect_timeout)
        session = ExportSession(new_cluster, self)
        self.hosts_to_sessions[host] = session
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
            writer = csv.writer(output, **self.options.dialect)

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
                         float_precision=self.float_precision, nullval=self.nullval, quote=False,
                         decimal_sep=self.decimal_sep, thousands_sep=self.thousands_sep,
                         boolean_styles=self.boolean_styles)

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
        query = 'SELECT %s FROM %s.%s' % (columnlist, protect_name(self.ks), protect_name(self.table))
        if start_token is not None or end_token is not None:
            query += ' WHERE'
        if start_token is not None:
            query += ' token(%s) > %s' % (pk_cols, start_token)
        if start_token is not None and end_token is not None:
            query += ' AND'
        if end_token is not None:
            query += ' token(%s) <= %s' % (pk_cols, end_token)
        return query


class ParseError(Exception):
    """ We failed to parse an import record """
    pass


class ImportConversion(object):
    """
    A class for converting strings to values when importing from csv, used by ImportProcess,
    the parent.
    """
    def __init__(self, parent, table_meta, statement):
        self.ks = parent.ks
        self.table = parent.table
        self.columns = parent.valid_columns
        self.nullval = parent.nullval
        self.printdebugmsg = parent.printdebugmsg
        self.decimal_sep = parent.decimal_sep
        self.thousands_sep = parent.thousands_sep
        self.boolean_styles = parent.boolean_styles
        self.date_time_format = parent.date_time_format.timestamp_format

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

        def convert_blob(v, **_):
            return bytearray.fromhex(v[2:])

        def convert_text(v, **_):
            return v

        def convert_uuid(v, **_):
            return UUID(v)

        def convert_bool(v, **_):
            return True if v.lower() == self.boolean_styles[0].lower() else False

        def get_convert_integer_fcn(adapter=int):
            """
            Return a slow and a fast integer conversion function depending on self.thousands_sep
            """
            if self.thousands_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.thousands_sep, ''))
            else:
                return lambda v, ct=cql_type: adapter(v)

        def get_convert_decimal_fcn(adapter=float):
            """
            Return a slow and a fast decimal conversion function depending on self.thousands_sep and self.decimal_sep
            """
            if self.thousands_sep and self.decimal_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.thousands_sep, '').replace(self.decimal_sep, '.'))
            elif self.thousands_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.thousands_sep, ''))
            elif self.decimal_sep:
                return lambda v, ct=cql_type: adapter(v.replace(self.decimal_sep, '.'))
            else:
                return lambda v, ct=cql_type: adapter(v)

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

        def convert_datetime(val, **_):
            try:
                tval = time.strptime(val, self.date_time_format)
                return timegm(tval) * 1e3  # scale seconds to millis for the raw value
            except ValueError:
                pass  # if it's not in the default format we try CQL formats

            m = p.match(val)
            if not m:
                raise ValueError("can't interpret %r as a date with this format: %s" % (val, self.date_time_format))

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

        def convert_date(v, **_):
            return Date(v)

        def convert_time(v, **_):
            return Time(v)

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

            self.printdebugmsg("Unknown type %s (%s) for val %s" % (ct, ct.typename, val))
            return val

        converters = {
            'blob': convert_blob,
            'decimal': get_convert_decimal_fcn(adapter=Decimal),
            'uuid': convert_uuid,
            'boolean': convert_bool,
            'tinyint': get_convert_integer_fcn(),
            'ascii': convert_text,
            'float': get_convert_decimal_fcn(),
            'double': get_convert_decimal_fcn(),
            'bigint': get_convert_integer_fcn(adapter=long),
            'int': get_convert_integer_fcn(),
            'varint': get_convert_integer_fcn(),
            'inet': convert_text,
            'counter': get_convert_integer_fcn(adapter=long),
            'timestamp': convert_datetime,
            'timeuuid': convert_uuid,
            'date': convert_date,
            'smallint': get_convert_integer_fcn(),
            'time': convert_time,
            'text': convert_text,
            'varchar': convert_text,
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
        def convert(n, val):
            try:
                return self.converters[self.columns[n]](val)
            except Exception, e:
                raise ParseError(e.message)

        ret = [None] * len(row)
        for i, val in enumerate(row):
            if val != self.nullval:
                ret[i] = convert(i, val)
            else:
                if i in self.primary_key_indexes:
                    raise ParseError(self.get_null_primary_key_message(i))

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
            try:
                c, v = self.columns[n], row[n]
                if v == self.nullval:
                    raise ParseError(self.get_null_primary_key_message(n))
                return self.cqltypes[c].serialize(self.converters[c](v), self.proto_version)
            except Exception, e:
                raise ParseError(e.message)

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

        self.skip_columns = params['skip_columns']
        self.valid_columns = params['valid_columns']
        self.skip_column_indexes = [i for i, c in enumerate(self.columns) if c in self.skip_columns]

        options = params['options']
        self.nullval = options.copy['nullval']
        self.max_attempts = options.copy['maxattempts']
        self.min_batch_size = options.copy['minbatchsize']
        self.max_batch_size = options.copy['maxbatchsize']
        self.ttl = options.copy['ttl']
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
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=self.local_dc)),
                ssl_options=ssl_settings(self.hostname, self.config_file) if self.ssl else None,
                default_retry_policy=ExpBackoffRetryPolicy(self),
                compression=None,
                control_connection_timeout=self.connect_timeout,
                connect_timeout=self.connect_timeout)

            self._session = cluster.connect(self.ks)
            self._session.default_timeout = None
        return self._session

    def run(self):
        try:
            table_meta = self.session.cluster.metadata.keyspaces[self.ks].tables[self.table]
            is_counter = ("counter" in [table_meta.columns[name].cql_type for name in self.valid_columns])

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
        query = 'UPDATE %s.%s SET %%s WHERE %%s' % (protect_name(self.ks), protect_name(self.table))

        # We prepare a query statement to find out the types of the partition key columns so we can
        # route the update query to the correct replicas. As far as I understood this is the easiest
        # way to find out the types of the partition columns, we will never use this prepared statement
        where_clause = ' AND '.join(['%s = ?' % (protect_name(c.name)) for c in table_meta.partition_key])
        select_query = 'SELECT * FROM %s.%s WHERE %s' % (protect_name(self.ks), protect_name(self.table), where_clause)
        conv = ImportConversion(self, table_meta, self.session.prepare(select_query))

        while True:
            batch = self.inmsg.get()
            try:
                for b in self.split_batches(batch, conv):
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
                                                        protect_name(self.table),
                                                        ', '.join(protect_names(self.valid_columns),),
                                                        ', '.join(['?' for _ in self.valid_columns]))
        if self.ttl >= 0:
            query += 'USING TTL %s' % (self.ttl,)

        query_statement = self.session.prepare(query)
        query_statement.consistency_level = self.consistency_level
        conv = ImportConversion(self, table_meta, query_statement)

        while True:
            batch = self.inmsg.get()
            try:
                for b in self.split_batches(batch, conv):
                    self.send_normal_batch(conv, query_statement, b)

            except Exception, exc:
                self.outmsg.put((batch, '%s - %s' % (exc.__class__.__name__, exc.message)))
                if self.debug:
                    traceback.print_exc(exc)

    def send_counter_batch(self, query_text, conv, batch):
        if self.test_failures and self.maybe_inject_failures(batch):
            return

        error_rows = []
        batch_statement = BatchStatement(batch_type=BatchType.COUNTER, consistency_level=self.consistency_level)

        for r in batch['rows']:
            row = self.filter_row_values(r)
            if len(row) != len(self.valid_columns):
                error_rows.append(row)
                continue

            where_clause = []
            set_clause = []
            for i, value in enumerate(row):
                if i in conv.primary_key_indexes:
                    where_clause.append("%s=%s" % (self.valid_columns[i], value))
                else:
                    set_clause.append("%s=%s+%s" % (self.valid_columns[i], self.valid_columns[i], value))

            full_query_text = query_text % (','.join(set_clause), ' AND '.join(where_clause))
            batch_statement.add(full_query_text)

        self.execute_statement(batch_statement, batch)

        if error_rows:
            self.outmsg.put((ImportTask.split_batch(batch, error_rows),
                            '%s - %s' % (ParseError.__name__, "Failed to parse one or more rows")))

    def send_normal_batch(self, conv, query_statement, batch):
        if self.test_failures and self.maybe_inject_failures(batch):
            return

        good_rows, converted_rows, errors = self.convert_rows(conv, batch['rows'])

        if converted_rows:
            try:
                statement = BatchStatement(batch_type=BatchType.UNLOGGED, consistency_level=self.consistency_level)
                for row in converted_rows:
                    statement.add(query_statement, row)
                self.execute_statement(statement, ImportTask.split_batch(batch, good_rows))
            except Exception, exc:
                self.err_callback(exc, ImportTask.split_batch(batch, good_rows))

        if errors:
            for msg, rows in errors.iteritems():
                self.outmsg.put((ImportTask.split_batch(batch, rows),
                                '%s - %s' % (ParseError.__name__, msg)))

    def convert_rows(self, conv, rows):
        """
        Try to convert each row. If conversion is OK then add the converted result to converted_rows
        and the original string to good_rows. Else add the original string to error_rows. Return the three
        arrays.
        """
        good_rows = []
        errors = defaultdict(list)
        converted_rows = []

        for r in rows:
            row = self.filter_row_values(r)
            if len(row) != len(self.valid_columns):
                msg = 'Invalid row length %d should be %d' % (len(row), len(self.valid_columns))
                errors[msg].append(row)
                continue

            try:
                converted_rows.append(conv.get_row_values(row))
                good_rows.append(row)
            except ParseError, err:
                errors[err.message].append(row)

        return good_rows, converted_rows, errors

    def filter_row_values(self, row):
        if not self.skip_column_indexes:
            return row

        return [v for i, v in enumerate(row) if i not in self.skip_column_indexes]

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
        Batch rows by partition key, if there are at least min_batch_size (2)
        rows with the same partition key. These batches can be as big as they want
        since this translates to a single insert operation server side.

        If there are less than min_batch_size rows for a partition, work out the
        first replica for this partition and add the rows to replica left-over rows.

        Then batch the left-overs of each replica up to max_batch_size.
        """
        rows_by_pk = defaultdict(list)
        errors = defaultdict(list)

        for row in batch['rows']:
            try:
                pk = conv.get_row_partition_key_values(row)
                rows_by_pk[pk].append(row)
            except ParseError, e:
                errors[e.message].append(row)

        if errors:
            for msg, rows in errors.iteritems():
                self.outmsg.put((ImportTask.split_batch(batch, rows),
                                 '%s - %s' % (ParseError.__name__, msg)))

        rows_by_replica = defaultdict(list)
        for pk, rows in rows_by_pk.iteritems():
            if len(rows) >= self.min_batch_size:
                yield ImportTask.make_batch(batch['id'], rows, batch['attempts'])
            else:
                replica = self.get_replica(pk)
                rows_by_replica[replica].extend(rows)

        for replica, rows in rows_by_replica.iteritems():
            for b in self.batches(rows, batch):
                yield b

    def get_replica(self, pk):
        """
        Return the first replica or the host we are already connected to if there are no local
        replicas that are up. We always use the first replica to match the replica chosen by the driver
        TAR, see TokenAwarePolicy.make_query_plan().
        """
        metadata = self.session.cluster.metadata
        replicas = filter(lambda r: r.is_up and r.datacenter == self.local_dc, metadata.get_replicas(self.ks, pk))
        ret = replicas[0].address if len(replicas) > 0 else self.hostname
        return ret

    def batches(self, rows, batch):
        """
        Split rows into batches of max_batch_size
        """
        for i in xrange(0, len(rows), self.max_batch_size):
            yield ImportTask.make_batch(batch['id'], rows[i:i + self.max_batch_size], batch['attempts'])

    def result_callback(self, _, batch):
        batch['imported'] = len(batch['rows'])
        batch['rows'] = []  # no need to resend these, just send the count in 'imported'
        self.outmsg.put((batch, None))

    def err_callback(self, response, batch):
        self.outmsg.put((batch, '%s - %s' % (response.__class__.__name__, response.message)))
        if self.debug:
            traceback.print_exc(response)


class RateMeter(object):

    def __init__(self, log_fcn, update_interval=0.25, log_file=''):
        self.log_fcn = log_fcn  # the function for logging, may be None to disable logging
        self.update_interval = update_interval  # how often we update in seconds
        self.log_file = log_file  # an optional file where to log statistics in addition to stdout
        self.start_time = time.time()  # the start time
        self.last_checkpoint_time = self.start_time  # last time we logged
        self.current_rate = 0.0  # rows per second
        self.current_record = 0  # number of records since we last updated
        self.total_records = 0   # total number of records

        if os.path.isfile(self.log_file):
            os.unlink(self.log_file)

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
        if not self.log_fcn:
            return

        output = 'Processed: %d rows; Rate: %7.0f rows/s; Avg. rate: %7.0f rows/s\r' % \
                 (self.total_records, self.current_rate, self.get_avg_rate())
        self.log_fcn(output, eol='\r')
        if self.log_file:
            with open(self.log_file, "a") as f:
                f.write(output + '\n')

    def get_total_records(self):
        self.update(time.time())
        self.log_message()
        return self.total_records
