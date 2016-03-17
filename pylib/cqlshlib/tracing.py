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

from cqlshlib.displaying import MAGENTA
from datetime import datetime
from formatting import CqlType
import time
from cassandra.query import QueryTrace, TraceUnavailable


def print_trace_session(shell, session, session_id, partial_session=False):
    """
    Lookup a trace by session and trace session ID, then print it.
    """
    trace = QueryTrace(session_id, session)
    try:
        wait_for_complete = not partial_session
        trace.populate(wait_for_complete=wait_for_complete)
    except TraceUnavailable:
        shell.printerr("Session %s wasn't found." % session_id)
    else:
        print_trace(shell, trace)


def print_trace(shell, trace):
    """
    Print an already populated cassandra.query.QueryTrace instance.
    """
    rows = make_trace_rows(trace)
    if not rows:
        shell.printerr("No rows for session %s found." % (trace.trace_id,))
        return
    names = ['activity', 'timestamp', 'source', 'source_elapsed', 'client']

    formatted_names = map(shell.myformat_colname, names)
    formatted_values = [map(shell.myformat_value, row) for row in rows]

    shell.writeresult('')
    shell.writeresult('Tracing session: ', color=MAGENTA, newline=False)
    shell.writeresult(trace.trace_id)
    shell.writeresult('')
    shell.print_formatted_result(formatted_names, formatted_values)
    shell.writeresult('')


def make_trace_rows(trace):
    if not trace.events:
        return []

    rows = [[trace.request_type, str(datetime_from_utc_to_local(trace.started_at)), trace.coordinator, 0, trace.client]]

    # append main rows (from events table).
    for event in trace.events:
        rows.append(["%s [%s]" % (event.description, event.thread_name),
                     str(datetime_from_utc_to_local(event.datetime)),
                     event.source,
                     event.source_elapsed.microseconds if event.source_elapsed else "--",
                     trace.client])
    # append footer row (from sessions table).
    if trace.duration:
        finished_at = (datetime_from_utc_to_local(trace.started_at) + trace.duration)
        rows.append(['Request complete', str(finished_at), trace.coordinator, trace.duration.microseconds, trace.client])
    else:
        finished_at = trace.duration = "--"

    return rows


def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset
