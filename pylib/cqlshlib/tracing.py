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

import time
from cql.cqltypes import UTF8Type, InetAddressType, Int32Type
from cqlshlib.displaying import MAGENTA

TRACING_KS = 'system_traces'
SESSIONS_CF = 'sessions'
EVENTS_CF = 'events'

def print_trace_session(shell, cursor, session_id):
    rows  = fetch_trace_session(cursor, session_id)
    names = ['activity', 'timestamp', 'source', 'source_elapsed']
    types = [UTF8Type, UTF8Type, InetAddressType, Int32Type]

    formatted_names = [shell.myformat_colname(name, UTF8Type) for name in names]
    formatted_values = [map(shell.myformat_value, row, types) for row in rows]

    shell.writeresult('')
    shell.writeresult('Tracing session: ', color=MAGENTA, newline=False)
    shell.writeresult(session_id)
    shell.writeresult('')
    shell.print_formatted_result(formatted_names, formatted_values)
    shell.writeresult('')

def fetch_trace_session(cursor, session_id):
    cursor.execute("SELECT request, coordinator, started_at, duration "
                   "FROM %s.%s "
                   "WHERE session_id = %s" % (TRACING_KS, SESSIONS_CF, session_id),
                   consistency_level='ONE')
    (request, coordinator, started_at, duration) = cursor.fetchone()

    cursor.execute("SELECT activity, event_id, source, source_elapsed "
                   "FROM %s.%s "
                   "WHERE session_id = %s" % (TRACING_KS, EVENTS_CF, session_id),
                   consistency_level='ONE')
    events = cursor.fetchall()

    rows = []
    # append header row (from sessions table).
    rows.append([request, format_timestamp(started_at), coordinator, 0])
    # append main rows (from events table).
    for activity, event_id, source, source_elapsed in events:
        rows.append([activity, format_timeuuid(event_id), source, source_elapsed])
    # append footer row (from sessions table).
    finished_at = started_at + (duration / 1000000.)
    rows.append(['Request complete', format_timestamp(finished_at), coordinator, duration])

    return rows

def format_timestamp(value):
    return format_time(int(value * 1000))

def format_timeuuid(value):
    return format_time((value.get_time() - 0x01b21dd213814000) / 10000)

def format_time(millis):
    s, ms = divmod(millis, 1000)
    return time.strftime('%H:%M:%S', time.localtime(s)) + ',' + str(ms).rjust(3, '0')
