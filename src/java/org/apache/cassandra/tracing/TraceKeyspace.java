/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.tracing;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.locator.SimpleStrategy;

public final class TraceKeyspace
{
    public static final String NAME = "system_traces";

    static final String SESSIONS_TABLE = "sessions";
    static final String EVENTS_TABLE = "events";

    private static final int DAY = (int) TimeUnit.DAYS.toSeconds(1);

    static final CFMetaData SessionsTable =
        compile(SESSIONS_TABLE, "tracing sessions",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "coordinator inet,"
                + "duration int,"
                + "parameters map<text, text>,"
                + "request text,"
                + "started_at timestamp,"
                + "PRIMARY KEY ((session_id)))")
                .defaultTimeToLive(DAY);

    static final CFMetaData EventsTable =
        compile(EVENTS_TABLE, "tracing events",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "event_id timeuuid,"
                + "activity text,"
                + "source inet,"
                + "source_elapsed int,"
                + "thread text,"
                + "PRIMARY KEY ((session_id), event_id))")
                .defaultTimeToLive(DAY);

    private static CFMetaData compile(String table, String comment, String cql)
    {
        return CFMetaData.compile(String.format(cql, table), NAME).comment(comment);
    }

    public static KSMetaData definition()
    {
        List<CFMetaData> tables = Arrays.asList(SessionsTable, EventsTable);
        return new KSMetaData(NAME, SimpleStrategy.class, ImmutableMap.of("replication_factor", "2"), true, tables);
    }
}
