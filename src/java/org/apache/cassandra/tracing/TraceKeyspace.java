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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.CFRowAdder;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public final class TraceKeyspace
{
    public static final String NAME = "system_traces";

    private static final String SESSIONS_TABLE = "sessions";
    private static final String EVENTS_TABLE = "events";

    private static final int DAY = (int) TimeUnit.DAYS.toSeconds(1);

    private static final CFMetaData SessionsTable =
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

    private static final CFMetaData EventsTable =
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

    static Mutation toStopSessionMutation(ByteBuffer sessionId, int elapsed)
    {
        Mutation mutation = new Mutation(NAME, sessionId);
        ColumnFamily cells = mutation.addOrGet(SessionsTable);

        CFRowAdder adder = new CFRowAdder(cells, cells.metadata().comparator.builder().build(), FBUtilities.timestampMicros());
        adder.add("duration", elapsed);

        return mutation;
    }

    static Mutation toStartSessionMutation(ByteBuffer sessionId, Map<String, String> parameters, String request, long startedAt)
    {
        Mutation mutation = new Mutation(NAME, sessionId);
        ColumnFamily cells = mutation.addOrGet(TraceKeyspace.SessionsTable);

        CFRowAdder adder = new CFRowAdder(cells, cells.metadata().comparator.builder().build(), FBUtilities.timestampMicros());
        adder.add("coordinator", FBUtilities.getBroadcastAddress());
        for (Map.Entry<String, String> entry : parameters.entrySet())
            adder.addMapEntry("parameters", entry.getKey(), entry.getValue());
        adder.add("request", request);
        adder.add("started_at", new Date(startedAt));

        return mutation;
    }

    static Mutation toEventMutation(ByteBuffer sessionId, String message, int elapsed, String threadName)
    {
        Mutation mutation = new Mutation(NAME, sessionId);
        ColumnFamily cells = mutation.addOrGet(EventsTable);

        CFRowAdder adder = new CFRowAdder(cells, cells.metadata().comparator.make(UUIDGen.getTimeUUID()), FBUtilities.timestampMicros());
        adder.add("activity", message);
        adder.add("source", FBUtilities.getBroadcastAddress());
        if (elapsed >= 0)
            adder.add("source_elapsed", elapsed);
        adder.add("thread", threadName);

        return mutation;
    }
}
