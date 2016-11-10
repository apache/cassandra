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

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

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

    public static final String SESSIONS = "sessions";
    public static final String EVENTS = "events";

    private static final CFMetaData Sessions =
        compile(SESSIONS,
                "tracing sessions",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "command text,"
                + "client inet,"
                + "coordinator inet,"
                + "duration int,"
                + "parameters map<text, text>,"
                + "request text,"
                + "started_at timestamp,"
                + "PRIMARY KEY ((session_id)))");

    private static final CFMetaData Events =
        compile(EVENTS,
                "tracing events",
                "CREATE TABLE %s ("
                + "session_id uuid,"
                + "event_id timeuuid,"
                + "activity text,"
                + "source inet,"
                + "source_elapsed int,"
                + "thread text,"
                + "PRIMARY KEY ((session_id), event_id))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), NAME)
                         .comment(description);
    }

    public static KSMetaData definition()
    {
        List<CFMetaData> tables = Arrays.asList(Sessions, Events);
        return new KSMetaData(NAME, SimpleStrategy.class, ImmutableMap.of("replication_factor", "2"), true, tables);
    }

    static Mutation makeStartSessionMutation(ByteBuffer sessionId,
                                             InetAddress client,
                                             Map<String, String> parameters,
                                             String request,
                                             long startedAt,
                                             String command,
                                             int ttl)
    {
        Mutation mutation = new Mutation(NAME, sessionId);
        ColumnFamily cells = mutation.addOrGet(TraceKeyspace.Sessions);

        CFRowAdder adder = new CFRowAdder(cells, cells.metadata().comparator.builder().build(), FBUtilities.timestampMicros(), ttl);
        adder.add("client", client)
             .add("coordinator", FBUtilities.getBroadcastAddress())
             .add("request", request)
             .add("started_at", new Date(startedAt))
             .add("command", command);
        for (Map.Entry<String, String> entry : parameters.entrySet())
            adder.addMapEntry("parameters", entry.getKey(), entry.getValue());

        return mutation;
    }

    static Mutation makeStopSessionMutation(ByteBuffer sessionId, int elapsed, int ttl)
    {
        Mutation mutation = new Mutation(NAME, sessionId);
        ColumnFamily cells = mutation.addOrGet(Sessions);

        CFRowAdder adder = new CFRowAdder(cells, cells.metadata().comparator.builder().build(), FBUtilities.timestampMicros(), ttl);
        adder.add("duration", elapsed);

        return mutation;
    }

    static Mutation makeEventMutation(ByteBuffer sessionId, String message, int elapsed, String threadName, int ttl)
    {
        Mutation mutation = new Mutation(NAME, sessionId);
        ColumnFamily cells = mutation.addOrGet(Events);

        CFRowAdder adder = new CFRowAdder(cells, cells.metadata().comparator.make(UUIDGen.getTimeUUID()), FBUtilities.timestampMicros(), ttl);
        adder.add("activity", message)
             .add("source", FBUtilities.getBroadcastAddress())
             .add("thread", threadName);
        if (elapsed >= 0)
            adder.add("source_elapsed", elapsed);

        return mutation;
    }
}
