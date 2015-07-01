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

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public final class TraceKeyspace
{
    private TraceKeyspace()
    {
    }

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

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(NAME, KeyspaceParams.simple(2), Tables.of(Sessions, Events));
    }

    static Mutation makeStartSessionMutation(ByteBuffer sessionId,
                                             InetAddress client,
                                             Map<String, String> parameters,
                                             String request,
                                             long startedAt,
                                             String command,
                                             int ttl)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Sessions, FBUtilities.timestampMicros(), ttl, sessionId)
                                 .clustering()
                                 .add("client", client)
                                 .add("coordinator", FBUtilities.getBroadcastAddress())
                                 .add("request", request)
                                 .add("started_at", new Date(startedAt))
                                 .add("command", command);

        for (Map.Entry<String, String> entry : parameters.entrySet())
            adder.addMapEntry("parameters", entry.getKey(), entry.getValue());
        return adder.build();
    }

    static Mutation makeStopSessionMutation(ByteBuffer sessionId, int elapsed, int ttl)
    {
        return new RowUpdateBuilder(Sessions, FBUtilities.timestampMicros(), ttl, sessionId)
               .clustering()
               .add("duration", elapsed)
               .build();
    }

    static Mutation makeEventMutation(ByteBuffer sessionId, String message, int elapsed, String threadName, int ttl)
    {
        RowUpdateBuilder adder = new RowUpdateBuilder(Events, FBUtilities.timestampMicros(), ttl, sessionId)
                                 .clustering(UUIDGen.getTimeUUID());
        adder.add("activity", message);
        adder.add("source", FBUtilities.getBroadcastAddress());
        adder.add("thread", threadName);
        if (elapsed >= 0)
            adder.add("source_elapsed", elapsed);
        return adder.build();
    }
}
