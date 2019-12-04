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
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.partitions.PartitionUpdate;
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

    /**
     * Generation is used as a timestamp for automatic table creation on startup.
     * If you make any changes to the tables below, make sure to increment the
     * generation and document your change here.
     *
     * gen 1577836800000000: (3.0) maps to Jan 1 2020; an arbitrary cut-off date by which we assume no nodes older than 2.0.2
     *                       will ever start; see the note below for why this is necessary; actual change in 3.0:
     *                       removed default ttl, reduced bloom filter fp chance from 0.1 to 0.01.
     * gen 1577836800000001: (pre-)adds coordinator_port column to sessions and source_port column to events in 3.0, 3.11, 4.0
     *
     * * Until CASSANDRA-6016 (Oct 13, 2.0.2) and in all of 1.2, we used to create system_traces keyspace and
     *   tables in the same way that we created the purely local 'system' keyspace - using current time on node bounce
     *   (+1). For new definitions to take, we need to bump the generation further than that.
     */
    public static final long GENERATION = 1577836800000001L;

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
                + "coordinator_port int,"
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
                + "source_port int,"
                + "source_elapsed int,"
                + "thread text,"
                + "PRIMARY KEY ((session_id), event_id))");

    private static CFMetaData compile(String name, String description, String schema)
    {
        return CFMetaData.compile(String.format(schema, name), SchemaConstants.TRACE_KEYSPACE_NAME)
                         .comment(description);
    }

    public static KeyspaceMetadata metadata()
    {
        return KeyspaceMetadata.create(SchemaConstants.TRACE_KEYSPACE_NAME, KeyspaceParams.simple(2), Tables.of(Sessions, Events));
    }

    static Mutation makeStartSessionMutation(ByteBuffer sessionId,
                                             InetAddress client,
                                             Map<String, String> parameters,
                                             String request,
                                             long startedAt,
                                             String command,
                                             int ttl)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(Sessions, sessionId);
        builder.row()
               .ttl(ttl)
               .add("client", client)
               .add("coordinator", FBUtilities.getBroadcastAddress())
               .add("request", request)
               .add("started_at", new Date(startedAt))
               .add("command", command)
               .appendAll("parameters", parameters);

        return builder.buildAsMutation();
    }

    static Mutation makeStopSessionMutation(ByteBuffer sessionId, int elapsed, int ttl)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(Sessions, sessionId);
        builder.row()
               .ttl(ttl)
               .add("duration", elapsed);
        return builder.buildAsMutation();
    }

    static Mutation makeEventMutation(ByteBuffer sessionId, String message, int elapsed, String threadName, int ttl)
    {
        PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(Events, sessionId);
        Row.SimpleBuilder rowBuilder = builder.row(UUIDGen.getTimeUUID())
                                              .ttl(ttl);

        rowBuilder.add("activity", message)
                  .add("source", FBUtilities.getBroadcastAddress())
                  .add("thread", threadName);

        if (elapsed >= 0)
            rowBuilder.add("source_elapsed", elapsed);

        return builder.buildAsMutation();
    }
}
