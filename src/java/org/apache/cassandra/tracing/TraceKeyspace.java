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
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

import static java.lang.String.format;

public final class TraceKeyspace
{
    private TraceKeyspace()
    {
    }

    public static final String SESSIONS = "sessions";
    public static final String EVENTS = "events";

    private static final TableMetadata Sessions =
        parse(SESSIONS,
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

    private static final TableMetadata Events =
        parse(EVENTS,
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

    private static TableMetadata parse(String table, String description, String cql)
    {
        return CreateTableStatement.parse(format(cql, table), SchemaConstants.TRACE_KEYSPACE_NAME)
                                   .id(TableId.forSystemTable(SchemaConstants.TRACE_KEYSPACE_NAME, table))
                                   .dcLocalReadRepairChance(0.0)
                                   .gcGraceSeconds(0)
                                   .memtableFlushPeriod((int) TimeUnit.HOURS.toMillis(1))
                                   .comment(description)
                                   .build();
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
               .add("coordinator", FBUtilities.getBroadcastAddressAndPort().address)
               .add("coordinator_port", FBUtilities.getBroadcastAddressAndPort().port)
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
                  .add("source", FBUtilities.getBroadcastAddressAndPort().address)
                  .add("source_port", FBUtilities.getBroadcastAddressAndPort().port)
                  .add("thread", threadName);

        if (elapsed >= 0)
            rowBuilder.add("source_elapsed", elapsed);

        return builder.buildAsMutation();
    }
}
