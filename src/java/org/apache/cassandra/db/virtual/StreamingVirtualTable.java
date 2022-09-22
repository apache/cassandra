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
package org.apache.cassandra.db.virtual;

import java.util.Date;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.streaming.StreamManager;
import org.apache.cassandra.streaming.StreamingState;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.cql3.statements.schema.CreateTableStatement.parse;

public class StreamingVirtualTable extends AbstractVirtualTable
{
    public StreamingVirtualTable(String keyspace)
    {
        super(parse("CREATE TABLE streaming (" +
                    "  id timeuuid,\n" +
                    "  follower boolean,\n" +
                    "  operation text, \n" +
                    "  peers frozen<list<text>>,\n" +
                    "  status text,\n" +
                    "  progress_percentage float,\n" +
                    "  last_updated_at timestamp,\n" +
                    "  duration_millis bigint,\n" +
                    "  failure_cause text,\n" +
                    "  success_message text,\n" +
                    "\n" +
                    StreamingState.Sessions.columns() +
                    "\n" +
                    stateColumns() +
                    "\n" +
                    "PRIMARY KEY ((id))" +
                    ")", keyspace)
              .kind(TableMetadata.Kind.VIRTUAL)
              .build());
    }

    private static String stateColumns()
    {
        StringBuilder sb = new StringBuilder();
        for (StreamingState.Status state : StreamingState.Status.values())
            sb.append("  status_").append(state.name().toLowerCase()).append("_timestamp timestamp,\n");
        return sb.toString();
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());
        StreamManager.instance.getStreamingStates()
                              .forEach(s -> updateDataSet(result, s));
        return result;
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        TimeUUID id = TimeUUIDType.instance.compose(partitionKey.getKey());
        SimpleDataSet result = new SimpleDataSet(metadata());
        StreamingState state = StreamManager.instance.getStreamingState(id);
        if (state != null)
            updateDataSet(result, state);
        return result;
    }

    private void updateDataSet(SimpleDataSet ds, StreamingState state)
    {
        ds.row(state.id());
        ds.column("last_updated_at", new Date(state.lastUpdatedAtMillis())); // read early to see latest state
        ds.column("follower", state.follower());
        ds.column("operation", state.operation().getDescription());
        ds.column("peers", state.peers().stream().map(Object::toString).collect(Collectors.toList()));
        ds.column("status", state.status().name().toLowerCase());
        ds.column("progress_percentage", round(state.progress() * 100));
        ds.column("duration_millis", state.durationMillis());
        ds.column("failure_cause", state.failureCause());
        ds.column("success_message", state.successMessage());
        for (Map.Entry<StreamingState.Status, Long> e : state.stateTimesMillis().entrySet())
            ds.column("status_" + e.getKey().name().toLowerCase() + "_timestamp", new Date(e.getValue()));

        state.sessions().update(ds);
    }

    static float round(float value)
    {
        return Math.round(value * 100) / 100;
    }
}
