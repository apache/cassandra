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

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.diag.DiagnosticEventTypes;
import org.apache.cassandra.schema.TableMetadata;

public class DiagnosticEventsTypes extends AbstractVirtualTable
{
    protected DiagnosticEventsTypes(String keyspace)
    {
        super(TableMetadata.builder(keyspace, "diagnostic_events_types")
                           .comment("List of diagnostic events and their event types avaliable for subscription.")
                           .kind(TableMetadata.Kind.VIRTUAL)
                           .addPartitionKeyColumn("class", UTF8Type.instance)
                           .addClusteringColumn("type", UTF8Type.instance)
                           .build());
    }

    @Override
    public DataSet data(DecoratedKey partitionKey)
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        String eventClass = UTF8Type.instance.getString(partitionKey.getKey());
        Set<Enum<?>> eventTypes = DiagnosticEventTypes.ALL_EVENTS.get(eventClass);

        if (eventTypes == null)
            return result;

        for (Enum<?> type : eventTypes)
            result.row(eventClass, type.name());

        return result;
    }

    @Override
    public DataSet data()
    {
        SimpleDataSet result = new SimpleDataSet(metadata());

        for (Map.Entry<String, Set<Enum<?>>> entry : DiagnosticEventTypes.ALL_EVENTS.entrySet())
        {
            for (Enum<?> type : entry.getValue())
                result.row(entry.getKey(), type.name());
        }

        return result;
    }
}
