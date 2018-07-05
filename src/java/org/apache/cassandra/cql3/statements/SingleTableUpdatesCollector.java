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
package org.apache.cassandra.cql3.statements;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.VirtualMutation;
import org.apache.cassandra.schema.TableMetadata;

/**
 * Utility class to collect updates.
 */
final class SingleTableUpdatesCollector implements UpdatesCollector
{
    /**
     * the table to be updated
     */
    private final TableMetadata metadata;

    /**
     * the columns to update
     */
    private final RegularAndStaticColumns updatedColumns;

    /**
     * The estimated number of updated row.
     */
    private final int updatedRows;

    /**
     * the partition update builders per key
     */
    private final Map<ByteBuffer, PartitionUpdate.Builder> puBuilders = new HashMap<>();

    /**
     * if it is a counter table, we will set this
     */
    private ConsistencyLevel counterConsistencyLevel = null;

    SingleTableUpdatesCollector(TableMetadata metadata, RegularAndStaticColumns updatedColumns, int updatedRows)
    {
        this.metadata = metadata;
        this.updatedColumns = updatedColumns;
        this.updatedRows = updatedRows;
    }

    public PartitionUpdate.Builder getPartitionUpdateBuilder(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency)
    {
        if (metadata.isCounter())
            counterConsistencyLevel = consistency;
        return puBuilders.computeIfAbsent(dk.getKey(), (k) -> new PartitionUpdate.Builder(metadata, dk, updatedColumns, updatedRows));
    }

    /**
     * Returns a collection containing all the mutations.
     * @return a collection containing all the mutations.
     */
    public List<IMutation> toMutations()
    {
        List<IMutation> ms = new ArrayList<>();
        for (PartitionUpdate.Builder builder : puBuilders.values())
        {
            IMutation mutation;

            if (metadata.isVirtual())
                mutation = new VirtualMutation(builder.build());
            else if (metadata.isCounter())
                mutation = new CounterMutation(new Mutation(builder.build()), counterConsistencyLevel);
            else
                mutation = new Mutation(builder.build());

            mutation.validateIndexedColumns();
            ms.add(mutation);
        }

        return ms;
    }
}
