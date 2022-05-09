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
import java.util.List;
import java.util.Map;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Maps;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.VirtualMutation;
import org.apache.cassandra.net.MessagingService;
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
     * The number of updated rows per key.
     */
    private final HashMultiset<ByteBuffer> perPartitionKeyCounts;

    /**
     * the partition update builders per key
     */
    private final Map<ByteBuffer, PartitionUpdate.Builder> puBuilders;

    /**
     * if it is a counter table, we will set this
     */
    private ConsistencyLevel counterConsistencyLevel = null;

    SingleTableUpdatesCollector(TableMetadata metadata, RegularAndStaticColumns updatedColumns, HashMultiset<ByteBuffer> perPartitionKeyCounts)
    {
        this.metadata = metadata;
        this.updatedColumns = updatedColumns;
        this.perPartitionKeyCounts = perPartitionKeyCounts;
        this.puBuilders = Maps.newHashMapWithExpectedSize(perPartitionKeyCounts.size());
    }

    public PartitionUpdate.Builder getPartitionUpdateBuilder(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency)
    {
        if (metadata.isCounter())
            counterConsistencyLevel = consistency;
        PartitionUpdate.Builder builder = puBuilders.get(dk.getKey());
        if (builder == null)
        {
            builder = new PartitionUpdate.Builder(metadata, dk, updatedColumns, perPartitionKeyCounts.count(dk.getKey()));
            puBuilders.put(dk.getKey(), builder);
        }
        return builder;
    }

    /**
     * Returns a collection containing all the mutations.
     * @return a collection containing all the mutations.
     */
    public List<IMutation> toMutations()
    {
        List<IMutation> ms = new ArrayList<>(puBuilders.size());
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
            mutation.validateSize(MessagingService.current_version, CommitLogSegment.ENTRY_OVERHEAD_SIZE);
            ms.add(mutation);
        }

        return ms;
    }
}
