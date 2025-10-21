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

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.commitlog.CommitLogSegment;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.virtual.VirtualMutation;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;

/**
 * Utility class to collect updates.
 */
final class SingleTableSinglePartitionUpdatesCollector implements UpdatesCollector
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
     * the partition update builders per key
     */
    private PartitionUpdate.Builder builder;

    /**
     * if it is a counter table, we will set this
     */
    private ConsistencyLevel counterConsistencyLevel = null;

    SingleTableSinglePartitionUpdatesCollector(TableMetadata metadata, RegularAndStaticColumns updatedColumns)
    {
        this.metadata = metadata;
        this.updatedColumns = updatedColumns;
    }

    public PartitionUpdate.Builder getPartitionUpdateBuilder(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency)
    {
        if (metadata.isCounter())
            counterConsistencyLevel = consistency;
        if (builder == null)
        {
            builder = new PartitionUpdate.Builder(metadata, dk, updatedColumns, 1);
        }
        return builder;
    }

    /**
     * Returns a collection containing all the mutations.
     */
    @Override
    public List<IMutation> toMutations(ClientState state, PotentialTxnConflicts potentialTxnConflicts)
    {
        // it is possible that a modification statement does not create any mutations
        // for example: DELETE FROM some_table WHERE part_key = 1 AND clust_key < 3 AND clust_key > 5
        if (builder == null)
            return Collections.emptyList();
        return Collections.singletonList(createMutation(state, builder, potentialTxnConflicts));
    }

    private IMutation createMutation(ClientState state, PartitionUpdate.Builder builder, PotentialTxnConflicts potentialTxnConflicts)
    {
        IMutation mutation;

        if (metadata.isVirtual())
            mutation = new VirtualMutation(builder.build());
        else if (metadata.isCounter())
            mutation = new CounterMutation(new Mutation(builder.build()), counterConsistencyLevel);
        else
            mutation = new Mutation(builder.build(), potentialTxnConflicts);

        mutation.validateIndexedColumns(state);
        mutation.validateSize(MessagingService.current_version, CommitLogSegment.ENTRY_OVERHEAD_SIZE);
        return mutation;
    }
}