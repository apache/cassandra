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
package org.apache.cassandra.db;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientState;

public interface IMutation
{
    long MAX_MUTATION_SIZE = DatabaseDescriptor.getMaxMutationSize();

    void apply();
    String getKeyspaceName();
    Collection<TableId> getTableIds();
    DecoratedKey key();
    long getTimeout(TimeUnit unit);
    String toString(boolean shallow);
    Collection<PartitionUpdate> getPartitionUpdates();
    boolean hasUpdateForTable(TableId tableId);
    Supplier<Mutation> hintOnFailure();

    default void validateIndexedColumns(ClientState state)
    {
        for (PartitionUpdate pu : getPartitionUpdates())
            pu.validateIndexedColumns(state);
    }

    /**
     * Validates size of mutation does not exceed {@link DatabaseDescriptor#getMaxMutationSize()}.
     *
     * @param version the MessagingService version the mutation is being serialized for.
     *                see {@link org.apache.cassandra.net.MessagingService#current_version}
     * @param overhead overhadd to add for mutation size to validate. Pass zero if not required but not a negative value.
     * @throws MutationExceededMaxSizeException if {@link DatabaseDescriptor#getMaxMutationSize()} is exceeded
      */
    void validateSize(int version, int overhead);

    /**
     * Computes the total data size of the specified mutations.
     * @param mutations the mutations
     * @return the total data size of the specified mutations
     */
    static long dataSize(Collection<? extends IMutation> mutations)
    {
        long size = 0;
        for (IMutation mutation : mutations)
        {
            for (PartitionUpdate update : mutation.getPartitionUpdates())
                size += update.dataSize();
        }
        return size;
    }

    /**
     * True if this mutation is being applied by a transaction system or doesn't need to be
     * and conflicts between this mutation and transactions systems that are managing all or part of this table
     * should be assumed to be handled already (by either Paxos or Accord) and the mutation should be applied.
     *
     * This causes mutations against tables to fail if they are from a non-transaction sub-system such as mutations,
     * logged and unlogged batches, hints, and read repair against tables that are being managed by a transaction system
     * like Accord that can't safely read data that is written non-transactionally.
     *
     */
    default PotentialTxnConflicts potentialTxnConflicts()
    {
        return PotentialTxnConflicts.DISALLOW;
    }

    // Construct replacement mutation that is identical except it only includes updates for the specified tables
    @Nullable IMutation filter(Predicate<TableId> predicate);

    default void clearCachedSerializationsForRetry() {}
}
