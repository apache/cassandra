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
import java.util.*;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.db.virtual.VirtualMutation;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.partitions.PartitionUpdate;

/**
 * Utility class to collect updates.
 *
 * <p>In a batch statement we don't want to recreate mutations every time as this is particularly inefficient when
 * applying multiple batch to the same partition (see #6737). </p>
 *
 */
final class BatchUpdatesCollector implements UpdatesCollector
{
    /**
     * The columns that will be updated for each table (keyed by the table ID).
     */
    private final Map<TableId, RegularAndStaticColumns> updatedColumns;

    /**
     * The estimated number of updated row.
     */
    private final int updatedRows;

    /**
     * The mutations per keyspace.
     */
    private final Map<String, Map<ByteBuffer, IMutationBuilder>> mutationBuilders = new HashMap<>();

    BatchUpdatesCollector(Map<TableId, RegularAndStaticColumns> updatedColumns, int updatedRows)
    {
        super();
        this.updatedColumns = updatedColumns;
        this.updatedRows = updatedRows;
    }

    /**
     * Gets the <code>PartitionUpdate.Builder</code> for the specified column family and key. If the builder does not
     * exist it will be created.
     *
     * @param metadata the column family meta data
     * @param dk the partition key
     * @param consistency the consistency level
     * @return the <code>PartitionUpdate.Builder</code> for the specified column family and key
     */
    public PartitionUpdate.Builder getPartitionUpdateBuilder(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency)
    {
        IMutationBuilder mut = getMutationBuilder(metadata, dk, consistency);
        PartitionUpdate.Builder upd = mut.get(metadata.id);
        if (upd == null)
        {
            RegularAndStaticColumns columns = updatedColumns.get(metadata.id);
            assert columns != null;
            upd = new PartitionUpdate.Builder(metadata, dk, columns, updatedRows);
            mut.add(upd);
        }
        return upd;
    }

    private IMutationBuilder getMutationBuilder(TableMetadata metadata, DecoratedKey dk, ConsistencyLevel consistency)
    {
        return keyspaceMap(metadata.keyspace).computeIfAbsent(dk.getKey(), k -> makeMutationBuilder(metadata, dk, consistency));
    }

    private IMutationBuilder makeMutationBuilder(TableMetadata metadata, DecoratedKey partitionKey, ConsistencyLevel cl)
    {
        if (metadata.isVirtual())
        {
            return new VirtualMutationBuilder(metadata.keyspace, partitionKey);
        }
        else
        {
            MutationBuilder builder = new MutationBuilder(metadata.keyspace, partitionKey);
            return metadata.isCounter() ? new CounterMutationBuilder(builder, cl) : builder;
        }
    }

    /**
     * Returns a collection containing all the mutations.
     * @return a collection containing all the mutations.
     */
    public Collection<IMutation> toMutations()
    {
        //TODO: The case where all statement where on the same keyspace is pretty common, optimize for that?
        List<IMutation> ms = new ArrayList<>();
        for (Map<ByteBuffer, IMutationBuilder> ksMap : mutationBuilders.values())
        {
            for (IMutationBuilder builder : ksMap.values())
            {
                IMutation mutation = builder.build();
                mutation.validateIndexedColumns();
                ms.add(mutation);
            }
        }
        return ms;
    }

    /**
     * Returns the key-mutation mappings for the specified keyspace.
     *
     * @param ksName the keyspace name
     * @return the key-mutation mappings for the specified keyspace.
     */
    private Map<ByteBuffer, IMutationBuilder> keyspaceMap(String ksName)
    {
        return mutationBuilders.computeIfAbsent(ksName, k -> new HashMap<>());
    }

    private interface IMutationBuilder
    {
        /**
         * Add a new PartitionUpdate builder to this mutation builder
         * @param builder the builder to add
         * @return this
         */
        IMutationBuilder add(PartitionUpdate.Builder builder);

        /**
         * Build the immutable mutation
         */
        IMutation build();

        /**
         * Get the builder for the given tableId
         */
        PartitionUpdate.Builder get(TableId tableId);
    }

    private static class MutationBuilder implements IMutationBuilder
    {
        private final HashMap<TableId, PartitionUpdate.Builder> modifications = new HashMap<>();
        private final DecoratedKey key;
        private final String keyspaceName;
        private final long createdAt = System.currentTimeMillis();

        private MutationBuilder(String keyspaceName, DecoratedKey key)
        {
            this.keyspaceName = keyspaceName;
            this.key = key;
        }

        public MutationBuilder add(PartitionUpdate.Builder updateBuilder)
        {
            assert updateBuilder != null;
            assert updateBuilder.partitionKey().getPartitioner() == key.getPartitioner();
            PartitionUpdate.Builder prev = modifications.put(updateBuilder.metadata().id, updateBuilder);
            if (prev != null)
                // developer error
                throw new IllegalArgumentException("Table " + updateBuilder.metadata().name + " already has modifications in this mutation: " + prev);
            return this;
        }

        public Mutation build()
        {
            ImmutableMap.Builder<TableId, PartitionUpdate> updates = new ImmutableMap.Builder<>();
            for (Map.Entry<TableId, PartitionUpdate.Builder> updateEntry : modifications.entrySet())
            {
                PartitionUpdate update = updateEntry.getValue().build();
                updates.put(updateEntry.getKey(), update);
            }
            return new Mutation(keyspaceName, key, updates.build(), createdAt);
        }

        public PartitionUpdate.Builder get(TableId tableId)
        {
            return modifications.get(tableId);
        }

        public DecoratedKey key()
        {
            return key;
        }

        public boolean isEmpty()
        {
            return modifications.isEmpty();
        }

        public String getKeyspaceName()
        {
            return keyspaceName;
        }
    }

    private static class CounterMutationBuilder implements IMutationBuilder
    {
        private final MutationBuilder mutationBuilder;
        private final ConsistencyLevel cl;

        private CounterMutationBuilder(MutationBuilder mutationBuilder, ConsistencyLevel cl)
        {
            this.mutationBuilder = mutationBuilder;
            this.cl = cl;
        }

        public IMutationBuilder add(PartitionUpdate.Builder builder)
        {
            return mutationBuilder.add(builder);
        }

        public IMutation build()
        {
            return new CounterMutation(mutationBuilder.build(), cl);
        }

        public PartitionUpdate.Builder get(TableId id)
        {
            return mutationBuilder.get(id);
        }
    }

    private static class VirtualMutationBuilder implements IMutationBuilder
    {
        private final String keyspaceName;
        private final DecoratedKey partitionKey;

        private final HashMap<TableId, PartitionUpdate.Builder> modifications = new HashMap<>();

        private VirtualMutationBuilder(String keyspaceName, DecoratedKey partitionKey)
        {
            this.keyspaceName = keyspaceName;
            this.partitionKey = partitionKey;
        }

        @Override
        public VirtualMutationBuilder add(PartitionUpdate.Builder builder)
        {
            PartitionUpdate.Builder prev = modifications.put(builder.metadata().id, builder);
            if (null != prev)
                throw new IllegalStateException();
            return this;
        }

        @Override
        public VirtualMutation build()
        {
            ImmutableMap.Builder<TableId, PartitionUpdate> updates = new ImmutableMap.Builder<>();
            modifications.forEach((tableId, updateBuilder) -> updates.put(tableId, updateBuilder.build()));
            return new VirtualMutation(keyspaceName, partitionKey, updates.build());
        }

        @Override
        public PartitionUpdate.Builder get(TableId tableId)
        {
            return modifications.get(tableId);
        }
    }
}
