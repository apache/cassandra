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

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.TableId;

/**
 * A specialised IMutation implementation for virtual keyspaces.
 *
 * Mainly overrides {@link #apply()} to go straight to {@link VirtualTable#apply(PartitionUpdate)} for every table involved.
 */
public final class VirtualMutation implements IMutation
{
    private final String keyspaceName;
    private final DecoratedKey partitionKey;
    private final ImmutableMap<TableId, PartitionUpdate> modifications;

    public VirtualMutation(PartitionUpdate update)
    {
        this(update.metadata().keyspace, update.partitionKey(), ImmutableMap.of(update.metadata().id, update));
    }

    public VirtualMutation(String keyspaceName, DecoratedKey partitionKey, ImmutableMap<TableId, PartitionUpdate> modifications)
    {
        this.keyspaceName = keyspaceName;
        this.partitionKey = partitionKey;
        this.modifications = modifications;
    }

    @Override
    public void apply()
    {
        modifications.forEach((id, update) -> VirtualKeyspaceRegistry.instance.getTableNullable(id).apply(update));
    }

    @Override
    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    @Override
    public Collection<TableId> getTableIds()
    {
        return modifications.keySet();
    }

    @Override
    public DecoratedKey key()
    {
        return partitionKey;
    }

    @Override
    public long getTimeout(TimeUnit unit)
    {
        return DatabaseDescriptor.getWriteRpcTimeout(unit);
    }

    @Override
    public String toString(boolean shallow)
    {
        MoreObjects.ToStringHelper helper =
            MoreObjects.toStringHelper(this)
                       .add("keyspace", keyspaceName)
                       .add("partition key", partitionKey);

        if (shallow)
            helper.add("tables", getTableIds());
        else
            helper.add("modifications", getPartitionUpdates());

        return helper.toString();
    }

    @Override
    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return modifications.values();
    }

    @Override
    public Supplier<Mutation> hintOnFailure()
    {
        return null;
    }

    @Override
    public void validateIndexedColumns()
    {
        // no-op
    }

    public void validateSize(int version, int overhead)
    {
        // no-op
    }
}
