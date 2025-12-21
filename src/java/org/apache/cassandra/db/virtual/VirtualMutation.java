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

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import accord.utils.Invariants;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientState;

/**
 * A specialised IMutation implementation for virtual keyspaces.
 *
 * Mainly overrides {@link #apply()} to go straight to {@link VirtualTable#apply(PartitionUpdate)} for every table involved.
 */
public final class VirtualMutation implements IMutation
{
    public static final IVersionedSerializer<VirtualMutation> serializer = new IVersionedSerializer<VirtualMutation>()
    {
        @Override
        public void serialize(VirtualMutation t, DataOutputPlus out, int version) throws IOException
        {
            Invariants.require(t.modifications.size() == 1);
            PartitionUpdate.serializer.serialize(t.modifications.values().iterator().next(), out, version);
        }

        @Override
        public VirtualMutation deserialize(DataInputPlus in, int version) throws IOException
        {
            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE);
            return new VirtualMutation(update);
        }

        @Override
        public long serializedSize(VirtualMutation t, int version)
        {
            Invariants.require(t.modifications.size() == 1);
            return PartitionUpdate.serializer.serializedSize(t.modifications.values().iterator().next(), version);
        }
    };

    public static final IVerbHandler<VirtualMutation> handler = message -> {
        message.payload.apply();
        MessagingService.instance().respond(NoPayload.noPayload, message);
    };

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
    public boolean hasUpdateForTable(TableId tableId)
    {
        return modifications.containsKey(tableId);
    }

    @Override
    public Supplier<Mutation> hintOnFailure()
    {
        return null;
    }

    @Override
    public void validateIndexedColumns(ClientState state)
    {
        // no-op
    }

    public void validateSize(int version, int overhead)
    {
        // no-op
    }

    @Override
    public @Nullable VirtualMutation filter(Predicate<TableId> test)
    {
        throw new UnsupportedOperationException();
    }

    /*
     * Accord doesn't support reading/writing virtual tables yet so updating them non-transactionally is always safe
     */
    @Override
    public PotentialTxnConflicts potentialTxnConflicts()
    {
        return PotentialTxnConflicts.ALLOW;
    }
}
