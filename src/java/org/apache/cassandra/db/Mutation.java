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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.StringUtils;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.ByteBufferUtil;

// TODO convert this to a Builder pattern instead of encouraging M.add directly,
// which is less-efficient since we have to keep a mutable HashMap around
public class Mutation implements IMutation
{
    public static final MutationSerializer serializer = new MutationSerializer();

    public static final String FORWARD_TO = "FWD_TO";
    public static final String FORWARD_FROM = "FWD_FRM";

    // todo this is redundant
    // when we remove it, also restore SerializationsTest.testMutationRead to not regenerate new Mutations each test
    private final String keyspaceName;

    private final DecoratedKey key;
    // map of column family id to mutations for that column family.
    private final Map<UUID, PartitionUpdate> modifications;

    // Time at which this mutation was instantiated
    public final long createdAt = System.currentTimeMillis();
    // keep track of when mutation has started waiting for a MV partition lock
    public final AtomicLong viewLockAcquireStart = new AtomicLong(0);

    private boolean cdcEnabled = false;

    public Mutation(String keyspaceName, DecoratedKey key)
    {
        this(keyspaceName, key, new HashMap<>());
    }

    public Mutation(PartitionUpdate update)
    {
        this(update.metadata().ksName, update.partitionKey(), Collections.singletonMap(update.metadata().cfId, update));
    }

    protected Mutation(String keyspaceName, DecoratedKey key, Map<UUID, PartitionUpdate> modifications)
    {
        this.keyspaceName = keyspaceName;
        this.key = key;
        this.modifications = modifications;
        for (PartitionUpdate pu : modifications.values())
            cdcEnabled |= pu.metadata().params.cdc;
    }

    public Mutation copy()
    {
        return new Mutation(keyspaceName, key, new HashMap<>(modifications));
    }

    public Mutation without(Set<UUID> cfIds)
    {
        if (cfIds.isEmpty())
            return this;

        Mutation copy = copy();
        copy.modifications.keySet().removeAll(cfIds);

        copy.cdcEnabled = false;
        for (PartitionUpdate pu : modifications.values())
            copy.cdcEnabled |= pu.metadata().params.cdc;

        return copy;
    }

    public Mutation without(UUID cfId)
    {
        return without(Collections.singleton(cfId));
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public Collection<UUID> getColumnFamilyIds()
    {
        return modifications.keySet();
    }

    public DecoratedKey key()
    {
        return key;
    }

    public Collection<PartitionUpdate> getPartitionUpdates()
    {
        return modifications.values();
    }

    public PartitionUpdate getPartitionUpdate(UUID cfId)
    {
        return modifications.get(cfId);
    }

    /**
     * Adds PartitionUpdate to the local set of modifications.
     * Assumes no updates for the Table this PartitionUpdate impacts.
     *
     * @param update PartitionUpdate to append to Modifications list
     * @return Mutation this mutation
     * @throws IllegalArgumentException If PartitionUpdate for duplicate table is passed as argument
     */
    public Mutation add(PartitionUpdate update)
    {
        assert update != null;
        assert update.partitionKey().getPartitioner() == key.getPartitioner();

        cdcEnabled |= update.metadata().params.cdc;

        PartitionUpdate prev = modifications.put(update.metadata().cfId, update);
        if (prev != null)
            // developer error
            throw new IllegalArgumentException("Table " + update.metadata().cfName + " already has modifications in this mutation: " + prev);
        return this;
    }

    public PartitionUpdate get(CFMetaData cfm)
    {
        return modifications.get(cfm.cfId);
    }

    public boolean isEmpty()
    {
        return modifications.isEmpty();
    }

    /**
     * Creates a new mutation that merges all the provided mutations.
     *
     * @param mutations the mutations to merge together. All mutation must be
     * on the same keyspace and partition key. There should also be at least one
     * mutation.
     * @return a mutation that contains all the modifications contained in {@code mutations}.
     *
     * @throws IllegalArgumentException if not all the mutations are on the same
     * keyspace and key.
     */
    public static Mutation merge(List<Mutation> mutations)
    {
        assert !mutations.isEmpty();

        if (mutations.size() == 1)
            return mutations.get(0);

        Set<UUID> updatedTables = new HashSet<>();
        String ks = null;
        DecoratedKey key = null;
        for (Mutation mutation : mutations)
        {
            updatedTables.addAll(mutation.modifications.keySet());
            if (ks != null && !ks.equals(mutation.keyspaceName))
                throw new IllegalArgumentException();
            if (key != null && !key.equals(mutation.key))
                throw new IllegalArgumentException();
            ks = mutation.keyspaceName;
            key = mutation.key;
        }

        List<PartitionUpdate> updates = new ArrayList<>(mutations.size());
        Map<UUID, PartitionUpdate> modifications = new HashMap<>(updatedTables.size());
        for (UUID table : updatedTables)
        {
            for (Mutation mutation : mutations)
            {
                PartitionUpdate upd = mutation.modifications.get(table);
                if (upd != null)
                    updates.add(upd);
            }

            if (updates.isEmpty())
                continue;

            modifications.put(table, updates.size() == 1 ? updates.get(0) : PartitionUpdate.merge(updates));
            updates.clear();
        }
        return new Mutation(ks, key, modifications);
    }

    public CompletableFuture<?> applyFuture()
    {
        Keyspace ks = Keyspace.open(keyspaceName);
        return ks.applyFuture(this, Keyspace.open(keyspaceName).getMetadata().params.durableWrites, true);
    }

    public void apply(boolean durableWrites, boolean isDroppable)
    {
        Keyspace.open(keyspaceName).apply(this, durableWrites, true, isDroppable);
    }

    public void apply(boolean durableWrites)
    {
        apply(durableWrites, true);
    }

    /*
     * This is equivalent to calling commit. Applies the changes to
     * to the keyspace that is obtained by calling Keyspace.open().
     */
    public void apply()
    {
        apply(Keyspace.open(keyspaceName).getMetadata().params.durableWrites);
    }

    public void applyUnsafe()
    {
        apply(false);
    }

    public MessageOut<Mutation> createMessage()
    {
        return createMessage(MessagingService.Verb.MUTATION);
    }

    public MessageOut<Mutation> createMessage(MessagingService.Verb verb)
    {
        return new MessageOut<>(verb, this, serializer);
    }

    public long getTimeout()
    {
        return DatabaseDescriptor.getWriteRpcTimeout();
    }

    public int smallestGCGS()
    {
        int gcgs = Integer.MAX_VALUE;
        for (PartitionUpdate update : getPartitionUpdates())
            gcgs = Math.min(gcgs, update.metadata().params.gcGraceSeconds);
        return gcgs;
    }

    public boolean trackedByCDC()
    {
        return cdcEnabled;
    }

    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean shallow)
    {
        StringBuilder buff = new StringBuilder("Mutation(");
        buff.append("keyspace='").append(keyspaceName).append('\'');
        buff.append(", key='").append(ByteBufferUtil.bytesToHex(key.getKey())).append('\'');
        buff.append(", modifications=[");
        if (shallow)
        {
            List<String> cfnames = new ArrayList<>(modifications.size());
            for (UUID cfid : modifications.keySet())
            {
                CFMetaData cfm = Schema.instance.getCFMetaData(cfid);
                cfnames.add(cfm == null ? "-dropped-" : cfm.cfName);
            }
            buff.append(StringUtils.join(cfnames, ", "));
        }
        else
        {
            buff.append("\n  ").append(StringUtils.join(modifications.values(), "\n  ")).append('\n');
        }
        return buff.append("])").toString();
    }

    /**
     * Creates a new simple mutuation builder.
     *
     * @param keyspaceName the name of the keyspace this is a mutation for.
     * @param partitionKey the key of partition this if a mutation for.
     * @return a newly created builder.
     */
    public static SimpleBuilder simpleBuilder(String keyspaceName, DecoratedKey partitionKey)
    {
        return new SimpleBuilders.MutationBuilder(keyspaceName, partitionKey);
    }

    /**
     * Interface for building mutations geared towards human.
     * <p>
     * This should generally not be used when performance matters too much, but provides a more convenient interface to
     * build a mutation than using the class constructor when performance is not of the utmost importance.
     */
    public interface SimpleBuilder
    {
        /**
         * Sets the timestamp to use for the following additions to this builder or any derived (update or row) builder.
         *
         * @param timestamp the timestamp to use for following additions. If that timestamp hasn't been set, the current
         * time in microseconds will be used.
         * @return this builder.
         */
        public SimpleBuilder timestamp(long timestamp);

        /**
         * Sets the ttl to use for the following additions to this builder or any derived (update or row) builder.
         * <p>
         * Note that the for non-compact tables, this method must be called before any column addition for this
         * ttl to be used for the row {@code LivenessInfo}.
         *
         * @param ttl the ttl to use for following additions. If that ttl hasn't been set, no ttl will be used.
         * @return this builder.
         */
        public SimpleBuilder ttl(int ttl);

        /**
         * Adds an update for table identified by the provided metadata and return a builder for that partition.
         *
         * @param metadata the metadata of the table for which to add an update.
         * @return a builder for the partition identified by {@code metadata} (and the partition key for which this is a
         * mutation of).
         */
        public PartitionUpdate.SimpleBuilder update(CFMetaData metadata);

        /**
         * Adds an update for table identified by the provided name and return a builder for that partition.
         *
         * @param tableName the name of the table for which to add an update.
         * @return a builder for the partition identified by {@code metadata} (and the partition key for which this is a
         * mutation of).
         */
        public PartitionUpdate.SimpleBuilder update(String tableName);

        /**
         * Build the mutation represented by this builder.
         *
         * @return the built mutation.
         */
        public Mutation build();
    }

    public static class MutationSerializer implements IVersionedSerializer<Mutation>
    {
        public void serialize(Mutation mutation, DataOutputPlus out, int version) throws IOException
        {
            if (version < MessagingService.VERSION_20)
                out.writeUTF(mutation.getKeyspaceName());

            /* serialize the modifications in the mutation */
            int size = mutation.modifications.size();

            if (version < MessagingService.VERSION_30)
            {
                ByteBufferUtil.writeWithShortLength(mutation.key().getKey(), out);
                out.writeInt(size);
            }
            else
            {
                out.writeUnsignedVInt(size);
            }

            assert size > 0;
            for (Map.Entry<UUID, PartitionUpdate> entry : mutation.modifications.entrySet())
                PartitionUpdate.serializer.serialize(entry.getValue(), out, version);
        }

        public Mutation deserialize(DataInputPlus in, int version, SerializationHelper.Flag flag) throws IOException
        {
            if (version < MessagingService.VERSION_20)
                in.readUTF(); // read pre-2.0 keyspace name

            ByteBuffer key = null;
            int size;
            if (version < MessagingService.VERSION_30)
            {
                key = ByteBufferUtil.readWithShortLength(in);
                size = in.readInt();
            }
            else
            {
                size = (int)in.readUnsignedVInt();
            }

            assert size > 0;

            PartitionUpdate update = PartitionUpdate.serializer.deserialize(in, version, flag, key);
            if (size == 1)
                return new Mutation(update);

            Map<UUID, PartitionUpdate> modifications = new HashMap<>(size);
            DecoratedKey dk = update.partitionKey();

            modifications.put(update.metadata().cfId, update);
            for (int i = 1; i < size; ++i)
            {
                update = PartitionUpdate.serializer.deserialize(in, version, flag, dk);
                modifications.put(update.metadata().cfId, update);
            }

            return new Mutation(update.metadata().ksName, dk, modifications);
        }

        public Mutation deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in, version, SerializationHelper.Flag.FROM_REMOTE);
        }

        public long serializedSize(Mutation mutation, int version)
        {
            int size = 0;

            if (version < MessagingService.VERSION_20)
                size += TypeSizes.sizeof(mutation.getKeyspaceName());

            if (version < MessagingService.VERSION_30)
            {
                int keySize = mutation.key().getKey().remaining();
                size += TypeSizes.sizeof((short) keySize) + keySize;
                size += TypeSizes.sizeof(mutation.modifications.size());
            }
            else
            {
                size += TypeSizes.sizeofUnsignedVInt(mutation.modifications.size());
            }

            for (Map.Entry<UUID, PartitionUpdate> entry : mutation.modifications.entrySet())
                size += PartitionUpdate.serializer.serializedSize(entry.getValue(), version);

            return size;
        }
    }
}
