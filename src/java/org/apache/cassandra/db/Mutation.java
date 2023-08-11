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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TeeDataInputPlus;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.AbstractWriteResponseHandler;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Future;

import static org.apache.cassandra.net.MessagingService.VERSION_40;
import static org.apache.cassandra.net.MessagingService.VERSION_50;
import static org.apache.cassandra.utils.MonotonicClock.Global.approxTime;

public class Mutation implements IMutation, Supplier<Mutation>
{
    public static final MutationSerializer serializer = new MutationSerializer();

    // todo this is redundant
    // when we remove it, also restore SerializationsTest.testMutationRead to not regenerate new Mutations each test
    private final String keyspaceName;

    private final DecoratedKey key;
    // map of column family id to mutations for that column family.
    private final ImmutableMap<TableId, PartitionUpdate> modifications;

    // Time at which this mutation or the builder that built it was instantiated
    final long approxCreatedAtNanos;
    // keep track of when mutation has started waiting for a MV partition lock
    final AtomicLong viewLockAcquireStart = new AtomicLong(0);

    private final boolean cdcEnabled;

    private static final int SERIALIZATION_VERSION_COUNT = MessagingService.Version.values().length;
    // Contains serialized representations of this mutation.
    // Note: there is no functionality to clear/remove serialized instances, because a mutation must never
    // be modified (e.g. calling add(PartitionUpdate)) when it's being serialized.
    private final Serialization[] cachedSerializations = new Serialization[SERIALIZATION_VERSION_COUNT];

    /** @see CassandraRelevantProperties#CACHEABLE_MUTATION_SIZE_LIMIT */
    private static final long CACHEABLE_MUTATION_SIZE_LIMIT = CassandraRelevantProperties.CACHEABLE_MUTATION_SIZE_LIMIT.getLong();

    public Mutation(PartitionUpdate update)
    {
        this(update.metadata().keyspace, update.partitionKey(), ImmutableMap.of(update.metadata().id, update), approxTime.now(), update.metadata().params.cdc);
    }

    public Mutation(String keyspaceName, DecoratedKey key, ImmutableMap<TableId, PartitionUpdate> modifications, long approxCreatedAtNanos)
    {
        this(keyspaceName, key, modifications, approxCreatedAtNanos, cdcEnabled(modifications.values()));
    }

    public Mutation(String keyspaceName, DecoratedKey key, ImmutableMap<TableId, PartitionUpdate> modifications, long approxCreatedAtNanos, boolean cdcEnabled)
    {
        this.keyspaceName = keyspaceName;
        this.key = key;
        this.modifications = modifications;
        this.cdcEnabled = cdcEnabled;
        this.approxCreatedAtNanos = approxCreatedAtNanos;
    }

    private static boolean cdcEnabled(Iterable<PartitionUpdate> modifications)
    {
        boolean cdc = false;
        for (PartitionUpdate pu : modifications)
            cdc |= pu.metadata().params.cdc;
        return cdc;
    }

    public Mutation without(Set<TableId> tableIds)
    {
        if (tableIds.isEmpty())
            return this;

        ImmutableMap.Builder<TableId, PartitionUpdate> builder = new ImmutableMap.Builder<>();
        for (Map.Entry<TableId, PartitionUpdate> update : modifications.entrySet())
        {
            if (!tableIds.contains(update.getKey()))
            {
                builder.put(update);
            }
        }

        return new Mutation(keyspaceName, key, builder.build(), approxCreatedAtNanos);
    }

    public Mutation without(TableId tableId)
    {
        return without(Collections.singleton(tableId));
    }

    public String getKeyspaceName()
    {
        return keyspaceName;
    }

    public Collection<TableId> getTableIds()
    {
        return modifications.keySet();
    }

    public DecoratedKey key()
    {
        return key;
    }

    public ImmutableCollection<PartitionUpdate> getPartitionUpdates()
    {
        return modifications.values();
    }

    @Override
    public Supplier<Mutation> hintOnFailure()
    {
        return this;
    }

    @Override
    public Mutation get()
    {
        return this;
    }

    public void validateSize(int version, int overhead)
    {
        long totalSize = serializedSize(version) + overhead;
        if(totalSize > MAX_MUTATION_SIZE)
        {
            CommitLog.instance.metrics.oversizedMutations.mark();
            throw new MutationExceededMaxSizeException(this, version, totalSize);
        }
    }

    public PartitionUpdate getPartitionUpdate(TableMetadata table)
    {
        return table == null ? null : modifications.get(table.id);
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

        Set<TableId> updatedTables = new HashSet<>();
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
        ImmutableMap.Builder<TableId, PartitionUpdate> modifications = new ImmutableMap.Builder<>();
        for (TableId table : updatedTables)
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
        return new Mutation(ks, key, modifications.build(), approxTime.now());
    }

    public Future<?> applyFuture()
    {
        Keyspace ks = Keyspace.open(keyspaceName);
        return ks.applyFuture(this, Keyspace.open(keyspaceName).getMetadata().params.durableWrites, true);
    }

    private void apply(Keyspace keyspace, boolean durableWrites, boolean isDroppable)
    {
        keyspace.apply(this, durableWrites, true, isDroppable);
    }

    public void apply(boolean durableWrites, boolean isDroppable)
    {
        apply(Keyspace.open(keyspaceName), durableWrites, isDroppable);
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
        Keyspace keyspace = Keyspace.open(keyspaceName);
        apply(keyspace, keyspace.getMetadata().params.durableWrites, true);
    }

    public void applyUnsafe()
    {
        apply(false);
    }

    public long getTimeout(TimeUnit unit)
    {
        return DatabaseDescriptor.getWriteRpcTimeout(unit);
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
            for (TableId tableId : modifications.keySet())
            {
                TableMetadata cfm = Schema.instance.getTableMetadata(tableId);
                cfnames.add(cfm == null ? "-dropped-" : cfm.name);
            }
            buff.append(StringUtils.join(cfnames, ", "));
        }
        else
        {
            buff.append("\n  ").append(StringUtils.join(modifications.values(), "\n  ")).append('\n');
        }
        return buff.append("])").toString();
    }

    private int serializedSize40;
    private int serializedSize50;

    public int serializedSize(int version)
    {
        switch (version)
        {
            case VERSION_40:
                if (serializedSize40 == 0)
                    serializedSize40 = (int) serializer.serializedSize(this, VERSION_40);
                return serializedSize40;
            case VERSION_50:
                if (serializedSize50 == 0)
                    serializedSize50 = (int) serializer.serializedSize(this, VERSION_50);
                return serializedSize50;

            default:
                throw new IllegalStateException("Unknown serialization version: " + version);
        }
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
        public PartitionUpdate.SimpleBuilder update(TableMetadata metadata);

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
            serialization(mutation, version).serialize(PartitionUpdate.serializer, mutation, out, version);
        }

        /**
         * Called early during request processing to prevent that {@link #serialization(Mutation, int)} is
         * called concurrently.
         * See {@link org.apache.cassandra.service.StorageProxy#sendToHintedReplicas(Mutation, ReplicaPlan.ForWrite, AbstractWriteResponseHandler, String, Stage)}
         */
        @SuppressWarnings("JavadocReference")
        public void prepareSerializedBuffer(Mutation mutation, int version)
        {
            serialization(mutation, version);
        }

        /**
         * Retrieve the cached serialization of this mutation, or compute and cache said serialization if it doesn't
         * exist yet. Note that this method is _not_ synchronized even though it may (and will often) be called
         * concurrently. Concurrent calls are still safe however, the only risk is that the value is not cached yet,
         * multiple concurrent calls may compute it multiple times instead of just once. This is ok as in practice
         * as we make sure this doesn't happen in the hot path by forcing the initial caching in
         * {@link org.apache.cassandra.service.StorageProxy#sendToHintedReplicas(Mutation, ReplicaPlan.ForWrite, AbstractWriteResponseHandler, String, Stage)}
         * via {@link #prepareSerializedBuffer(Mutation)}, which is the only caller that passes
         * {@code isPrepare==true}.
         */
        @SuppressWarnings("JavadocReference")
        private Serialization serialization(Mutation mutation, int version)
        {
            int versionOrdinal = MessagingService.getVersionOrdinal(version);
            // Retrieves the cached version, or build+cache it if it's not cached already.
            Serialization serialization = mutation.cachedSerializations[versionOrdinal];
            if (serialization == null)
            {
                serialization = new SizeOnlyCacheableSerialization();
                long serializedSize = serialization.serializedSize(PartitionUpdate.serializer, mutation, version);

                // Excessively large mutation objects cause GC pressure and huge allocations when serialized.
                // so we only cache serialized mutations when they are below the defined limit.
                if (serializedSize < CACHEABLE_MUTATION_SIZE_LIMIT)
                {
                    try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
                    {
                        serializeInternal(PartitionUpdate.serializer, mutation, dob, version);
                        serialization = new CachedSerialization(dob.toByteArray());
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
                mutation.cachedSerializations[versionOrdinal] = serialization;
            }

            return serialization;
        }

        static void serializeInternal(PartitionUpdate.PartitionUpdateSerializer serializer,
                                         Mutation mutation,
                                         DataOutputPlus out,
                                         int version) throws IOException
        {
            Map<TableId, PartitionUpdate> modifications = mutation.modifications;

            /* serialize the modifications in the mutation */
            int size = modifications.size();
            out.writeUnsignedVInt32(size);

            assert size > 0;
            for (PartitionUpdate partitionUpdate : modifications.values())
            {
                serializer.serialize(partitionUpdate, out, version);
            }
        }

        public Mutation deserialize(DataInputPlus in, int version, DeserializationHelper.Flag flag) throws IOException
        {
            Mutation m;
            TeeDataInputPlus teeIn;
            try (DataOutputBuffer dob = DataOutputBuffer.scratchBuffer.get())
            {
                teeIn = new TeeDataInputPlus(in, dob, CACHEABLE_MUTATION_SIZE_LIMIT);

                int size = teeIn.readUnsignedVInt32();
                assert size > 0;

                PartitionUpdate update = PartitionUpdate.serializer.deserialize(teeIn, version, flag);
                if (size == 1)
                {
                    m = new Mutation(update);
                }
                else
                {
                    ImmutableMap.Builder<TableId, PartitionUpdate> modifications = new ImmutableMap.Builder<>();
                    DecoratedKey dk = update.partitionKey();

                    modifications.put(update.metadata().id, update);
                    for (int i = 1; i < size; ++i)
                    {
                        update = PartitionUpdate.serializer.deserialize(teeIn, version, flag);
                        modifications.put(update.metadata().id, update);
                    }
                    m = new Mutation(update.metadata().keyspace, dk, modifications.build(), approxTime.now());
                }

                //Only cache serializations that don't hit the limit
                if (!teeIn.isLimitReached())
                    m.cachedSerializations[MessagingService.getVersionOrdinal(version)] = new CachedSerialization(dob.toByteArray());

                return m;
            }
        }

        public Mutation deserialize(DataInputPlus in, int version) throws IOException
        {
            return deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE);
        }

        public long serializedSize(Mutation mutation, int version)
        {
            return serialization(mutation, version).serializedSize(PartitionUpdate.serializer, mutation, version);
        }
    }

    /**
     * There are two implementations of this class. One that keeps the serialized representation on-heap for later
     * reuse and one that doesn't. Keeping all sized mutations around may lead to "bad" GC pressure (G1 GC) due to humongous objects.
     * By default serialized mutations up to 2MB are kept on-heap - see {@link org.apache.cassandra.config.CassandraRelevantProperties#CACHEABLE_MUTATION_SIZE_LIMIT}.
     */
    private static abstract class Serialization
    {
        abstract void serialize(PartitionUpdate.PartitionUpdateSerializer serializer, Mutation mutation, DataOutputPlus out, int version) throws IOException;

        abstract long serializedSize(PartitionUpdate.PartitionUpdateSerializer serializer, Mutation mutation, int version);
    }

    /**
     * Represents the cached serialization of a {@link Mutation} as a {@code byte[]}.
     */
    private static final class CachedSerialization extends Serialization
    {
        private final byte[] serialized;

        CachedSerialization(byte[] serialized)
        {
            this.serialized = Preconditions.checkNotNull(serialized);
        }

        @Override
        void serialize(PartitionUpdate.PartitionUpdateSerializer serializer, Mutation mutation, DataOutputPlus out, int version) throws IOException
        {
            out.write(serialized);
        }

        @Override
        long serializedSize(PartitionUpdate.PartitionUpdateSerializer serializer, Mutation mutation, int version)
        {
            return serialized.length;
        }
    }

    /**
     * Represents a non-cacheable serialization of a {@link Mutation}, only the size of the mutation is lazily cached.
     */
    private static final class SizeOnlyCacheableSerialization extends Serialization
    {
        private volatile long size;

        @Override
        void serialize(PartitionUpdate.PartitionUpdateSerializer serializer, Mutation mutation, DataOutputPlus out, int version) throws IOException
        {
            MutationSerializer.serializeInternal(serializer, mutation, out, version);
        }

        @Override
        long serializedSize(PartitionUpdate.PartitionUpdateSerializer serializer, Mutation mutation, int version)
        {
            long size = this.size;
            if (size == 0L)
            {
                size = TypeSizes.sizeofUnsignedVInt(mutation.modifications.size());
                for (PartitionUpdate partitionUpdate : mutation.modifications.values())
                    size += serializer.serializedSize(partitionUpdate, version);
                this.size = size;
            }
            return size;
        }
    }

    /**
     * Collects finalized partition updates
     */
    public static class PartitionUpdateCollector
    {
        private final ImmutableMap.Builder<TableId, PartitionUpdate> modifications = new ImmutableMap.Builder<>();
        private final String keyspaceName;
        private final DecoratedKey key;
        private final long approxCreatedAtNanos = approxTime.now();
        private boolean empty = true;

        public PartitionUpdateCollector(String keyspaceName, DecoratedKey key)
        {
            this.keyspaceName = keyspaceName;
            this.key = key;
        }

        public PartitionUpdateCollector add(PartitionUpdate partitionUpdate)
        {
            assert partitionUpdate != null;
            assert partitionUpdate.partitionKey().getPartitioner() == key.getPartitioner();
            // note that ImmutableMap.Builder only allows put:ing the same key once, it will fail during build() below otherwise
            modifications.put(partitionUpdate.metadata().id, partitionUpdate);
            empty = false;
            return this;
        }

        public DecoratedKey key()
        {
            return key;
        }

        public String getKeyspaceName()
        {
            return keyspaceName;
        }

        public boolean isEmpty()
        {
            return empty;
        }

        public Mutation build()
        {
            return new Mutation(keyspaceName, key, modifications.build(), approxCreatedAtNanos);
        }
    }
}
