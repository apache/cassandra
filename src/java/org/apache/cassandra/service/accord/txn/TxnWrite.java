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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Write;
import accord.local.CommandStore;
import accord.local.SafeCommandStore;
import accord.primitives.PartialTxn;
import accord.primitives.Routable.Domain;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.SimpleBitSet;
import accord.utils.SimpleBitSets;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;

import org.apache.cassandra.cql3.Attributes;
import org.apache.cassandra.cql3.RowUpdateBuilder;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.execution.AccordExecutor;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.SimpleBitSetSerializers;

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.apache.cassandra.db.SerializationHeader.StableHeaderSerializer.STABLE;
import static org.apache.cassandra.db.rows.DeserializationHelper.Flag.FROM_REMOTE;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.ArraySerializers.skipArray;

public class TxnWrite extends AbstractKeySorted<TxnWrite.LateBindWrite> implements Write
{
    public static final long NO_TIMESTAMP = 0;
    public static final int USE_DEFAULT_TTL = -1;

    /** The timestamp live data in a normalised base update is written at; see {@link Fragment#normaliseBaseUpdateTimes} */
    public static final long ENCODED_TIMESTAMP = 1;
    /** The nowInSec local deletion/expiration times in a normalised base update are relative to; see {@link Fragment#normaliseBaseUpdateTimes} */
    public static final long ENCODED_NOW_IN_SEC = 0;

    /**
     * Deserializes a normalised update (see {@link Fragment#normaliseBaseUpdateTimes}) bound to the provided execution time,
     * by offsetting the minimum timestamp and local deletion time of the header that all other times are encoded
     * relative to. TTLs are durations, so are unaffected.
     */
    public static class TimeOffsetHeaderDeserializer extends SerializationHeader.AbstractStableHeaderSerializer
    {
        final long timestampOffset;
        final long nowInSec;

        public TimeOffsetHeaderDeserializer(long timestamp, long nowInSec)
        {
            this.timestampOffset = timestamp - ENCODED_TIMESTAMP;
            this.nowInSec = nowInSec - ENCODED_NOW_IN_SEC;
        }

        @Override
        protected SerializationHeader construct(TableMetadata metadata, RegularAndStaticColumns columns, EncodingStats stats)
        {
            Invariants.require(stats.minTimestamp <= 1); // shouldn't be possible to write no timestamp, but could write only a deletion timestamp
            long minTimestamp = stats.minTimestamp + timestampOffset;
            long minLocalDeletionTime =  stats.minLocalDeletionTime + nowInSec; // unconditionally add nowInSec, to avoid latent bug where there's real data with TTL == DELETION_TIME_EPOCH
            return super.construct(metadata, columns, new EncodingStats(minTimestamp, minLocalDeletionTime, stats.minTTL));
        }
    }

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnWrite.class);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnWrite(TableMetadatas.none(), Collections.emptyList(), SimpleBitSets.allUnset(1)));

    public static class LateBindWrite extends AbstractParameterisedVersionedSerialized<PartitionUpdate, TableMetadatas>
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new LateBindWrite(null, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        public final PartitionKey key;
        public final int index;

        public LateBindWrite(PartitionKey key, int index, PartitionUpdate update, TableMetadatas tables)
        {
            this(key, index, serializeInternal(update, tables, Version.LATEST));
        }

        private LateBindWrite(PartitionKey key, int index, ByteBuffer latestVersionBytes)
        {
            super(latestVersionBytes);
            this.key = key;
            this.index = index;
        }

        @Override
        public long estimatedSizeOnHeap()
        {
            // we don't measure the key, as this is shared
            if (latestVersionBytes == null)
                return EMPTY_SIZE;
            return EMPTY_SIZE + ByteBufferUtil.estimatedSizeOnHeap(latestVersionBytes);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            LateBindWrite write = (LateBindWrite) o;
            return index == write.index && key.equals(write.key);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(super.hashCode(), key, index);
        }

        @Override
        public String toString()
        {
            return "Complete{" +
                   "key=" + key +
                   ", index=" + index +
                   '}';
        }

        @Override
        protected ByteBuffer serialize(PartitionUpdate value, TableMetadatas tables, Version version)
        {
            return serializeInternal(value, tables, version);
        }

        @Override
        protected ByteBuffer reserialize(ByteBuffer bytes, TableMetadatas param, Version srcVersion, Version trgVersion)
        {
            return bytes;
        }

        @Override
        protected PartitionUpdate deserialize(TableMetadatas tables, ByteBuffer bytes, Version version)
        {
            return deserialize(key, tables, bytes, version);
        }

        private static ByteBuffer serializeInternal(PartitionUpdate value, TableMetadatas tables, Version version)
        {
            try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
            {
                PartitionUpdate.serializer.serializeWithoutKey(value, tables, out, version.messageVersion());
                return out.asNewBuffer();
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        private static PartitionUpdate deserialize(PartitionKey key, TableMetadatas tables, ByteBuffer bytes, Version version)
        {
            try (DataInputBuffer in = new DataInputBuffer(bytes, true))
            {
                return PartitionUpdate.serializer.deserialize(key, tables, in, version.messageVersion(), FROM_REMOTE);
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }

        public static final ParameterisedVersionedSerializer<LateBindWrite, TableMetadatasAndKeys, Version> serializer = new ParameterisedVersionedSerializer<LateBindWrite, TableMetadatasAndKeys, Version>()
        {
            @Override
            public void serialize(LateBindWrite write, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
            {
                tablesAndKeys.serializeKey(write.key, out);
                out.writeInt(write.index);
                ByteBufferUtil.writeWithVIntLength(write.bytes(tablesAndKeys.tables, version), out);
            }

            ByteBuffer reserialize(ByteBuffer buffer, TableMetadatasAndKeys tablesAndKeys, Version srcVersion, Version trgVersion)
            {
                return buffer;
            }

            @Override
            public LateBindWrite deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
            {
                PartitionKey key = tablesAndKeys.deserializeKey(in);
                int index = in.readInt();
                ByteBuffer bytes = ByteBufferUtil.readWithVIntLength(in);
                if (version != Version.LATEST)
                    bytes = reserialize(bytes, tablesAndKeys, version, Version.LATEST);
                return new LateBindWrite(key, index, bytes);
            }

            @Override
            public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
            {
                tablesAndKeys.deserializeKey(in);
                in.readInt();
                ByteBufferUtil.skipWithVIntLength(in);
            }

            @Override
            public long serializedSize(LateBindWrite write, TableMetadatasAndKeys tablesAndKeys, Version version)
            {
                long size = 0;
                size += tablesAndKeys.serializedKeySize(write.key);
                size += TypeSizes.INT_SIZE;
                size += ByteBufferUtil.serializedSizeWithVIntLength(write.bytes(tablesAndKeys.tables, version));
                return size;
            }
        };
    }

    /**
     * Partition update that can later be supplemented with data from the read phase
     */
    public static class Fragment
    {
        public final PartitionKey key;
        public final int index;
        public final PartitionUpdate baseUpdate;
        public final TxnReferenceOperations referenceOps;

        /** If positive, a timestamp; if negative, a TTL; if zero, neither. */
        private final long timestampOrTtl;

        public Fragment(PartitionKey key, int index, PartitionUpdate baseUpdate, TxnReferenceOperations referenceOps, long timestamp)
        {
            this(encode(timestamp, USE_DEFAULT_TTL), key, index, baseUpdate, referenceOps);
        }

        public Fragment(PartitionKey key, int index, PartitionUpdate baseUpdate, TxnReferenceOperations referenceOps, long timestamp, int ttl)
        {
            this(encode(timestamp, ttl), key, index, baseUpdate, referenceOps);
        }

        private Fragment(long timestampOrTtl, PartitionKey key, int index, PartitionUpdate baseUpdate, TxnReferenceOperations referenceOps)
        {
            this.key = key;
            this.index = index;
            this.baseUpdate = baseUpdate;
            this.referenceOps = referenceOps;
            this.timestampOrTtl = timestampOrTtl;
        }

        private static final long MIN_ENCODED_TTL = -1L - Attributes.MAX_TTL;

        /**
         * Set timestamp=1, nowInSec=0, so we may deserialize directly to executeAt by offset
         */
        public static PartitionUpdate normaliseBaseUpdateTimes(PartitionUpdate update)
        {
            return new PartitionUpdate.Builder(update, 0).updateTimesForAccord(ENCODED_TIMESTAMP, ENCODED_NOW_IN_SEC).build();
        }

        private static long encode(long timestamp, int ttl)
        {
            if (ttl == USE_DEFAULT_TTL)
            {
                Invariants.requireArgument(timestamp >= 0, "Negative timestamp %d cannot be encoded", timestamp);
                return timestamp;
            }

            Invariants.requireArgument(timestamp == NO_TIMESTAMP, "Cannot encode both a custom timestamp (%d) and TTL (%d)", timestamp, ttl);
            Invariants.requireArgument(ttl >= 0 && ttl <= Attributes.MAX_TTL, "Invalid TTL %d", ttl);
            return -1L - ttl;
        }

        public long timestampOrTtl()
        {
            return timestampOrTtl;
        }

        public long timestamp()
        {
            return Math.max(NO_TIMESTAMP, timestampOrTtl);
        }

        public int ttl(TableMetadata metadata)
        {
            return timestampOrTtl >= 0 ? metadata.params.defaultTimeToLive : (int) (-1L - timestampOrTtl);
        }

        public int ttlOrUseDefault()
        {
            return timestampOrTtl >= 0 ? USE_DEFAULT_TTL : (int) (-1L - timestampOrTtl);
        }

        public static int compareKeys(Fragment left, Fragment right)
        {
            return left.key.compareTo(right.key);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Fragment fragment = (Fragment) o;
            return index == fragment.index && timestampOrTtl == fragment.timestampOrTtl && key.equals(fragment.key) && baseUpdate.equals(fragment.baseUpdate) && referenceOps.equals(fragment.referenceOps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, index, baseUpdate, referenceOps, timestampOrTtl);
        }

        @Override
        public String toString()
        {
            return "Fragment{key=" + key + ", index=" + index + ", baseUpdate=" + baseUpdate + ", referenceOps=" + referenceOps +
                   (timestampOrTtl > 0 ? ", timestamp=" + timestampOrTtl : timestampOrTtl < 0 ? ", ttl=" + timestampOrTtl : "") + '}';
        }

        public boolean isComplete()
        {
            return referenceOps.isEmpty();
        }

        public PartitionUpdate complete(AccordUpdateParameters parameters)
        {
            if (isComplete())
                return baseUpdate;

            DecoratedKey key = baseUpdate.partitionKey();
            TableMetadata metadata = baseUpdate.metadata();
            PartitionUpdate.Builder updateBuilder = new PartitionUpdate.Builder(metadata,
                                                                                key,
                                                                                columns(baseUpdate, referenceOps),
                                                                                baseUpdate.rowCount(),
                                                                                baseUpdate.canHaveShadowedData());

            RowUpdateBuilder up = parameters.updateBuilder(metadata, key, index, timestamp(), ttl(metadata));
            TxnData data = parameters.getData();
            Row staticRow = applyUpdates(baseUpdate.staticRow(), referenceOps.statics, key, Clustering.STATIC_CLUSTERING, up, data);

            if (!staticRow.isEmpty())
                updateBuilder.add(staticRow);

            for (Clustering<?> clustering : referenceOps.clusterings)
            {
                Row existing = baseUpdate.hasRows() ? baseUpdate.getRow(clustering) : null;
                Row row = applyUpdates(existing, referenceOps.regulars, key, clustering, up, data);
                if (row != null)
                    updateBuilder.add(row);
            }

            return updateBuilder.build();
        }

        public LateBindWrite completeSerialized(AccordUpdateParameters parameters, TableMetadatas tables)
        {
            return new LateBindWrite(key, index, complete(parameters), tables);
        }

        private static Columns columns(Columns current, List<TxnReferenceOperation> referenceOps)
        {
            if (referenceOps.isEmpty())
                return current;

            Set<ColumnMetadata> missing = null;
            for (int i = 0, mi = referenceOps.size() ; i < mi ; ++i)
            {
                ColumnMetadata cm = referenceOps.get(i).receiver();
                if (!current.contains(cm))
                {
                    if (missing == null)
                        missing = new HashSet<>();
                    missing.add(cm);
                }
            }
            if (missing == null)
                return current;
            return current.mergeTo(Columns.from(missing));
        }

        private static RegularAndStaticColumns columns(PartitionUpdate update, TxnReferenceOperations referenceOps)
        {
            checkState(!referenceOps.isEmpty());
            RegularAndStaticColumns current = update.columns();
            return new RegularAndStaticColumns(columns(current.statics, referenceOps.statics),
                                               columns(current.regulars, referenceOps.regulars));
        }

        private static Row applyUpdates(Row existing, List<TxnReferenceOperation> operations, DecoratedKey key, Clustering<?> clustering, RowUpdateBuilder up, TxnData data)
        {
            if (operations.isEmpty())
                return existing;

            if (existing != null && !existing.isEmpty())
            {
                checkState(existing.clustering().equals(clustering));
                up.addRow(existing);
            }
            else
                up.newRow(clustering);

            operations.forEach(op -> op.apply(data, key, up));
            return up.buildRow();
        }

        static final FragmentSerializer serializer = new FragmentSerializer();
        static class FragmentSerializer
        {
            public void serialize(Fragment fragment, TableMetadatas tables, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUnsignedVInt32(fragment.index);
                PartitionUpdate.serializer.serializeWithoutKey(fragment.baseUpdate, tables, out, version.messageVersion());
                TxnReferenceOperations.serializer.serialize(fragment.referenceOps, tables, out, version);
                out.writeUnsignedVInt(fragment.timestampOrTtl);
            }

            public Fragment deserialize(PartitionKey key, TableMetadatas tables, DataInputPlus in, Version version) throws IOException
            {
                return deserialize(key, tables, in, version, STABLE);
            }

            /**
             * @param headerSerializer {@link SerializationHeader.StableHeaderSerializer#STABLE}, or a {@link TimeOffsetHeaderDeserializer}
             *                         to bind a normalised base update to its execution time
             */
            public Fragment deserialize(PartitionKey key, TableMetadatas tables, DataInputPlus in, Version version, SerializationHeader.AbstractStableHeaderSerializer headerSerializer) throws IOException
            {
                int idx = in.readUnsignedVInt32();
                // TODO (required): why FROM_REMOTE?
                PartitionUpdate baseUpdate = PartitionUpdate.serializer.deserialize(key, tables, in, version.messageVersion(), FROM_REMOTE, headerSerializer, null);
                TxnReferenceOperations referenceOps = TxnReferenceOperations.serializer.deserialize(tables, in, version);
                long timestampOrTtl = in.readUnsignedVInt();
                if (timestampOrTtl < MIN_ENCODED_TTL)
                    throw new IOException("Invalid encoded timestamp or TTL " + timestampOrTtl);
                return new Fragment(timestampOrTtl, key, idx, baseUpdate, referenceOps);
            }

            public long serializedSize(Fragment fragment, TableMetadatas tables, Version version)
            {
                long size = 0;
                size += TypeSizes.sizeofUnsignedVInt(fragment.index);
                size += PartitionUpdate.serializer.serializedSizeWithoutKey(fragment.baseUpdate, tables, version.messageVersion());
                size += TxnReferenceOperations.serializer.serializedSize(fragment.referenceOps, tables, version);
                size += TypeSizes.sizeofUnsignedVInt(fragment.timestampOrTtl);
                return size;
            }

            public static ByteBuffer serialize(Fragment fragment, TableMetadatas tables, Version version)
            {
                long size = TypeSizes.sizeofUnsignedVInt(version.version);
                size += Fragment.serializer.serializedSize(fragment, tables, version);

                try (DataOutputBuffer out = new DataOutputBuffer((int) size))
                {
                    out.writeUnsignedVInt32(version.version);
                    Fragment.serializer.serialize(fragment, tables, out, version);
                    return out.buffer(false);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }

            private static Fragment deserialize(PartitionKey key, TableMetadatas tables, ByteBuffer bytes)
            {
                try (DataInputBuffer in = new DataInputBuffer(bytes, true))
                {
                    Version version = Version.fromVersion(in.readUnsignedVInt32());
                    return Fragment.serializer.deserialize(key, tables, in, version);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public final TableMetadatas tables;
    private final SimpleBitSet conditionalBlockBitSet;

    private TxnWrite(TableMetadatas tables, LateBindWrite[] items, SimpleBitSet conditionalBlockBitSet)
    {
        super(items, Domain.Key);
        this.tables = tables;
        this.conditionalBlockBitSet = conditionalBlockBitSet;
    }

    public TxnWrite(TableMetadatas tables, List<LateBindWrite> items, SimpleBitSet conditionalBlockBitSet)
    {
        super(items, Domain.Key);
        this.tables = tables;
        this.conditionalBlockBitSet = conditionalBlockBitSet;
    }

    @Override
    int compareNonKeyFields(LateBindWrite left, LateBindWrite right)
    {
        return Integer.compare(left.index, right.index);
    }

    @Override
    Seekable getKey(LateBindWrite item)
    {
        return item.key;
    }

    @Override
    LateBindWrite[] newArray(int size)
    {
        return new LateBindWrite[size];
    }

    public void unmemoize()
    {
        for (int i = 0 ; i < size() ; ++i)
            items[i].unmemoize();
    }

    @Override
    public AsyncChain<Void> apply(SafeCommandStore safeStore, Seekable key, TxnId txnId, Timestamp executeAt, PartialTxn txn)
    {
        return applyDirect(safeStore.commandStore(), key, txnId, executeAt, txn);
    }

    @Override
    public AsyncChain<Void> applyDirect(CommandStore commandStore, Seekable key, TxnId txnId, Timestamp executeAt, PartialTxn txn)
    {
        // UnrecoverableRepairUpdate will deserialize as null at other nodes
        // Accord should skip the Update for a read transaction, but handle it here anyways
        TxnUpdate txnUpdate = ((TxnUpdate)txn.update());
        if (txnUpdate == null)
            return AsyncChains.success(null);

        long timestamp = executeAt.uniqueHlc();
        long nowInSec = MICROSECONDS.toSeconds(timestamp);
        boolean preserveTimestamps = txnUpdate.preserveTimestamps();
        boolean decodeWithOffset = txnUpdate.hasNormalisedTimes() && canDecodeNowInSecWithOffset(nowInSec);

        // TODO (expected): optimise for the common single update case; lots of lists allocated
        List<PartitionUpdate> updates = new ArrayList<>(1);
        if (!conditionalBlockBitSet.isEmpty())
        {
            // Apply updates not specified fully by the client but built from fragments completed by data from reads.
            // This occurs, for example, when an UPDATE statement uses a value assigned by a LET statement.
            // These are constructed in TxnUpdate.apply() and their reference operations are bound to executeAt by
            // AccordUpdateParameters, but they also contain the (normalised) base update's rows, and may have been
            // computed by a node that did not know AccordTimestamps.NORMALISED, so we always rebase them (they are small).
            forEachWithKey(key, write -> updates.add(preserveTimestamps ? write.deserialize(tables) : rebase(write.deserialize(tables), timestamp, nowInSec)));
            // Apply updates that are fully specified by the client and not reliant on data from reads.
            // ex. INSERT INTO tbl (a, b, c) VALUES (1, 2, 3)
            // These updates are persisted only in TxnUpdate and not in TxnWrite to avoid duplication.
            // If normalised, they are bound to executeAt by offsetting during deserialization; otherwise we must rebase.
            int completeFrom = updates.size();
            txnUpdate.completeWritesForKey(updates, conditionalBlockBitSet, (RoutableKey) key, tables,
                                           decodeWithOffset ? new TimeOffsetHeaderDeserializer(timestamp, nowInSec) : STABLE);

            if (!preserveTimestamps && !decodeWithOffset)
            {
                // rebase each before merging, so that tombstones (at timestamp - 1) do not shadow our own writes
                for (int i = completeFrom ; i < updates.size() ; ++i)
                    updates.set(i, rebase(updates.get(i), timestamp, nowInSec));
            }
        }

        if (updates.isEmpty())
            return AsyncChains.success(null);

        PartitionUpdate update = updates.size() == 1 ? updates.get(0) : PartitionUpdate.merge(updates);

        AccordExecutor executor = ((AccordCommandStore) commandStore).executor();
        return executor.continuationChain(() -> new Mutation(update, PotentialTxnConflicts.ALLOW).apply(false, false));
    }

    /**
     * Offsetting cannot apply the cap of {@link ExpirationDateOverflowHandling#computeLocalExpirationTime}, so we may only
     * bind normalised updates by offset if no TTL could reach the cap (until ~2086 for the 2106 cap; never for the
     * legacy 2038 cap); otherwise we rebase.
     */
    @VisibleForTesting
    static boolean canDecodeNowInSecWithOffset(long nowInSec)
    {
        return nowInSec + Attributes.MAX_TTL <= Cell.getVersionedMaxDeletionTime();
    }

    private static PartitionUpdate rebase(PartitionUpdate update, long timestamp, long nowInSec)
    {
        return new PartitionUpdate.Builder(update, 0).updateTimesForAccord(timestamp, nowInSec).build();
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (int i = 0 ; i < size() ; ++i)
            size += get(i).estimatedSizeOnHeap();
        return size;
    }

    public static final ParameterisedVersionedSerializer<TxnWrite, Seekables, Version> serializer = new ParameterisedVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnWrite write, Seekables keys, DataOutputPlus out, Version version) throws IOException
        {
            write.tables.serializeSelf(out);
            SimpleBitSetSerializers.any.serialize(write.conditionalBlockBitSet, out);
            serializeArray(write.items, new TableMetadatasAndKeys(write.tables, keys), out, version, LateBindWrite.serializer);
        }

        @Override
        public TxnWrite deserialize(Seekables keys, DataInputPlus in, Version version) throws IOException
        {
            TableMetadatas tables = TableMetadatas.deserializeSelf(in);
            SimpleBitSet conditionalBlockBitSet = SimpleBitSetSerializers.any.deserialize(in);
            return new TxnWrite(tables, deserializeArray(new TableMetadatasAndKeys(tables, keys), in, version, LateBindWrite.serializer, LateBindWrite[]::new), conditionalBlockBitSet);
        }

        @Override
        public void skip(Seekables keys, DataInputPlus in, Version version) throws IOException
        {
            TableMetadatas tables = TableMetadatas.deserializeSelf(in);
            SimpleBitSetSerializers.any.deserialize(in);
            skipArray(new TableMetadatasAndKeys(tables, keys), in, version, LateBindWrite.serializer);
        }

        @Override
        public long serializedSize(TxnWrite write, Seekables keys, Version version)
        {
            return write.tables.serializedSelfSize()
                   + SimpleBitSetSerializers.any.serializedSize(write.conditionalBlockBitSet)
                   + serializedArraySize(write.items, new TableMetadatasAndKeys(write.tables, keys), version, LateBindWrite.serializer);
        }
    };
}
