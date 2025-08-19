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
import java.util.concurrent.Executor;

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
import accord.primitives.Writes;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand.PotentialTxnConflicts;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.utils.BooleanSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.db.rows.DeserializationHelper.Flag.FROM_REMOTE;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.ArraySerializers.skipArray;

public class TxnWrite extends AbstractKeySorted<TxnWrite.Update> implements Write
{
    public static final long NO_TIMESTAMP = 0;

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnWrite.class);

    public static final TxnWrite EMPTY_CONDITION_FAILED = new TxnWrite(TableMetadatas.none(), Collections.emptyList(), false);

    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY_CONDITION_FAILED);

    public static class Update extends AbstractParameterisedVersionedSerialized<PartitionUpdate, TableMetadatas>
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Update(null, 0, ByteBufferUtil.EMPTY_BYTE_BUFFER));
        public final PartitionKey key;
        public final int index;

        public Update(PartitionKey key, int index, PartitionUpdate update, TableMetadatas tables)
        {
            this(key, index, serializeInternal(update, tables, Version.LATEST));
        }

        private Update(PartitionKey key, int index, ByteBuffer latestVersionBytes)
        {
            super(latestVersionBytes);
            this.key = key;
            this.index = index;
        }

        @Override
        public long estimatedSizeOnHeap()
        {
            // we don't measure the key, as this is shared
            return EMPTY_SIZE + ByteBufferUtil.estimatedSizeOnHeap(unsafeBytes());
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Update update = (Update) o;
            return index == update.index && key.equals(update.key);
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

        public AsyncChain<Void> write(Executor executor, TableMetadatas tables, boolean preserveTimestamps, long timestamp)
        {
            PartitionUpdate update = deserialize(tables);
            if (!preserveTimestamps)
                update = new PartitionUpdate.Builder(update, 0).updateAllTimestamp(timestamp).build();
            Mutation mutation = new Mutation(update, PotentialTxnConflicts.ALLOW);
            return AsyncChains.ofRunnable(executor, () -> mutation.apply(false, false));
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

        public static final ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version> serializer = new ParameterisedVersionedSerializer<Update, TableMetadatasAndKeys, Version>()
        {
            @Override
            public void serialize(Update write, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
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
            public Update deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
            {
                PartitionKey key = tablesAndKeys.deserializeKey(in);
                int index = in.readInt();
                ByteBuffer bytes = ByteBufferUtil.readWithVIntLength(in);
                if (version != Version.LATEST)
                    bytes = reserialize(bytes, tablesAndKeys, version, Version.LATEST);
                return new Update(key, index, bytes);
            }

            @Override
            public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
            {
                PartitionKey key = tablesAndKeys.deserializeKey(in);
                int index = in.readInt();
                ByteBufferUtil.skipWithVIntLength(in);
            }

            @Override
            public long serializedSize(Update write, TableMetadatasAndKeys tablesAndKeys, Version version)
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
        public final long timestamp;

        public Fragment(PartitionKey key, int index, PartitionUpdate baseUpdate, TxnReferenceOperations referenceOps, long timestamp)
        {
            this.key = key;
            this.index = index;
            this.baseUpdate = baseUpdate;
            this.referenceOps = referenceOps;
            this.timestamp = timestamp;
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
            return index == fragment.index && key.equals(fragment.key) && baseUpdate.equals(fragment.baseUpdate) && referenceOps.equals(fragment.referenceOps);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, index, baseUpdate, referenceOps);
        }

        @Override
        public String toString()
        {
            return "Fragment{key=" + key + ", index=" + index + ", baseUpdate=" + baseUpdate + ", referenceOps=" + referenceOps + '}';
        }

        public boolean isComplete()
        {
            return referenceOps.isEmpty();
        }

        public Update toUpdate(TableMetadatas tables)
        {
            return new Update(key, index, baseUpdate, tables);
        }

        public Update complete(AccordUpdateParameters parameters, TableMetadatas tables)
        {
            if (isComplete())
                return toUpdate(tables);

            DecoratedKey key = baseUpdate.partitionKey();
            PartitionUpdate.Builder updateBuilder = new PartitionUpdate.Builder(baseUpdate.metadata(),
                                                                                key,
                                                                                columns(baseUpdate, referenceOps),
                                                                                baseUpdate.rowCount(),
                                                                                baseUpdate.canHaveShadowedData());

            UpdateParameters up = parameters.updateParameters(baseUpdate.metadata(), key, index, timestamp);
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

            return new Update(this.key, index, updateBuilder.build(), tables);
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

        private static Row applyUpdates(Row existing, List<TxnReferenceOperation> operations, DecoratedKey key, Clustering<?> clustering, UpdateParameters up, TxnData data)
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
                out.writeUnsignedVInt(fragment.timestamp);
            }

            public Fragment deserialize(PartitionKey key, TableMetadatas tables, DataInputPlus in, Version version) throws IOException
            {
                int idx = in.readUnsignedVInt32();
                // TODO (required): why FROM_REMOTE?
                PartitionUpdate baseUpdate = PartitionUpdate.serializer.deserialize(key, tables, in, version.messageVersion(), FROM_REMOTE);
                TxnReferenceOperations referenceOps = TxnReferenceOperations.serializer.deserialize(tables, in, version);
                long timestamp = in.readUnsignedVInt();
                return new Fragment(key, idx, baseUpdate, referenceOps, timestamp);
            }

            public long serializedSize(Fragment fragment, TableMetadatas tables, Version version)
            {
                long size = 0;
                size += TypeSizes.sizeofUnsignedVInt(fragment.index);
                size += PartitionUpdate.serializer.serializedSizeWithoutKey(fragment.baseUpdate, tables, version.messageVersion());
                size += TxnReferenceOperations.serializer.serializedSize(fragment.referenceOps, tables, version);
                size += TypeSizes.sizeofUnsignedVInt(fragment.timestamp);
                return size;
            }
        }
    }

    public final TableMetadatas tables;
    private final boolean isConditionMet;

    private TxnWrite(TableMetadatas tables, Update[] items, boolean isConditionMet)
    {
        super(items, Domain.Key);
        this.tables = tables;
        this.isConditionMet = isConditionMet;
    }

    public TxnWrite(TableMetadatas tables, List<Update> items, boolean isConditionMet)
    {
        super(items, Domain.Key);
        this.tables = tables;
        this.isConditionMet = isConditionMet;
    }

    @Override
    int compareNonKeyFields(Update left, Update right)
    {
        return Integer.compare(left.index, right.index);
    }

    @Override
    Seekable getKey(Update item)
    {
        return item.key;
    }

    @Override
    Update[] newArray(int size)
    {
        return new Update[size];
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
            return Writes.SUCCESS;

        long timestamp = executeAt.uniqueHlc();

        // TODO (expected): optimise for the common single update case; lots of lists allocated
        List<AsyncChain<Void>> results = new ArrayList<>();
        if (isConditionMet)
        {
            AccordExecutor executor = ((AccordCommandStore) commandStore).executor();
            boolean preserveTimestamps = txnUpdate.preserveTimestamps().preserve;
            // Apply updates not specified fully by the client but built from fragments completed by data from reads.
            // This occurs, for example, when an UPDATE statement uses a value assigned by a LET statement.
            forEachWithKey(key, write -> results.add(write.write(executor, tables, preserveTimestamps, timestamp)));
            // Apply updates that are fully specified by the client and not reliant on data from reads.
            // ex. INSERT INTO tbl (a, b, c) VALUES (1, 2, 3)
            // These updates are persisted only in TxnUpdate and not in TxnWrite to avoid duplication.
            List<Update> updates = txnUpdate.completeUpdatesForKey((RoutableKey) key);
            updates.forEach(write -> results.add(write.write(executor, tables, preserveTimestamps, timestamp)));
        }

        if (results.isEmpty())
            return Writes.SUCCESS;

        if (results.size() == 1)
            return results.get(0).flatMap(o -> Writes.SUCCESS);

        return AsyncChains.reduce(results, (i1, i2) -> null, (Void)null).flatMap(ignore -> Writes.SUCCESS);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (Update update : this)
            size += update.estimatedSizeOnHeap();
        return size;
    }

    public static final ParameterisedVersionedSerializer<TxnWrite, Seekables, Version> serializer = new ParameterisedVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnWrite write, Seekables keys, DataOutputPlus out, Version version) throws IOException
        {
            write.tables.serializeSelf(out);
            BooleanSerializer.serializer.serialize(write.isConditionMet, out);
            serializeArray(write.items, new TableMetadatasAndKeys(write.tables, keys), out, version, Update.serializer);
        }

        @Override
        public TxnWrite deserialize(Seekables keys, DataInputPlus in, Version version) throws IOException
        {
            TableMetadatas tables = TableMetadatas.deserializeSelf(in);
            boolean isConditionMet = BooleanSerializer.serializer.deserialize(in);
            return new TxnWrite(tables, deserializeArray(new TableMetadatasAndKeys(tables, keys), in, version, Update.serializer, Update[]::new), isConditionMet);
        }

        @Override
        public void skip(Seekables keys, DataInputPlus in, Version version) throws IOException
        {
            TableMetadatas tables = TableMetadatas.deserializeSelf(in);
            BooleanSerializer.serializer.deserialize(in);
            skipArray(new TableMetadatasAndKeys(tables, keys), in, version, Update.serializer);
        }

        @Override
        public long serializedSize(TxnWrite write, Seekables keys, Version version)
        {
            return write.tables.serializedSelfSize()
                   + BooleanSerializer.serializer.serializedSize(write.isConditionMet)
                   + serializedArraySize(write.items, new TableMetadatasAndKeys(write.tables, keys), version, Update.serializer);
        }
    };
}
