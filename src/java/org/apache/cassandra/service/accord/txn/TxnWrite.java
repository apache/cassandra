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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.DataStore;
import accord.api.Write;
import accord.local.SafeCommandStore;
import accord.primitives.PartialTxn;
import accord.primitives.RoutableKey;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.Writes;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Columns;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.service.accord.AccordSafeCommandStore;
import org.apache.cassandra.service.accord.AccordSafeCommandsForKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.BooleanSerializer;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static org.apache.cassandra.cql3.Lists.accordListPathSupplier;
import static org.apache.cassandra.service.accord.AccordSerializers.partitionUpdateSerializer;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;

public class TxnWrite extends AbstractKeySorted<TxnWrite.Update> implements Write
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnWrite.class);

    public static final TxnWrite EMPTY_CONDITION_FAILED = new TxnWrite(Collections.emptyList(), false);

    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY_CONDITION_FAILED);

    public static class Update extends AbstractSerialized<PartitionUpdate>
    {
        private static final long EMPTY_SIZE = ObjectSizes.measure(new Update(null, 0, (ByteBuffer) null));
        public final PartitionKey key;
        public final int index;

        public Update(PartitionKey key, int index, PartitionUpdate update)
        {
            super(update);
            this.key = key;
            this.index = index;
        }

        private Update(PartitionKey key, int index, ByteBuffer bytes)
        {
            super(bytes);
            this.key = key;
            this.index = index;
        }

        long estimatedSizeOnHeap()
        {
            return EMPTY_SIZE
                   + key.estimatedSizeOnHeap()
                   + ByteBufferUtil.estimatedSizeOnHeap(bytes());
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
                   ", update=" + get() +
                   '}';
        }

        public AsyncChain<Void> write(@Nonnull Function<Cell, CellPath> cellToMaybeNewListPath, long timestamp, int nowInSeconds)
        {
            PartitionUpdate update = new PartitionUpdate.Builder(get(), 0).updateTimesAndPathsForAccord(cellToMaybeNewListPath, timestamp, nowInSeconds).build();
            Mutation mutation = new Mutation(update);
            return AsyncChains.ofRunnable(Stage.MUTATION.executor(), mutation::apply);
        }

        @Override
        protected IVersionedSerializer<PartitionUpdate> serializer()
        {
            return partitionUpdateSerializer;
        }

        public static final IVersionedSerializer<Update> serializer = new IVersionedSerializer<Update>()
        {
            @Override
            public void serialize(Update write, DataOutputPlus out, int version) throws IOException
            {
                PartitionKey.serializer.serialize(write.key, out, version);
                out.writeInt(write.index);
                ByteBufferUtil.writeWithVIntLength(write.bytes(), out);

            }

            @Override
            public Update deserialize(DataInputPlus in, int version) throws IOException
            {
                PartitionKey key = PartitionKey.serializer.deserialize(in, version);
                int index = in.readInt();
                ByteBuffer bytes = ByteBufferUtil.readWithVIntLength(in);
                return new Update(key, index, bytes);
            }

            @Override
            public long serializedSize(Update write, int version)
            {
                long size = 0;
                size += PartitionKey.serializer.serializedSize(write.key, version);
                size += TypeSizes.INT_SIZE;
                size += ByteBufferUtil.serializedSizeWithVIntLength(write.bytes());
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

        public Fragment(PartitionKey key, int index, PartitionUpdate baseUpdate, TxnReferenceOperations referenceOps)
        {
            this.key = key;
            this.index = index;
            this.baseUpdate = baseUpdate;
            this.referenceOps = referenceOps;
        }

        public Fragment(int index, PartitionUpdate baseUpdate, TxnReferenceOperations referenceOps)
        {
            this(PartitionKey.of(baseUpdate), index, baseUpdate, referenceOps);
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

        public Update toUpdate()
        {
            return new Update(key, index, baseUpdate);
        }

        public Update complete(AccordUpdateParameters parameters)
        {
            if (isComplete())
                return toUpdate();

            DecoratedKey key = baseUpdate.partitionKey();
            PartitionUpdate.Builder updateBuilder = new PartitionUpdate.Builder(baseUpdate.metadata(),
                                                                                key,
                                                                                columns(baseUpdate, referenceOps),
                                                                                baseUpdate.rowCount(),
                                                                                baseUpdate.canHaveShadowedData());

            UpdateParameters up = parameters.updateParameters(baseUpdate.metadata(), key, index);
            TxnData data = parameters.getData();
            Row staticRow = applyUpdates(baseUpdate.staticRow(), referenceOps.statics, key, Clustering.STATIC_CLUSTERING, up, data);

            if (!staticRow.isEmpty())
                updateBuilder.add(staticRow);

            Row existing = !baseUpdate.isEmpty() ? Iterables.getOnlyElement(baseUpdate) : null;
            Row row = applyUpdates(existing, referenceOps.regulars, key, referenceOps.clustering, up, data);
            if (row != null)
                updateBuilder.add(row);

            return new Update(this.key, index, updateBuilder.build());
        }

        private static Columns columns(Columns current, List<TxnReferenceOperation> referenceOps)
        {
            if (referenceOps.isEmpty())
                return current;

            Set<ColumnMetadata> combined = new HashSet<>(current);
            referenceOps.forEach(op -> combined.add(op.receiver()));
            return Columns.from(combined);
        }

        private static RegularAndStaticColumns columns(PartitionUpdate update, TxnReferenceOperations referenceOps)
        {
            Preconditions.checkState(!referenceOps.isEmpty());
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
                Preconditions.checkState(existing.clustering().equals(clustering));
                up.addRow(existing);
            }
            else
                up.newRow(clustering);

            operations.forEach(op -> op.apply(data, key, up));
            return up.buildRow();
        }

        static final IVersionedSerializer<Fragment> serializer = new IVersionedSerializer<Fragment>()
        {
            @Override
            public void serialize(Fragment fragment, DataOutputPlus out, int version) throws IOException
            {
                PartitionKey.serializer.serialize(fragment.key, out, version);
                out.writeInt(fragment.index);
                partitionUpdateSerializer.serialize(fragment.baseUpdate, out, version);
                TxnReferenceOperations.serializer.serialize(fragment.referenceOps, out, version);
            }

            @Override
            public Fragment deserialize(DataInputPlus in, int version) throws IOException
            {
                PartitionKey key = PartitionKey.serializer.deserialize(in, version);
                int idx = in.readInt();
                PartitionUpdate baseUpdate = partitionUpdateSerializer.deserialize(in, version);
                TxnReferenceOperations referenceOps = TxnReferenceOperations.serializer.deserialize(in, version);
                return new Fragment(key, idx, baseUpdate, referenceOps);
            }

            @Override
            public long serializedSize(Fragment fragment, int version)
            {
                long size = 0;
                size += PartitionKey.serializer.serializedSize(fragment.key, version);
                size += TypeSizes.INT_SIZE;
                size += partitionUpdateSerializer.serializedSize(fragment.baseUpdate, version);
                size += TxnReferenceOperations.serializer.serializedSize(fragment.referenceOps, version);
                return size;
            }
        };
    }

    private final boolean isConditionMet;

    private TxnWrite(Update[] items, boolean isConditionMet)
    {
        super(items);
        this.isConditionMet = isConditionMet;
    }

    public TxnWrite(List<Update> items, boolean isConditionMet)
    {
        super(items);
        this.isConditionMet = isConditionMet;
    }

    @Override
    int compareNonKeyFields(Update left, Update right)
    {
        return Integer.compare(left.index, right.index);
    }

    @Override
    PartitionKey getKey(Update item)
    {
        return item.key;
    }

    @Override
    Update[] newArray(int size)
    {
        return new Update[size];
    }

    @Override
    public AsyncChain<Void> apply(Seekable key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store, PartialTxn txn)
    {
        // TODO (expected, efficiency): 99.9999% of the time we can just use executeAt.hlc(), so can avoid bringing
        //  cfk into memory by retaining at all times in memory key ranges that are dirty and must use this logic;
        //  any that aren't can just use executeAt.hlc
        AccordSafeCommandsForKey cfk = ((AccordSafeCommandStore) safeStore).commandsForKey((RoutableKey) key);
        cfk.updateLastExecutionTimestamps(executeAt, true);
        long timestamp = cfk.timestampMicrosFor(executeAt, true);
        // TODO (low priority - do we need to compute nowInSeconds, or can we just use executeAt?)
        int nowInSeconds = cfk.nowInSecondsFor(executeAt, true);

        List<AsyncChain<Void>> results = new ArrayList<>();

        // Apply updates not specified fully by the client but built from fragments completed by data from reads.
        // This occurs, for example, when an UPDATE statement uses a value assigned by a LET statement.
        Function<Cell, CellPath> accordListPathSuppler = accordListPathSupplier(timestamp);
        forEachWithKey((PartitionKey) key, write -> results.add(write.write(accordListPathSuppler, timestamp, nowInSeconds)));

        if (isConditionMet)
        {
            // Apply updates that are fully specified by the client and not reliant on data from reads.
            // ex. INSERT INTO tbl (a, b, c) VALUES (1, 2, 3)
            // These updates are persisted only in TxnUpdate and not in TxnWrite to avoid duplication.
            TxnUpdate txnUpdate = (TxnUpdate) txn.update();
            assert txnUpdate != null : "PartialTxn should contain an update if we're applying a write!";
            List<Update> updates = txnUpdate.completeUpdatesForKey((RoutableKey) key);
            updates.forEach(update -> results.add(update.write(accordListPathSuppler, timestamp, nowInSeconds)));
        }

        if (results.isEmpty())
            return Writes.SUCCESS;

        if (results.size() == 1)
            return results.get(0).flatMap(o -> Writes.SUCCESS);

        return AsyncChains.all(results).flatMap(objects -> Writes.SUCCESS);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        for (Update update : this)
            size += update.estimatedSizeOnHeap();
        return size;
    }

    public static final IVersionedSerializer<TxnWrite> serializer = new IVersionedSerializer<TxnWrite>()
    {
        @Override
        public void serialize(TxnWrite write, DataOutputPlus out, int version) throws IOException
        {
            BooleanSerializer.serializer.serialize(write.isConditionMet, out, version);
            serializeArray(write.items, out, version, Update.serializer);
        }

        @Override
        public TxnWrite deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean isConditionMet = BooleanSerializer.serializer.deserialize(in, version);
            return new TxnWrite(deserializeArray(in, version, Update.serializer, Update[]::new), isConditionMet);
        }

        @Override
        public long serializedSize(TxnWrite write, int version)
        {
            return BooleanSerializer.serializer.serializedSize(write.isConditionMet, version) + serializedArraySize(write.items, version, Update.serializer);
        }
    };
}
