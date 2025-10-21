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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Data;
import accord.api.Update;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Ranges;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import accord.utils.Invariants;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.accord.AccordObjectSizes;
import org.apache.cassandra.service.accord.IAccordService;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnCondition.SerializedTxnCondition;
import org.apache.cassandra.service.accord.txn.TxnWrite.Fragment;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import static accord.utils.Invariants.requireArgument;
import static accord.utils.SortedArrays.Search.CEIL;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Boolean.FALSE;
import static org.apache.cassandra.service.accord.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.utils.ArraySerializers.deserializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializeArray;
import static org.apache.cassandra.utils.ArraySerializers.serializedArraySize;
import static org.apache.cassandra.utils.ArraySerializers.skipArray;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.skipWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class TxnUpdate extends AccordUpdate
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnUpdate(TableMetadatas.none(), null, new ByteBuffer[0], null, null, PreserveTimestamp.no));
    private static final int FLAG_PRESERVE_TIMESTAMPS = 0x1;

    final TableMetadatas tables;
    private final Keys keys;
    private final ByteBuffer[] fragments;
    private final SerializedTxnCondition condition;

    @Nullable
    private final ConsistencyLevel cassandraCommitCL;

    // Hints and batchlog want to write with the lower timestamp they generated when applying their writes via Accord
    // so they don't resurrect data if they are applied at a later time. Accord should be fine with this because
    // the writes are still deterministic from the perspective of coordinators/recovery coordinators.
    private final PreserveTimestamp preserveTimestamps;

    // Memoize computation of condition
    private Boolean conditionResult;

    public TxnUpdate(TableMetadatas tables, List<Fragment> fragments, TxnCondition condition, @Nullable ConsistencyLevel cassandraCommitCL, PreserveTimestamp preserveTimestamps)
    {
        requireArgument(cassandraCommitCL == null || IAccordService.SUPPORTED_COMMIT_CONSISTENCY_LEVELS.contains(cassandraCommitCL));
        this.tables = tables;
        this.keys = Keys.of(fragments, fragment -> fragment.key);
        fragments.sort(Fragment::compareKeys);
        // TODO (required): this node could be on version N while the peers are on N-1, which would have issues as the peers wouldn't know about N yet.
        //  Can not eagerly serialize until we know the "correct" version, else we need a way to fallback on mismatch.
        this.fragments = toSerializedValuesArray(keys, fragments, tables, Version.LATEST);
        // TODO (desired): slice TxnCondition, or pick a single shard to persist it
        this.condition = new SerializedTxnCondition(condition, tables);
        this.condition.unmemoize();
        this.condition.deserialize(tables);
        this.cassandraCommitCL = cassandraCommitCL;
        this.preserveTimestamps = preserveTimestamps;
    }

    private TxnUpdate(TableMetadatas tables, Keys keys, ByteBuffer[] fragments, SerializedTxnCondition condition, ConsistencyLevel cassandraCommitCL, PreserveTimestamp preserveTimestamps)
    {
        this.tables = tables;
        this.keys = keys;
        this.fragments = fragments;
        this.condition = condition;
        this.cassandraCommitCL = cassandraCommitCL;
        this.preserveTimestamps = preserveTimestamps;
    }

    public static TxnUpdate empty()
    {
        return new TxnUpdate(TableMetadatas.none(), Collections.emptyList(), TxnCondition.none(), null, PreserveTimestamp.no);
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE + condition.estimatedSizeOnHeap();
        for (ByteBuffer update : fragments)
            size += ByteBufferUtil.estimatedSizeOnHeap(update);
        size += AccordObjectSizes.keys(keys);
        return size;
    }

    @Override
    public String toString()
    {
        return "TxnUpdate{updates=" + deserialize(keys, tables, fragments) +
               ", condition=" + condition.deserialize(tables) + '}';
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TxnUpdate txnUpdate = (TxnUpdate) o;
        return Arrays.equals(fragments, txnUpdate.fragments) && Objects.equals(condition, txnUpdate.condition);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(condition);
        result = 31 * result + Arrays.hashCode(fragments);
        return result;
    }

    @Override
    public Keys keys()
    {
        // TODO: It doesn't seem to affect correctness, but should we return the union of the fragment + condition keys?
        return keys;
    }

    // Batch log and hints want to keep their lower timestamp for the applied writes to avoid resurrecting old data
    // when they are applied later, possibly after further updates have already been acknowledged.
    public PreserveTimestamp preserveTimestamps()
    {
        return preserveTimestamps;
    }

    @Override
    public Update slice(Ranges ranges)
    {
        Keys keys = this.keys.slice(ranges);
        // TODO (desired): Slice the condition.
        return new TxnUpdate(tables, keys, select(this.keys, keys, fragments), condition, cassandraCommitCL, preserveTimestamps);
    }

    @Override
    public Update intersecting(Participants<?> participants)
    {
        Keys keys = this.keys.intersecting(participants);
        // TODO (desired): Slice the condition.
        return new TxnUpdate(tables, keys, select(this.keys, keys, fragments), condition, cassandraCommitCL, preserveTimestamps);
    }

    private static ByteBuffer[] select(Keys in, Keys out, ByteBuffer[] from)
    {
        ByteBuffer[] result = new ByteBuffer[out.size()];
        int j = 0;
        for (int i = 0 ; i < out.size() ; ++i)
        {
            j = in.findNext(j, out.get(i), CEIL);
            result[i] = from[j];
        }
        return result;
    }

    @Override
    public Update merge(Update update)
    {
        TxnUpdate that = (TxnUpdate) update;
        Keys mergedKeys = this.keys.with(that.keys);
        // TODO (desired): special method for linear merging keyed and non-keyed lists simultaneously
        ByteBuffer[] mergedFragments = merge(this.keys, that.keys, this.fragments, that.fragments, mergedKeys.size());
        return new TxnUpdate(tables, mergedKeys, mergedFragments, condition, cassandraCommitCL, preserveTimestamps);
    }

    private static ByteBuffer[] merge(Keys leftKeys, Keys rightKeys, ByteBuffer[] left, ByteBuffer[] right, int outputSize)
    {
        ByteBuffer[] out = new ByteBuffer[outputSize];
        int l = 0, r = 0, o = 0;
        while (l < leftKeys.size() && r < rightKeys.size())
        {
            int c = leftKeys.get(l).compareTo(rightKeys.get(r));
            if (c < 0) { out[o++] = left[l++]; }
            else if (c > 0) { out[o++] = right[r++]; }
            else if (ByteBufferUtil.compareUnsigned(left[l], right[r]) != 0) { throw new IllegalStateException("The same keys have different values in each input"); }
            else { out[o++] = left[l++]; r++; }
        }
        while (l < leftKeys.size()) { out[o++] = left[l++]; }
        while (r < rightKeys.size()) { out[o++] = right[r++]; }
        return out;
    }

    @Override
    public TxnWrite apply(Timestamp executeAt, Data data)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        checkState(cm.epoch.getEpoch() >= executeAt.epoch(), "TCM epoch %d is < executeAt epoch %d", cm.epoch.getEpoch(), executeAt.epoch());
        if (!checkCondition(data))
            return TxnWrite.EMPTY_CONDITION_FAILED;

        if (keys.isEmpty())
            return new TxnWrite(TableMetadatas.none(), Collections.emptyList(), true);

        List<Fragment> fragments = deserialize(keys, tables, this.fragments);
        List<TxnWrite.Update> updates = new ArrayList<>(fragments.size());
        QueryOptions options = QueryOptions.forProtocolVersion(ProtocolVersion.CURRENT);
        AccordUpdateParameters parameters = new AccordUpdateParameters((TxnData) data, options, executeAt.uniqueHlc());

        for (Fragment fragment : fragments)
            // Filter out fragments that already constitute complete updates to avoid persisting them via TxnWrite:
            if (!fragment.isComplete())
                updates.add(fragment.complete(parameters, tables));

        return new TxnWrite(tables, updates, true);
    }

    public List<TxnWrite.Update> completeUpdatesForKey(RoutableKey key)
    {
        List<Fragment> fragments = deserialize(keys, tables, this.fragments);
        List<TxnWrite.Update> updates = new ArrayList<>(fragments.size());

        for (Fragment fragment : fragments)
            if (fragment.isComplete() && fragment.key.equals(key))
                updates.add(fragment.toUpdate(tables));

        return updates;
    }

    public static final AccordUpdateSerializer<TxnUpdate> serializer = new AccordUpdateSerializer<>()
    {
        @Override
        public void serialize(TxnUpdate update, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
        {
            // Serializing it with the condition result set shouldn't be needed
            checkState(update.conditionResult == null, "Can't serialize if conditionResult is set without adding it to serialization");
            // Once in accord "mixedTimeSource" and "yes" are the same, so only care about the side effect: that the timestamp is preserved or not
            out.writeByte(update.preserveTimestamps.preserve ? FLAG_PRESERVE_TIMESTAMPS : 0);
            tablesAndKeys.serializeKeys(update.keys, out);
            writeWithVIntLength(update.condition.bytes(), out);
            serializeArray(update.fragments, out, ByteBufferUtil.byteBufferSerializer);
            serializeNullable(update.cassandraCommitCL, out, consistencyLevelSerializer);
        }

        @Override
        public TxnUpdate deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            int flags = in.readByte();
            boolean preserveTimestamps = (FLAG_PRESERVE_TIMESTAMPS & flags) == 1;
            Keys keys = tablesAndKeys.deserializeKeys(in);
            ByteBuffer condition = readWithVIntLength(in);
            ByteBuffer[] fragments = deserializeArray(in, ByteBufferUtil.byteBufferSerializer, ByteBuffer[]::new);
            ConsistencyLevel consistencyLevel = deserializeNullable(in, consistencyLevelSerializer);
            return new TxnUpdate(tablesAndKeys.tables, keys, fragments, new SerializedTxnCondition(condition), consistencyLevel, preserveTimestamps ? PreserveTimestamp.yes : PreserveTimestamp.no);
        }

        @Override
        public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            in.readByte();
            tablesAndKeys.skipKeys(in);
            skipWithVIntLength(in);
            skipArray(in, ByteBufferUtil.byteBufferSerializer);
            deserializeNullable(in, consistencyLevelSerializer);
        }

        @Override
        public long serializedSize(TxnUpdate update, TableMetadatasAndKeys tablesAndKeys, Version version)
        {
            long size = 1; // flags
            size += tablesAndKeys.serializedKeysSize(update.keys);
            size += serializedSizeWithVIntLength(update.condition.bytes());
            size += serializedArraySize(update.fragments, ByteBufferUtil.byteBufferSerializer);
            size += serializedNullableSize(update.cassandraCommitCL, consistencyLevelSerializer);
            return size;
        }
    };

    private static ByteBuffer[] toSerializedValuesArray(Keys keys, List<Fragment> items, TableMetadatas tables, Version version)
    {
        ByteBuffer[] result = new ByteBuffer[keys.size()];
        int i = 0, mi = items.size(), ki = 0;
        while (i < mi)
        {
            PartitionKey key = items.get(i).key;
            int j = i + 1;
            while (j < mi && items.get(j).key.equals(key))
                ++j;

            int nextki = keys.findNext(ki, key, CEIL);
            Arrays.fill(result, ki, nextki, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ki = nextki;
            result[ki++] = toSerializedValues(items, tables, i, j, version);
            i = j;
        }
        Arrays.fill(result, ki, result.length, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        return result;
    }

    private static ByteBuffer toSerializedValues(List<Fragment> items, TableMetadatas tables, int start, int end, Version version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(version.version) + TypeSizes.sizeofUnsignedVInt(end - start);
        for (int i = start ; i < end ; ++i)
            size += Fragment.serializer.serializedSize(items.get(i), tables, version);

        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeUnsignedVInt32(version.version);
            out.writeUnsignedVInt32(end - start);
            for (int i = start ; i < end ; ++i)
                Fragment.serializer.serialize(items.get(i), tables, out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static List<Fragment> deserialize(PartitionKey key, TableMetadatas tables, ByteBuffer bytes)
    {
        if (!bytes.hasRemaining())
            return Collections.emptyList();

        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            Version version = Version.fromVersion(in.readUnsignedVInt32());
            int count = in.readUnsignedVInt32();
            switch (count)
            {
                case 0: throw new IllegalStateException();
                case 1: return Collections.singletonList(Fragment.serializer.deserialize(key, tables, in, version));
                default:
                    List<Fragment> result = new ArrayList<>();
                    for (int i = 0 ; i < count ; ++i)
                        result.add(Fragment.serializer.deserialize(key, tables, in, version));
                    return result;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static List<Fragment> deserialize(Keys keys, TableMetadatas tables, ByteBuffer[] buffers)
    {
        Invariants.require(keys.size() == buffers.length);
        List<Fragment> result = new ArrayList<>(buffers.length);
        for (int i = 0 ; i < keys.size() ; ++i)
            result.addAll(deserialize((PartitionKey) keys.get(i), tables, buffers[i]));
        return result;
    }

    @Override
    public void failCondition()
    {
        conditionResult = FALSE;
    }

    @Override
    public boolean checkCondition(Data data)
    {
        // Assert data that was memoized is same as data that is provided?
        if (conditionResult != null)
            return conditionResult;
        TxnCondition condition = this.condition.deserialize(tables);
        if (condition == TxnCondition.none())
            return conditionResult = true;
        return conditionResult = condition.applies((TxnData) data);
    }

    @Override
    public Kind kind()
    {
        return Kind.TXN;
    }

    @Override
    public ConsistencyLevel cassandraCommitCL()
    {
        return cassandraCommitCL;
    }

    @VisibleForTesting
    public void unsafeResetCondition()
    {
        conditionResult = null;
    }
}
