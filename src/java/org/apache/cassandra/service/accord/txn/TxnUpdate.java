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

import accord.api.Data;
import accord.api.Key;
import accord.api.Update;
import accord.api.Write;
import accord.primitives.Keys;
import accord.primitives.Ranges;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.accord.AccordSerializers;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.ObjectSizes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;

import static java.lang.Math.toIntExact;
import static org.apache.cassandra.service.accord.AccordSerializers.serialize;
import static org.apache.cassandra.utils.ArraySerializers.*;
import static org.apache.cassandra.utils.ByteBufferUtil.*;

public class TxnUpdate implements Update
{
    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnUpdate(null, new ByteBuffer[0], null));

    private final Keys keys;
    private final ByteBuffer[] fragments;
    private final ByteBuffer condition;

    // Memoize computation of condition
    private Boolean conditionResult;

    public TxnUpdate(List<TxnWrite.Fragment> fragments, TxnCondition condition)
    {
        // TODO: Figure out a way to shove keys into TxnCondition, and have it implement slice/merge.
        this.keys = Keys.of(fragments, fragment -> fragment.key);
        fragments.sort(TxnWrite.Fragment::compareKeys);
        this.fragments = toSerializedValuesArray(keys, fragments, fragment -> fragment.key, TxnWrite.Fragment.serializer);
        this.condition = serialize(condition, TxnCondition.serializer);
    }

    private TxnUpdate(Keys keys, ByteBuffer[] fragments, ByteBuffer condition)
    {
        this.keys = keys;
        this.fragments = fragments;
        this.condition = condition;
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE + ByteBufferUtil.estimatedSizeOnHeap(condition);
        for (ByteBuffer update : fragments)
            size += ByteBufferUtil.estimatedSizeOnHeap(update);
        return size;
    }

    @Override
    public String toString()
    {
        return "TxnUpdate{updates=" + deserialize(fragments, TxnWrite.Fragment.serializer) +
               ", condition=" + AccordSerializers.deserialize(condition, TxnCondition.serializer) + '}';
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

    @Override
    public Update slice(Ranges ranges)
    {
        Keys keys = this.keys.slice(ranges);
        // TODO: Slice the condition.
        return new TxnUpdate(keys, select(this.keys, keys, fragments), condition);
    }

    private static ByteBuffer[] select(Keys in, Keys out, ByteBuffer[] from)
    {
        ByteBuffer[] result = new ByteBuffer[out.size()];
        int j = 0;
        for (int i = 0 ; i < out.size() ; ++i)
        {
            j = in.findNext(out.get(i), j);
            result[i] = from[j];
        }
        return result;
    }

    @Override
    public Update merge(Update update)
    {
        // TODO: special method for linear merging keyed and non-keyed lists simultaneously
        TxnUpdate that = (TxnUpdate) update;
        Keys mergedKeys = this.keys.union(that.keys);
        ByteBuffer[] mergedFragments = merge(this.keys, that.keys, this.fragments, that.fragments, mergedKeys.size());
        return new TxnUpdate(mergedKeys, mergedFragments, condition);
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
        while (l < leftKeys.size()) { out[o++] = left[l]; }
        while (r < rightKeys.size()) { out[o++] = right[r++]; }
        return out;
    }

    @Override
    public Write apply(Data data)
    {
        if (!checkCondition(data))
            return TxnWrite.EMPTY;

        List<TxnWrite.Fragment> fragments = deserialize(this.fragments, TxnWrite.Fragment.serializer);
        List<TxnWrite.Update> updates = new ArrayList<>(fragments.size());
        QueryOptions options = QueryOptions.forProtocolVersion(ProtocolVersion.CURRENT);
        AccordUpdateParameters parameters = new AccordUpdateParameters((TxnData) data, options);

        for (TxnWrite.Fragment fragment : fragments)
            updates.add(fragment.complete(parameters));

        return new TxnWrite(updates);
    }

    // Should we serialize the conditionResult?
    public static final IVersionedSerializer<TxnUpdate> serializer = new IVersionedSerializer<TxnUpdate>()
    {
        @Override
        public void serialize(TxnUpdate update, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.keys.serialize(update.keys, out, version);
            writeWithVIntLength(update.condition, out);
            serializeArray(update.fragments, out, version, ByteBufferUtil.vintSerializer);
        }

        @Override
        public TxnUpdate deserialize(DataInputPlus in, int version) throws IOException
        {
            Keys keys = KeySerializers.keys.deserialize(in, version);
            ByteBuffer condition = readWithVIntLength(in);
            ByteBuffer[] fragments = deserializeArray(in, version, ByteBufferUtil.vintSerializer, ByteBuffer[]::new);
            return new TxnUpdate(keys, fragments, condition);
        }

        @Override
        public long serializedSize(TxnUpdate update, int version)
        {
            long size = KeySerializers.keys.serializedSize(update.keys, version);
            size += serializedSizeWithVIntLength(update.condition);
            size += serializedArraySize(update.fragments, version, ByteBufferUtil.vintSerializer);
            assert(ByteBufferUtil.serialized(this, update, version).remaining() == size);
            return size;
        }
    };

    private static <T> ByteBuffer[] toSerializedValuesArray(Keys keys, List<T> items, Function<? super T, ? extends Key> toKey, IVersionedSerializer<T> serializer)
    {
        ByteBuffer[] result = new ByteBuffer[keys.size()];
        int i = 0, mi = items.size(), ki = 0;
        while (i < mi)
        {
            Key key = toKey.apply(items.get(i));
            int j = i + 1;
            while (j < mi && toKey.apply(items.get(j)).equals(key))
                ++j;

            int nextki = keys.findNext(key, ki);
            Arrays.fill(result, ki, nextki, ByteBufferUtil.EMPTY_BYTE_BUFFER);
            ki = nextki;
            result[ki++] = toSerializedValues(items, i, j, serializer, MessagingService.current_version);
            i = j;
        }
        Arrays.fill(result, ki, result.length, ByteBufferUtil.EMPTY_BYTE_BUFFER);
        return result;
    }

    private static <T> ByteBuffer toSerializedValues(List<T> items, int start, int end, IVersionedSerializer<T> serializer, int version)
    {
        long size = TypeSizes.sizeofUnsignedVInt(version) + TypeSizes.sizeofUnsignedVInt(end - start);
        for (int i = start ; i < end ; ++i)
            size += serializer.serializedSize(items.get(i), version);

        try (DataOutputBuffer out = new DataOutputBuffer((int) size))
        {
            out.writeUnsignedVInt(version);
            out.writeUnsignedVInt(end - start);
            for (int i = start ; i < end ; ++i)
                serializer.serialize(items.get(i), out, version);
            return out.buffer(false);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> List<T> deserialize(ByteBuffer bytes, IVersionedSerializer<T> serializer)
    {
        if (!bytes.hasRemaining())
            return Collections.emptyList();

        try (DataInputBuffer in = new DataInputBuffer(bytes, true))
        {
            int version = toIntExact(in.readUnsignedVInt());
            int count = toIntExact(in.readUnsignedVInt());
            switch (count)
            {
                case 0: throw new IllegalStateException();
                case 1: return Collections.singletonList(serializer.deserialize(in, version));
                default:
                    List<T> result = new ArrayList<>();
                    for (int i = 0 ; i < count ; ++i)
                        result.add(serializer.deserialize(in, version));
                    return result;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static <T> List<T> deserialize(ByteBuffer[] buffers, IVersionedSerializer<T> serializer)
    {
        List<T> result = new ArrayList<>(buffers.length);
        for (ByteBuffer bytes : buffers)
            result.addAll(deserialize(bytes, serializer));
        return result;
    }

    // maybeCheckCondition? checkConditionMemoized?
    public boolean checkCondition(Data data)
    {
        // Assert data that was memoized is same as data that is provided?
        if (conditionResult != null)
            return conditionResult;
        TxnCondition condition = AccordSerializers.deserialize(this.condition, TxnCondition.serializer);
        conditionResult = condition.applies((TxnData) data);
        return conditionResult;
    }
}
