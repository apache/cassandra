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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.function.IntFunction;

import accord.api.RoutingKey;
import accord.local.DurableBefore;
import accord.local.RedundantBefore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.TriFunction;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.NullableSerializer;

public class CommandStoreSerializers
{
    private CommandStoreSerializers() {}

    public static class ReducingRangeMapSerializer<T, R extends ReducingRangeMap<T>> implements IVersionedSerializer<R>
    {
        final IVersionedSerializer<T> valueSerializer;
        final IntFunction<T[]> newValueArray;
        final TriFunction<Boolean, RoutingKey[], T[], R> constructor;

        public ReducingRangeMapSerializer(IVersionedSerializer<T> valueSerializer, IntFunction<T[]> newValueArray, TriFunction<Boolean, RoutingKey[], T[], R> constructor)
        {
            this.valueSerializer = valueSerializer;
            this.newValueArray = newValueArray;
            this.constructor = constructor;
        }

        public void serialize(R map, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(map.inclusiveEnds());
            int size = map.size();
            out.writeUnsignedVInt32(size);

            for (int i=0; i<size; i++)
            {
                KeySerializers.routingKey.serialize(map.startAt(i), out, version);
                valueSerializer.serialize(map.valueAt(i), out, version);
            }
            KeySerializers.routingKey.serialize(map.startAt(size), out, version);
        }

        public R deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean inclusiveEnds = in.readBoolean();
            int size = in.readUnsignedVInt32();
            RoutingKey[] keys = new RoutingKey[size + 1];
            T[] values = newValueArray.apply(size);
            for (int i=0; i<size; i++)
            {
                keys[i] = KeySerializers.routingKey.deserialize(in, version);
                values[i] = valueSerializer.deserialize(in, version);
            }
            keys[size] = KeySerializers.routingKey.deserialize(in, version);
            return constructor.apply(inclusiveEnds, keys, values);
        }

        public long serializedSize(R map, int version)
        {
            long size = TypeSizes.BOOL_SIZE;
            int mapSize = map.size();
            size += TypeSizes.sizeofUnsignedVInt(mapSize);
            for (int i=0; i<mapSize; i++)
            {
                size += KeySerializers.routingKey.serializedSize(map.startAt(i), version);
                size += valueSerializer.serializedSize(map.valueAt(i), version);
            }
            size += KeySerializers.routingKey.serializedSize(map.startAt(mapSize), version);

            return size;
        }
    }

    public static IVersionedSerializer<ReducingRangeMap<Timestamp>> rejectBefore = new ReducingRangeMapSerializer<>(CommandSerializers.nullableTimestamp, Timestamp[]::new, ReducingRangeMap.SerializerSupport::create);
    public static IVersionedSerializer<DurableBefore> durableBefore = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(new IVersionedSerializer<DurableBefore.Entry>()
    {
        @Override
        public void serialize(DurableBefore.Entry t, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.txnId.serialize(t.majorityBefore, out, version);
            CommandSerializers.txnId.serialize(t.universalBefore, out, version);
        }

        @Override
        public DurableBefore.Entry deserialize(DataInputPlus in, int version) throws IOException
        {
            TxnId majorityBefore = CommandSerializers.txnId.deserialize(in, version);
            TxnId universalBefore = CommandSerializers.txnId.deserialize(in, version);
            return new DurableBefore.Entry(majorityBefore, universalBefore);
        }

        @Override
        public long serializedSize(DurableBefore.Entry t, int version)
        {
            return   CommandSerializers.txnId.serializedSize(t.majorityBefore, version)
                   + CommandSerializers.txnId.serializedSize(t.universalBefore, version);
        }
    }), DurableBefore.Entry[]::new, DurableBefore.SerializerSupport::create);

    public static final IVersionedSerializer<RedundantBefore.Entry> redundantBeforeEntry = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(RedundantBefore.Entry t, DataOutputPlus out, int version) throws IOException
        {
            TokenRange.serializer.serialize((TokenRange) t.range, out, version);
            Invariants.checkState(t.startEpoch <= t.endEpoch);
            out.writeUnsignedVInt(t.startEpoch);
            if (t.endEpoch == Long.MAX_VALUE) out.writeUnsignedVInt(0L);
            else out.writeUnsignedVInt(1 + t.endEpoch - t.startEpoch);
            CommandSerializers.txnId.serialize(t.locallyAppliedOrInvalidatedBefore, out, version);
            CommandSerializers.txnId.serialize(t.shardAppliedOrInvalidatedBefore, out, version);
            CommandSerializers.txnId.serialize(t.bootstrappedAt, out, version);
            CommandSerializers.nullableTimestamp.serialize(t.staleUntilAtLeast, out, version);
        }

        @Override
        public RedundantBefore.Entry deserialize(DataInputPlus in, int version) throws IOException
        {
            Range range = TokenRange.serializer.deserialize(in, version);
            long startEpoch = in.readUnsignedVInt();
            long endEpoch = in.readUnsignedVInt();
            if (endEpoch == 0) endEpoch = Long.MAX_VALUE;
            else endEpoch = endEpoch - 1 + startEpoch;
            TxnId locallyAppliedOrInvalidatedBefore = CommandSerializers.txnId.deserialize(in, version);
            TxnId shardAppliedOrInvalidatedBefore = CommandSerializers.txnId.deserialize(in, version);
            TxnId bootstrappedAt = CommandSerializers.txnId.deserialize(in, version);
            Timestamp staleUntilAtLeast = CommandSerializers.nullableTimestamp.deserialize(in, version);
            return new RedundantBefore.Entry(range, startEpoch, endEpoch, locallyAppliedOrInvalidatedBefore, shardAppliedOrInvalidatedBefore, bootstrappedAt, staleUntilAtLeast);
        }

        @Override
        public long serializedSize(RedundantBefore.Entry t, int version)
        {
            long size = TokenRange.serializer.serializedSize((TokenRange) t.range, version);
            size += TypeSizes.sizeofUnsignedVInt(t.startEpoch);
            size += TypeSizes.sizeofUnsignedVInt(t.endEpoch == Long.MAX_VALUE ? 0 : 1 + t.endEpoch - t.startEpoch);
            size += CommandSerializers.txnId.serializedSize(t.locallyAppliedOrInvalidatedBefore, version);
            size += CommandSerializers.txnId.serializedSize(t.shardAppliedOrInvalidatedBefore, version);
            size += CommandSerializers.txnId.serializedSize(t.bootstrappedAt, version);
            size += CommandSerializers.nullableTimestamp.serializedSize(t.staleUntilAtLeast, version);
            return size;
        }
    };
    public static IVersionedSerializer<RedundantBefore> redundantBefore = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(redundantBeforeEntry), RedundantBefore.Entry[]::new, RedundantBefore.SerializerSupport::create);

    private static class TimestampToRangesSerializer<T extends Timestamp> implements IVersionedSerializer<NavigableMap<T, Ranges>>
    {
        private final IVersionedSerializer<T> timestampSerializer;

        public TimestampToRangesSerializer(IVersionedSerializer<T> timestampSerializer)
        {
            this.timestampSerializer = timestampSerializer;
        }

        public void serialize(NavigableMap<T, Ranges> map, DataOutputPlus out, int version) throws IOException
        {
            CollectionSerializers.serializeMap(map, out, version, timestampSerializer, KeySerializers.ranges);
        }

        public NavigableMap<T, Ranges> deserialize(DataInputPlus in, int version) throws IOException
        {
            return CollectionSerializers.deserializeMap(in, version, timestampSerializer, KeySerializers.ranges, i -> new TreeMap<>());

        }

        public long serializedSize(NavigableMap<T, Ranges> map, int version)
        {
            return CollectionSerializers.serializedMapSize(map, version, timestampSerializer, KeySerializers.ranges);
        }
    }

    public static final IVersionedSerializer<NavigableMap<TxnId, Ranges>> bootstrapBeganAt = new TimestampToRangesSerializer<>(CommandSerializers.txnId);
    public static final IVersionedSerializer<NavigableMap<Timestamp, Ranges>> safeToRead = new TimestampToRangesSerializer<>(CommandSerializers.timestamp);
}
