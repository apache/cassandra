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
import accord.local.RejectBefore;
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
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.NullableSerializer;

import static org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer.deserializeNullable;
import static org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer.serializeNullable;
import static org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer.serializedNullableSize;

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
            int mapSize = map.size();
            out.writeUnsignedVInt32(mapSize);

            for (int i=0; i<mapSize; i++)
            {
                KeySerializers.routingKey.serialize(map.startAt(i), out, version);
                valueSerializer.serialize(map.valueAt(i), out, version);
            }
            if (mapSize > 0)
                KeySerializers.routingKey.serialize(map.startAt(mapSize), out, version);
        }

        public R deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean inclusiveEnds = in.readBoolean();
            int mapSize = in.readUnsignedVInt32();
            RoutingKey[] keys = new RoutingKey[mapSize + 1];
            T[] values = newValueArray.apply(mapSize);
            for (int i=0; i<mapSize; i++)
            {
                keys[i] = KeySerializers.routingKey.deserialize(in, version);
                values[i] = valueSerializer.deserialize(in, version);
            }
            if (mapSize > 0)
                keys[mapSize] = KeySerializers.routingKey.deserialize(in, version);
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
            if (mapSize > 0)
                size += KeySerializers.routingKey.serializedSize(map.startAt(mapSize), version);

            return size;
        }
    }

    public static IVersionedSerializer<RejectBefore> rejectBefore = new ReducingRangeMapSerializer<>(CommandSerializers.nullableTxnId, TxnId[]::new, RejectBefore.SerializerSupport::create);
    public static IVersionedSerializer<DurableBefore> durableBefore = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(new IVersionedSerializer<>()
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

    public static final IVersionedSerializer<RedundantBefore.Bounds> redundantBeforeEntry = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(RedundantBefore.Bounds b, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.range.serialize(b.range, out, version);
            Invariants.require(b.startEpoch <= b.endEpoch);
            out.writeUnsignedVInt(b.startEpoch);
            if (b.endEpoch == Long.MAX_VALUE) out.writeUnsignedVInt(0L);
            else out.writeUnsignedVInt(1 + b.endEpoch - b.startEpoch);
            serializeNullable(b.staleUntilAtLeast, out);
            out.writeUnsignedVInt32(b.bounds.length);
            for (TxnId bound : b.bounds)
            {
                CommandSerializers.txnId.serialize(bound, out, version);
            }
            int prev = 0;
            for (int status : b.statuses)
            {
                out.writeUnsignedVInt32(status ^ prev);
                prev = status;
            }
        }

        @Override
        public RedundantBefore.Bounds deserialize(DataInputPlus in, int version) throws IOException
        {
            Range range = KeySerializers.range.deserialize(in, version);
            long startEpoch = in.readUnsignedVInt();
            long endEpoch = in.readUnsignedVInt();
            if (endEpoch == 0) endEpoch = Long.MAX_VALUE;
            else endEpoch = endEpoch - 1 + startEpoch;
            Timestamp staleUntilAtLeast = deserializeNullable(in);
            int count = in.readUnsignedVInt32();

            TxnId[] bounds = new TxnId[count];
            for (int i = 0 ; i < bounds.length ; ++i)
                bounds[i] = CommandSerializers.txnId.deserialize(in);
            int[] statuses = new int[count * 2];
            int prev = 0;
            for (int i = 0 ; i < statuses.length ; ++i)
                statuses[i] = prev = in.readUnsignedVInt32() ^ prev;

            return new RedundantBefore.Bounds(range, startEpoch, endEpoch, bounds, statuses, staleUntilAtLeast);
        }

        @Override
        public long serializedSize(RedundantBefore.Bounds b, int version)
        {
            long size = KeySerializers.range.serializedSize(b.range, version);
            size += TypeSizes.sizeofUnsignedVInt(b.startEpoch);
            size += TypeSizes.sizeofUnsignedVInt(b.endEpoch == Long.MAX_VALUE ? 0 : 1 + b.endEpoch - b.startEpoch);
            size += serializedNullableSize(b.staleUntilAtLeast);
            size += TypeSizes.sizeofUnsignedVInt(b.bounds.length);
            for (TxnId bound : b.bounds)
            {
                size += CommandSerializers.txnId.serializedSize(bound, version);
            }
            int prev = 0;
            for (int status : b.statuses)
            {
                size += TypeSizes.sizeofUnsignedVInt(status ^ prev);
                prev = status;
            }
            return size;
        }
    };
    public static IVersionedSerializer<RedundantBefore> redundantBefore = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(redundantBeforeEntry), RedundantBefore.Bounds[]::new, RedundantBefore.SerializerSupport::create);

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
