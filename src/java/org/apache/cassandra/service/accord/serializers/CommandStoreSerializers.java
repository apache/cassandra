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
import org.apache.cassandra.io.UnversionedSerializer;
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

    public static class ReducingRangeMapSerializer<T, R extends ReducingRangeMap<T>> implements UnversionedSerializer<R>
    {
        final UnversionedSerializer<T> valueSerializer;
        final IntFunction<T[]> newValueArray;
        final TriFunction<Boolean, RoutingKey[], T[], R> constructor;

        public ReducingRangeMapSerializer(UnversionedSerializer<T> valueSerializer, IntFunction<T[]> newValueArray, TriFunction<Boolean, RoutingKey[], T[], R> constructor)
        {
            this.valueSerializer = valueSerializer;
            this.newValueArray = newValueArray;
            this.constructor = constructor;
        }

        @Override
        public void serialize(R map, DataOutputPlus out) throws IOException
        {
            out.writeBoolean(map.inclusiveEnds());
            int mapSize = map.size();
            out.writeUnsignedVInt32(mapSize);

            for (int i=0; i<mapSize; i++)
            {
                KeySerializers.routingKey.serialize(map.startAt(i), out);
                valueSerializer.serialize(map.valueAt(i), out);
            }
            if (mapSize > 0)
                KeySerializers.routingKey.serialize(map.startAt(mapSize), out);
        }

        @Override
        public R deserialize(DataInputPlus in) throws IOException
        {
            boolean inclusiveEnds = in.readBoolean();
            int mapSize = in.readUnsignedVInt32();
            RoutingKey[] keys = new RoutingKey[mapSize + 1];
            T[] values = newValueArray.apply(mapSize);
            for (int i=0; i<mapSize; i++)
            {
                keys[i] = KeySerializers.routingKey.deserialize(in);
                values[i] = valueSerializer.deserialize(in);
            }
            if (mapSize > 0)
                keys[mapSize] = KeySerializers.routingKey.deserialize(in);
            return constructor.apply(inclusiveEnds, keys, values);
        }

        @Override
        public long serializedSize(R map)
        {
            long size = TypeSizes.BOOL_SIZE;
            int mapSize = map.size();
            size += TypeSizes.sizeofUnsignedVInt(mapSize);
            for (int i=0; i<mapSize; i++)
            {
                size += KeySerializers.routingKey.serializedSize(map.startAt(i));
                size += valueSerializer.serializedSize(map.valueAt(i));
            }
            if (mapSize > 0)
                size += KeySerializers.routingKey.serializedSize(map.startAt(mapSize));

            return size;
        }
    }

    public static UnversionedSerializer<DurableBefore> durableBefore = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(new UnversionedSerializer<>()
    {
        @Override
        public void serialize(DurableBefore.Entry t, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(t.quorumBefore, out);
            CommandSerializers.txnId.serialize(t.universalBefore, out);
        }

        @Override
        public DurableBefore.Entry deserialize(DataInputPlus in) throws IOException
        {
            TxnId quorumBefore = CommandSerializers.txnId.deserialize(in);
            TxnId universalBefore = CommandSerializers.txnId.deserialize(in);
            return new DurableBefore.Entry(quorumBefore, universalBefore);
        }

        @Override
        public long serializedSize(DurableBefore.Entry t)
        {
            return   CommandSerializers.txnId.serializedSize(t.quorumBefore)
                   + CommandSerializers.txnId.serializedSize(t.universalBefore);
        }
    }), DurableBefore.Entry[]::new, DurableBefore.SerializerSupport::create);

    public static final UnversionedSerializer<RedundantBefore.Bounds> redundantBeforeEntry = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(RedundantBefore.Bounds b, DataOutputPlus out) throws IOException
        {
            KeySerializers.range.serialize(b.range, out);
            Invariants.require(b.startEpoch <= b.endEpoch);
            out.writeUnsignedVInt(b.startEpoch);
            if (b.endEpoch == Long.MAX_VALUE) out.writeUnsignedVInt(0L);
            else out.writeUnsignedVInt(1 + b.endEpoch - b.startEpoch);
            serializeNullable(b.staleUntilAtLeast, out);
            out.writeUnsignedVInt32(b.bounds.length);
            for (TxnId bound : b.bounds)
            {
                CommandSerializers.txnId.serialize(bound, out);
            }
            for (int i = 0 ; i < b.bounds.length ; ++i)
            {
                out.writeShort(b.status(i * 2));
                out.writeShort(b.status(i * 2 + 1));
            }
        }

        @Override
        public RedundantBefore.Bounds deserialize(DataInputPlus in) throws IOException
        {
            Range range = KeySerializers.range.deserialize(in);
            long startEpoch = in.readUnsignedVInt();
            long endEpoch = in.readUnsignedVInt();
            if (endEpoch == 0) endEpoch = Long.MAX_VALUE;
            else endEpoch = endEpoch - 1 + startEpoch;
            Timestamp staleUntilAtLeast = deserializeNullable(in);
            int count = in.readUnsignedVInt32();

            TxnId[] bounds = new TxnId[count];
            for (int i = 0 ; i < bounds.length ; ++i)
                bounds[i] = CommandSerializers.txnId.deserialize(in);
            short[] statuses = new short[count * 2];
            for (int i = 0 ; i < statuses.length ; ++i)
                statuses[i] = in.readShort();

            return new RedundantBefore.Bounds(range, startEpoch, endEpoch, bounds, statuses, staleUntilAtLeast);
        }

        @Override
        public long serializedSize(RedundantBefore.Bounds b)
        {
            long size = KeySerializers.range.serializedSize(b.range);
            size += TypeSizes.sizeofUnsignedVInt(b.startEpoch);
            size += TypeSizes.sizeofUnsignedVInt(b.endEpoch == Long.MAX_VALUE ? 0 : 1 + b.endEpoch - b.startEpoch);
            size += serializedNullableSize(b.staleUntilAtLeast);
            size += TypeSizes.sizeofUnsignedVInt(b.bounds.length);
            for (TxnId bound : b.bounds)
            {
                size += CommandSerializers.txnId.serializedSize(bound);
            }
            size += 2L * 2 * b.bounds.length;
            return size;
        }
    };
    public static UnversionedSerializer<RedundantBefore> redundantBefore = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(redundantBeforeEntry), RedundantBefore.Bounds[]::new, RedundantBefore.SerializerSupport::create);

    private static class TimestampToRangesSerializer<T extends Timestamp> implements UnversionedSerializer<NavigableMap<T, Ranges>>
    {
        private final UnversionedSerializer<T> timestampSerializer;

        public TimestampToRangesSerializer(UnversionedSerializer<T> timestampSerializer)
        {
            this.timestampSerializer = timestampSerializer;
        }

        @Override
        public void serialize(NavigableMap<T, Ranges> map, DataOutputPlus out) throws IOException
        {
            CollectionSerializers.serializeMap(map, out, timestampSerializer, KeySerializers.ranges);
        }

        @Override
        public NavigableMap<T, Ranges> deserialize(DataInputPlus in) throws IOException
        {
            return CollectionSerializers.deserializeMap(in, timestampSerializer, KeySerializers.ranges, i -> new TreeMap<>());

        }

        @Override
        public long serializedSize(NavigableMap<T, Ranges> map)
        {
            return CollectionSerializers.serializedMapSize(map, timestampSerializer, KeySerializers.ranges);
        }
    }

    public static final UnversionedSerializer<NavigableMap<TxnId, Ranges>> bootstrapBeganAt = new TimestampToRangesSerializer<>(CommandSerializers.txnId);
    public static final UnversionedSerializer<NavigableMap<Timestamp, Ranges>> safeToRead = new TimestampToRangesSerializer<>(CommandSerializers.timestamp);
}
