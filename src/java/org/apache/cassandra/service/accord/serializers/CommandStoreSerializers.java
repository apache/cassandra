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
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import accord.api.LocalListeners.TxnListener;
import accord.api.RoutingKey;
import accord.impl.cfr.IdEntry;
import accord.impl.cfr.IdMultiEntry;
import accord.impl.cfr.IdSingleEntry;
import accord.impl.progresslog.TxnState;
import accord.local.DurableBefore;
import accord.local.MaxConflicts;
import accord.local.MaxDecidedRX;
import accord.local.RedundantBefore;
import accord.local.RejectBefore;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.SaveStatus;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.BTreeReducingIntervalMap;
import accord.utils.BTreeReducingIntervalMap.AbstractBoundariesBuilder;
import accord.utils.BTreeReducingRangeMap;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;

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
    public static final UnversionedSerializer<DurableBefore.Entry> durableBeforeEntry = new DurableBeforeEntrySerializer();
    public static final UnversionedSerializer<DurableBefore> durableBefore = new ReducingRangeMapSerializer<>(NullableSerializer.wrap(durableBeforeEntry), DurableBefore.Entry[]::new, DurableBefore.SerializerSupport::create, DurableBefore.EMPTY);
    public static final UnversionedSerializer<MaxConflicts> maxConflicts = new MaxConflictsSerializer();
    public static final UnversionedSerializer<MaxDecidedRX> maxDecidedRX = new ReducingRangeMapSerializer<>(new DecidedRXSerializer(), MaxDecidedRX.DecidedRX[]::new, MaxDecidedRX.SerializerSupport::create, MaxDecidedRX.EMPTY);
    public static final UnversionedSerializer<RedundantBefore.Bounds> redundantBeforeShortBounds = new RedundantBeforeShortBoundsSerializer();
    public static final UnversionedSerializer<RedundantBefore> redundantBefore = new ReducingRangeMapSerializer<>(redundantBeforeShortBounds, RedundantBefore.Bounds[]::new, RedundantBefore.SerializerSupport::create, RedundantBefore.EMPTY);
    public static final UnversionedSerializer<RejectBefore> rejectBefore = new ReducingRangeMapSerializer<>(CommandSerializers.timestamp, Timestamp[]::new, RejectBefore.SerializerSupport::create, RejectBefore.EMPTY);
    public static final UnversionedSerializer<NavigableMap<TxnId, Ranges>> bootstrapBeganAt = new TimestampToRangesMapSerializer<>(CommandSerializers.txnId);
    public static final UnversionedSerializer<NavigableMap<Timestamp, Ranges>> safeToRead = new TimestampToRangesMapSerializer<>(CommandSerializers.timestamp);
    public static final UnversionedSerializer<TxnListener> txnListener = new TxnListenerSerializer();
    public static final UnversionedSerializer<TxnState> progressLogState = new ProgressLogStateSerializer();
    public static final UnversionedSerializer<IdEntry> rangeIndexIdEntry = new RangeIndexIdEntrySerializer();

    private CommandStoreSerializers() {}

    // TODO (expected): use flags to switch to bitset encoding for nulls
    private static abstract class AbstractReducingRangeMapSerializer<V, Map extends ReducingRangeMap<V>> implements UnversionedSerializer<Map>
    {
        // note: originally we redundantly encoded a value of 1 because we were encoding inclusiveEnds as a boolean
        private static final int RESERVED_FLAG_BITS = 3;
        final IntFunction<V[]> newValueArray;
        final BiFunction<RoutingKey[], V[], Map> constructor;
        final Map empty;

        public AbstractReducingRangeMapSerializer(IntFunction<V[]> newValueArray, BiFunction<RoutingKey[], V[], Map> constructor, Map empty)
        {
            this.newValueArray = newValueArray;
            this.constructor = constructor;
            this.empty = empty;
        }

        protected abstract int flags(Map map);
        protected abstract UnversionedSerializer<V> valueSerializer(int flags);

        private int safeFlags(Map map)
        {
            int flags = flags(map);
            Invariants.require((flags & ((1 << RESERVED_FLAG_BITS) - 1)) == 0);
            // encoded flags supersede writeBoolean(true), so we default to setting the lowest bit, so we can interpret 0 as a flag bit
            return flags | 1;
        }

        @Override
        public void serialize(Map map, DataOutputPlus out) throws IOException
        {
            int flags = safeFlags(map);
            int mapSize = map.size();
            out.writeUnsignedVInt32(flags);
            out.writeUnsignedVInt32(mapSize);

            if (mapSize == 0)
                return;

            UnversionedSerializer<V> valueSerializer = valueSerializer(flags);

            for (int i=0; i<mapSize; i++)
            {
                KeySerializers.routingKey.serialize(map.startAt(i), out);
                valueSerializer.serialize(map.valueAt(i), out);
            }
            KeySerializers.routingKey.serialize(map.startAt(mapSize), out);
        }

        @Override
        public Map deserialize(DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            int mapSize = in.readUnsignedVInt32();

            if (mapSize == 0)
                return empty;

            RoutingKey[] keys = new RoutingKey[mapSize + 1];
            V[] values = newValueArray.apply(mapSize);
            UnversionedSerializer<V> valueSerializer = valueSerializer(flags);
            for (int i=0; i<mapSize; i++)
            {
                keys[i] = KeySerializers.routingKey.deserialize(in);
                values[i] = valueSerializer.deserialize(in);
            }
            keys[mapSize] = KeySerializers.routingKey.deserialize(in);

            return constructor.apply(keys, values);
        }

        @Override
        public long serializedSize(Map map)
        {
            int flags = safeFlags(map);
            int mapSize = map.size();

            long size = 0;
            size += TypeSizes.sizeofUnsignedVInt(flags);
            size += TypeSizes.sizeofUnsignedVInt(mapSize);

            if (mapSize == 0)
                return size;

            UnversionedSerializer<V> valueSerializer = valueSerializer(flags);
            for (int i=0; i<mapSize; i++)
            {
                size += KeySerializers.routingKey.serializedSize(map.startAt(i));
                size += valueSerializer.serializedSize(map.valueAt(i));
            }
            size += KeySerializers.routingKey.serializedSize(map.startAt(mapSize));
            return size;
        }
    }

    private static class ReducingRangeMapSerializer<T, Map extends ReducingRangeMap<T>> extends AbstractReducingRangeMapSerializer<T, Map> implements UnversionedSerializer<Map>
    {
        final UnversionedSerializer<T> defaultValueSerializer;

        public ReducingRangeMapSerializer(UnversionedSerializer<T> defaultValueSerializer, IntFunction<T[]> newValueArray, BiFunction<RoutingKey[], T[], Map> constructor, Map empty)
        {
            super(newValueArray, constructor, empty);
            this.defaultValueSerializer = defaultValueSerializer;
        }

        @Override
        protected int flags(Map map)
        {
            return 0;
        }

        @Override
        protected UnversionedSerializer<T> valueSerializer(int flags)
        {
            return defaultValueSerializer;
        }
    }

    private static final class DurableBeforeEntrySerializer implements UnversionedSerializer<DurableBefore.Entry>
    {
        private DurableBeforeEntrySerializer() {}

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
            return CommandSerializers.txnId.serializedSize(t.quorumBefore)
                   + CommandSerializers.txnId.serializedSize(t.universalBefore);
        }
    }

    private static final class DecidedRXSerializer implements UnversionedSerializer<MaxDecidedRX.DecidedRX>
    {
        private DecidedRXSerializer() {}

        @Override
        public void serialize(MaxDecidedRX.DecidedRX t, DataOutputPlus out) throws IOException
        {
            if (t == null)
            {
                CommandSerializers.txnId.serialize(null, out);
            }
            else
            {
                CommandSerializers.txnId.serialize(t.any, out);
                CommandSerializers.txnId.serialize(t.hlcBound, out);
            }
        }

        @Override
        public MaxDecidedRX.DecidedRX deserialize(DataInputPlus in) throws IOException
        {
            TxnId any = CommandSerializers.txnId.deserialize(in);
            if (any == null)
                return null;
            TxnId hlcBound = CommandSerializers.txnId.deserialize(in);
            return new MaxDecidedRX.DecidedRX(any, hlcBound);
        }

        @Override
        public long serializedSize(MaxDecidedRX.DecidedRX t)
        {
            if (t == null)
                return CommandSerializers.txnId.serializedSize(null);

            return CommandSerializers.txnId.serializedSize(t.any)
                   + CommandSerializers.txnId.serializedSize(t.hlcBound);
        }
    }

    private static class RedundantBeforeShortBoundsSerializer implements UnversionedSerializer<RedundantBefore.Bounds>
    {
        private RedundantBeforeShortBoundsSerializer() {}

        @Override
        public void serialize(RedundantBefore.Bounds b, DataOutputPlus out) throws IOException
        {
            // was previously wrapped in NullableSerializer; inlined logic here so we can convert to flags in future and save bytes
            if (b == null)
            {
                out.writeByte(0);
                return;
            }
            out.writeByte(1);

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
                out.writeShort(cast(b.status(i * 2)));
                out.writeShort(cast(b.status(i * 2 + 1)));
            }
        }

        private short cast(long v)
        {
            if ((v & ~0xFFFF) != 0)
                throw new IllegalStateException("Cannot serialize RedundantStatus larger than 0xFFFF. Requires serialization changes.");
            return (short)v;
        }

        @Override
        public RedundantBefore.Bounds deserialize(DataInputPlus in) throws IOException
        {
            if (in.readByte() == 0)
                return null;

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
            int[] statuses = new int[count * 2];
            for (int i = 0 ; i < statuses.length ; ++i)
                statuses[i] = in.readShort();

            return new RedundantBefore.Bounds(range, startEpoch, endEpoch, bounds, statuses, staleUntilAtLeast);
        }

        @Override
        public long serializedSize(RedundantBefore.Bounds b)
        {
            if (b == null)
                return 1;

            long size = 1 + KeySerializers.range.serializedSize(b.range);
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
    }

    private static class TimestampToRangesMapSerializer<T extends Timestamp> implements UnversionedSerializer<NavigableMap<T, Ranges>>
    {
        private final UnversionedSerializer<T> timestampSerializer;

        public TimestampToRangesMapSerializer(UnversionedSerializer<T> timestampSerializer)
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

    private static class BTreeReducingRangeMapSerializer<V, Map extends BTreeReducingRangeMap<V>> implements UnversionedSerializer<Map>
    {
        final UnversionedSerializer<V> valueSerializer;
        final Map empty;
        final IntFunction<AbstractBoundariesBuilder<RoutingKey, V, Map>> builderFactory;

        public BTreeReducingRangeMapSerializer(UnversionedSerializer<V> valueSerializer,
                                               Map empty,
                                               IntFunction<AbstractBoundariesBuilder<RoutingKey, V, Map>> builderFactory)
        {
            this.valueSerializer = valueSerializer;
            this.empty = empty;
            this.builderFactory = builderFactory;
        }

        @Override
        public void serialize(Map map, DataOutputPlus out) throws IOException
        {
            int flags = 0;
            int mapSize = map.size();
            out.writeUnsignedVInt32(flags);
            out.writeUnsignedVInt32(mapSize);

            if (mapSize == 0)
                return;

            BTreeReducingIntervalMap.WithBoundsIterator<RoutingKey, V> iter = map.withBoundsIterator(false);
            RoutingKey end = null;
            while (iter.advance())
            {
                KeySerializers.routingKey.serialize(iter.start(), out);
                valueSerializer.serialize(iter.value(), out);
                end = iter.end();
            }
            KeySerializers.routingKey.serialize(end, out);
        }

        @Override
        public Map deserialize(DataInputPlus in) throws IOException
        {
            int flags = in.readUnsignedVInt32();
            Invariants.expect(flags == 0);
            int mapSize = in.readUnsignedVInt32();

            if (mapSize == 0)
                return empty;

            AbstractBoundariesBuilder<RoutingKey, V, Map> builder = builderFactory.apply(mapSize);
            while (mapSize-- > 0)
            {
                RoutingKey key = KeySerializers.routingKey.deserialize(in);
                V value = valueSerializer.deserialize(in);
                builder.append(key, value, (a, b) -> { throw new IllegalStateException(); });
            }
            RoutingKey key = KeySerializers.routingKey.deserialize(in);
            builder.append(key, null, (a, b) -> { throw new IllegalStateException(); });

            return builder.build();
        }

        @Override
        public long serializedSize(Map map)
        {
            int flags = 0;
            int mapSize = map.size();
            long size = TypeSizes.sizeofUnsignedVInt(flags);
            size += TypeSizes.sizeofUnsignedVInt(mapSize);

            if (mapSize == 0)
                return size;

            BTreeReducingIntervalMap.WithBoundsIterator<RoutingKey, V> iter = map.withBoundsIterator(false);
            RoutingKey end = null;
            while (iter.advance())
            {
                size += KeySerializers.routingKey.serializedSize(iter.start());
                size += valueSerializer.serializedSize(iter.value());
                end = iter.end();
            }
            size += KeySerializers.routingKey.serializedSize(end);

            return size;
        }
    }

    private static final class MaxConflictsSerializer extends BTreeReducingRangeMapSerializer<Timestamp, MaxConflicts>
    {
        private MaxConflictsSerializer()
        {
            super(CommandSerializers.timestamp, MaxConflicts.EMPTY, MaxConflicts.Builder::new);
        }
    }

    private static final class TxnListenerSerializer implements UnversionedSerializer<TxnListener>
    {
        private TxnListenerSerializer() {}

        @Override
        public void serialize(TxnListener t, DataOutputPlus out) throws IOException
        {
            if (t == null)
            {
                CommandSerializers.txnId.serialize(null, out);
            }
            else
            {
                CommandSerializers.txnId.serialize(t.waiter, out);
                CommandSerializers.txnId.serialize(t.waitingOn, out);
                CommandSerializers.saveStatus.serialize(t.awaitingStatus, out);
            }
        }

        @Override
        public TxnListener deserialize(DataInputPlus in) throws IOException
        {
            TxnId waiter = CommandSerializers.txnId.deserialize(in);
            if (waiter == null)
                return null;
            TxnId waitingOn = CommandSerializers.txnId.deserialize(in);
            SaveStatus awaitingStatus = CommandSerializers.saveStatus.deserialize(in);
            return new TxnListener(waiter, waitingOn, awaitingStatus);
        }

        @Override
        public long serializedSize(TxnListener t)
        {
            if (t == null)
                return CommandSerializers.txnId.serializedSize(null);

            return CommandSerializers.txnId.serializedSize(t.waiter)
                   + CommandSerializers.txnId.serializedSize(t.waitingOn)
                   + CommandSerializers.saveStatus.serializedSize(t.awaitingStatus);
        }
    }

    private static final class ProgressLogStateSerializer implements UnversionedSerializer<TxnState>
    {
        private ProgressLogStateSerializer() {}

        @Override
        public void serialize(TxnState t, DataOutputPlus out) throws IOException
        {
            if (t == null)
            {
                CommandSerializers.txnId.serialize(null, out);
            }
            else
            {
                CommandSerializers.txnId.serialize(t.txnId, out);
                out.writeLong(t.encodedState());
            }
        }

        @Override
        public TxnState deserialize(DataInputPlus in) throws IOException
        {
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            if (txnId == null)
                return null;
            long encodedState = in.readLong();
            return TxnState.SerializationSupport.create(txnId, encodedState);
        }

        @Override
        public long serializedSize(TxnState t)
        {
            if (t == null)
                return CommandSerializers.txnId.serializedSize(null);

            return CommandSerializers.txnId.serializedSize(t.txnId) + TypeSizes.LONG_SIZE;
        }
    }

    private static final class RangeIndexIdEntrySerializer implements UnversionedSerializer<IdEntry>
    {
        private RangeIndexIdEntrySerializer() {}

        @Override
        public void serialize(IdEntry t, DataOutputPlus out) throws IOException
        {
            byte flags = (byte) ((t.getClass() == IdSingleEntry.class) ? 0 : 1);
            out.writeByte(flags);
            CommandSerializers.txnId.serialize(t, out);
            if (flags == 0)
            {
                IdSingleEntry e = (IdSingleEntry) t;
                KeySerializers.range.serialize(e.range);
            }
            else
            {
                IdMultiEntry e = (IdMultiEntry) t;
                KeySerializers.ranges.serialize(e.ranges);
            }
        }

        @Override
        public IdEntry deserialize(DataInputPlus in) throws IOException
        {
            byte flags = in.readByte();
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            if (flags == 0)
            {
                Range range = KeySerializers.range.deserialize(in);
                return new IdSingleEntry(txnId, range);
            }
            else
            {
                Ranges ranges = KeySerializers.ranges.deserialize(in);
                return new IdMultiEntry(txnId, ranges);
            }
        }

        @Override
        public long serializedSize(IdEntry t)
        {
            return 1 + CommandSerializers.txnId.serializedSize(t)
                   + (t.getClass() == IdSingleEntry.class ? KeySerializers.range.serializedSize(((IdSingleEntry)t).range)
                                                          : KeySerializers.ranges.serializedSize(((IdMultiEntry)t).ranges));
        }
    }

}
