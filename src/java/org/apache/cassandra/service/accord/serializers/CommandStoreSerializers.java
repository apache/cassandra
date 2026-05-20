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
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.IntFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.LocalListeners.TxnListener;
import accord.api.RoutingKey;
import accord.impl.cfr.IdEntry;
import accord.impl.cfr.IdMultiEntry;
import accord.impl.cfr.IdSingleEntry;
import accord.impl.progresslog.TxnState;
import accord.local.CommandStores;
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
import accord.utils.BTreeReducingRangeMap;
import accord.utils.Invariants;
import accord.utils.ReducingRangeMap;
import accord.utils.VIntCoding;
import accord.utils.btree.ReducingBTree;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;
import org.apache.cassandra.utils.NoSpamLogger;

import static accord.utils.Invariants.illegalState;
import static org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer.deserializeNullable;
import static org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer.serializeNullable;
import static org.apache.cassandra.service.accord.serializers.CommandSerializers.ExecuteAtSerializer.serializedNullableSize;

public class CommandStoreSerializers
{
    private static final Logger logger = LoggerFactory.getLogger(CommandStoreSerializers.class);
    private static final NoSpamLogger noSpamLogger = NoSpamLogger.getLogger(logger, 1, TimeUnit.MINUTES);
    private static final int REDUCING_BTREE_MODE = 0;
    private static final int REDUCING_ARRAY_MODE = 1;
    private static final int REDUCING_MODE_BIT = 1;
    private static final int REDUCING_RESERVED_FLAG_BITS = 3;

    public static final UnversionedSerializer<DurableBefore> durableBefore = new DurableBeforeSerializer();
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
    public static final UnversionedSerializer<CommandStores.RangesForEpoch> rangesForEpoch = new RangesForEpochSerializer();

    private CommandStoreSerializers() {}

    // TODO (expected): use flags to switch to bitset encoding for nulls
    private static abstract class AbstractReducingRangeMapSerializer<V, Map extends ReducingRangeMap<V>> implements UnversionedSerializer<Map>
    {
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
            Invariants.require((flags & ((1 << REDUCING_RESERVED_FLAG_BITS) - 1)) == 0);
            return flags | REDUCING_ARRAY_MODE;
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

    static class ReducingRangeMapSerializer<T, Map extends ReducingRangeMap<T>> extends AbstractReducingRangeMapSerializer<T, Map> implements UnversionedSerializer<Map>
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

    private static abstract class BTreeReducingRangeMapSerializer<E extends ReducingBTree.Entry<E>, Map extends BTreeReducingRangeMap<E>> implements UnversionedSerializer<Map>
    {
        private static final int RESERVED_MAP_MASK = 0x3;

        private static final int DISCONTIGUOUS = 1;
        private static final int NEW_PREFIX = 2;

        public BTreeReducingRangeMapSerializer()
        {
        }

        abstract Map empty();
        abstract BTreeReducingRangeMap.Builder<E, Map> builder();
        abstract void serializeWithoutRange(E e, DataOutputPlus out) throws IOException;
        abstract long serializedSizeWithoutRange(E e);
        abstract E deserialize(RoutingKey start, RoutingKey end, DataInputPlus in, int mapFlags) throws IOException;
        abstract E deserializeArrayModeWithoutRange(DataInputPlus in) throws IOException;

        protected int mapFlags() { return 0; }

        @Override
        public void serialize(Map map, DataOutputPlus out) throws IOException
        {
            // for upgrading non-tree structures
            int mapFlags = mapFlags();
            Invariants.require((mapFlags & RESERVED_MAP_MASK) == 0);
            mapFlags |= REDUCING_BTREE_MODE;
            int mapSize = map.size();
            out.writeUnsignedVInt32(mapFlags);
            out.writeUnsignedVInt32(mapSize);

            if (mapSize == 0)
                return;

            E prev = null;
            int fixedLength = 0;
            for (E e : map)
            {
                int flags = 0;
                if (prev == null)
                {
                    flags = NEW_PREFIX | DISCONTIGUOUS;
                }
                else
                {
                    int c = prev.end().compareTo(e.start());
                    if (c > 0)
                        throw illegalState("Not well-formed: %s overlaps %s in %s", prev, e, map);

                    if (c < 0)
                    {
                        flags = DISCONTIGUOUS;
                        if (!prev.prefix().equals(e.prefix()))
                            flags |= NEW_PREFIX;
                    }
                    out.writeByte(flags);
                }

                if ((flags & DISCONTIGUOUS) != 0)
                {
                    if ((flags & NEW_PREFIX) != 0)
                    {
                        KeySerializers.routingKey.serializePrefix(e.prefix(), out);
                        fixedLength = KeySerializers.routingKey.fixedKeyLengthForPrefix(e.prefix());
                    }
                    if (fixedLength < 0)
                        out.writeUnsignedVInt32(KeySerializers.routingKey.serializedSizeWithoutPrefixOrLength(e.start()));
                    KeySerializers.routingKey.serializeWithoutPrefixOrLength(e.start(), out);
                }
                if (fixedLength < 0)
                    out.writeUnsignedVInt32(KeySerializers.routingKey.serializedSizeWithoutPrefixOrLength(e.end()));
                KeySerializers.routingKey.serializeWithoutPrefixOrLength(e.end(), out);
                serializeWithoutRange(e, out);
                prev = e;
            }
        }

        @Override
        public Map deserialize(DataInputPlus in) throws IOException
        {
            int mapFlags = in.readUnsignedVInt32();
            int mapSize = in.readUnsignedVInt32();

            if (mapSize == 0)
                return empty();

            try (BTreeReducingRangeMap.Builder<E, Map> builder = builder())
            {
                if ((mapFlags & REDUCING_MODE_BIT) == REDUCING_BTREE_MODE)
                {
                    Object prefix = null;
                    RoutingKey prevEnd = null;
                    E prev = null;
                    int fixedLength = 0;
                    while (mapSize-- > 0)
                    {
                        int flags;
                        if (prefix == null) flags = NEW_PREFIX | DISCONTIGUOUS;
                        else flags = in.readByte();

                        RoutingKey start;
                        if ((flags & DISCONTIGUOUS) == 0)
                        {
                            start = prevEnd;
                        }
                        else
                        {
                            if ((flags & NEW_PREFIX) != 0)
                            {
                                prefix = KeySerializers.routingKey.deserializePrefix(in);
                                fixedLength = KeySerializers.routingKey.fixedKeyLengthForPrefix(in);
                            }
                            int length = fixedLength >= 0 ? fixedLength : in.readUnsignedVInt32();
                            start = KeySerializers.routingKey.deserializeWithPrefix(prefix, length, in);
                        }

                        int length = fixedLength >= 0 ? fixedLength : in.readUnsignedVInt32();
                        RoutingKey end = KeySerializers.routingKey.deserializeWithPrefix(prefix, length, in);
                        E cur = deserialize(start, end, in, mapFlags);
                        if ((flags & DISCONTIGUOUS) != 0)
                        {
                            if (prev != null && prev.end().compareTo(start) > 0)
                            {
                                if (prev.end().compareTo(end) > 0)
                                {
                                    noSpamLogger.warn("BTreeReducingRangeMap not well-formed: {} not before {}; skipping", prev, cur);
                                    prevEnd = end;
                                    continue;
                                }
                                else
                                {
                                    E newCur = cur.with(prev.end(), end);
                                    noSpamLogger.warn("BTreeReducingRangeMap not well-formed: {} not before {}; appending {}", prev, cur, newCur);
                                    cur = newCur;
                                }
                            }
                        }
                        builder.append(cur);
                        prevEnd = end;
                        prev = cur;
                    }
                }
                else
                {
                    // read linear format for upgrading from non-tree versions of collections
                    E prev = null;
                    RoutingKey prevStart = null;
                    while (mapSize-- > 0)
                    {
                        RoutingKey prevEnd = KeySerializers.routingKey.deserialize(in);
                        if (prev != null)
                            builder.append(prev.with(prevStart, prevEnd));
                        prev = deserializeArrayModeWithoutRange(in);
                        prevStart = prevEnd;
                    }
                    RoutingKey prevEnd = KeySerializers.routingKey.deserialize(in);
                    if (prev != null)
                        builder.append(prev.with(prevStart, prevEnd));

                }
                return builder.build();
            }

        }

        @Override
        public long serializedSize(Map map)
        {
            // for upgrading non-tree structures
            // noinspection UnnecessaryLocalVariable
            int mapFlags = REDUCING_BTREE_MODE;
            int mapSize = map.size();

            long size = TypeSizes.sizeofUnsignedVInt(mapFlags);
            size += TypeSizes.sizeofUnsignedVInt(mapSize);

            if (mapSize == 0)
                return size;

            E prev = null;
            int fixedLength = 0;
            for (E e : map)
            {
                int flags = 0;
                if (prev == null)
                {
                    fixedLength = KeySerializers.routingKey.fixedKeyLengthForPrefix(e.prefix());
                    flags = NEW_PREFIX | DISCONTIGUOUS;
                }
                else
                {
                    if (!prev.end().equals(e.start()))
                    {
                        flags = DISCONTIGUOUS;
                        if (!prev.prefix().equals(e.prefix()))
                            flags |= NEW_PREFIX;
                    }
                    size += 1;
                }

                if ((flags & DISCONTIGUOUS) != 0)
                {
                    if ((flags & NEW_PREFIX) != 0)
                    {
                        size += KeySerializers.routingKey.serializedSizeOfPrefix(e.prefix());
                        fixedLength = KeySerializers.routingKey.fixedKeyLengthForPrefix(e.prefix());
                    }
                    if (fixedLength < 0)
                        size += VIntCoding.sizeOfUnsignedVInt(KeySerializers.routingKey.serializedSizeWithoutPrefixOrLength(e.start()));
                    size += KeySerializers.routingKey.serializedSizeWithoutPrefixOrLength(e.start());
                }
                if (fixedLength < 0)
                    size +=  VIntCoding.sizeOfUnsignedVInt(KeySerializers.routingKey.serializedSizeWithoutPrefixOrLength(e.end()));
                size += KeySerializers.routingKey.serializedSizeWithoutPrefixOrLength(e.end());
                size += serializedSizeWithoutRange(e);
                prev = e;
            }

            return size;
        }
    }

    private static final class MaxConflictsSerializer extends BTreeReducingRangeMapSerializer<MaxConflicts.Entry, MaxConflicts>
    {
        // use top bits of a single byte vint, to leave room for base impl to fill other way
        private static final int SEPARATE_WRITES = 0x40;

        private MaxConflictsSerializer() {}

        @Override
        protected int mapFlags()
        {
            return SEPARATE_WRITES;
        }

        @Override
        MaxConflicts empty()
        {
            return MaxConflicts.EMPTY;
        }

        @Override
        BTreeReducingRangeMap.Builder<MaxConflicts.Entry, MaxConflicts> builder()
        {
            return new MaxConflicts.Builder();
        }

        @Override
        void serializeWithoutRange(MaxConflicts.Entry entry, DataOutputPlus out) throws IOException
        {
            CommandSerializers.timestamp.serialize(entry.any, out);
            CommandSerializers.timestamp.serialize(entry.write, out);
        }

        @Override
        long serializedSizeWithoutRange(MaxConflicts.Entry entry)
        {
            return CommandSerializers.timestamp.serializedSize(entry.any)
                    + CommandSerializers.timestamp.serializedSize(entry.write);
        }

        @Override
        MaxConflicts.Entry deserialize(RoutingKey start, RoutingKey end, DataInputPlus in, int mapFlags) throws IOException
        {
            Timestamp all = CommandSerializers.timestamp.deserialize(in);
            Timestamp writes = all;
            if ((mapFlags & SEPARATE_WRITES) != 0)
                writes = CommandSerializers.timestamp.deserialize(in);
            return new MaxConflicts.Entry(start, end, all, writes);
        }

        @Override
        MaxConflicts.Entry deserializeArrayModeWithoutRange(DataInputPlus in) throws IOException
        {
            Timestamp all = CommandSerializers.timestamp.deserialize(in);
            return new MaxConflicts.Entry(all, all);
        }
    }

    private static final class DurableBeforeSerializer extends BTreeReducingRangeMapSerializer<DurableBefore.Entry, DurableBefore>
    {
        private DurableBeforeSerializer() {}

        @Override
        DurableBefore empty()
        {
            return DurableBefore.EMPTY;
        }

        @Override
        DurableBefore.Builder builder()
        {
            return new DurableBefore.Builder();
        }

        @Override
        void serializeWithoutRange(DurableBefore.Entry entry, DataOutputPlus out) throws IOException
        {
            CommandSerializers.txnId.serialize(entry.quorum, out);
            CommandSerializers.txnId.serialize(entry.universal, out);
        }

        @Override
        long serializedSizeWithoutRange(DurableBefore.Entry entry)
        {
            return CommandSerializers.txnId.serializedSize(entry.quorum)
                 + CommandSerializers.txnId.serializedSize(entry.universal);
        }

        @Override
        DurableBefore.Entry deserialize(RoutingKey start, RoutingKey end, DataInputPlus in, int mapFlags) throws IOException
        {
            TxnId quorum = CommandSerializers.txnId.deserialize(in);
            TxnId universal = CommandSerializers.txnId.deserialize(in);
            return new DurableBefore.Entry(start, end, quorum, universal);
        }

        @Override
        DurableBefore.Entry deserializeArrayModeWithoutRange(DataInputPlus in) throws IOException
        {
            if (!in.readBoolean())
                return null;
            TxnId quorum = CommandSerializers.txnId.deserialize(in);
            TxnId universal = CommandSerializers.txnId.deserialize(in);
            return DurableBefore.Entry.constructWithoutRange(quorum, universal);
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
            out.writeUnsignedVInt32(t.encoded());
            if (flags == 0)
            {
                IdSingleEntry e = (IdSingleEntry) t;
                KeySerializers.range.serialize(e.range, out);
            }
            else
            {
                IdMultiEntry e = (IdMultiEntry) t;
                KeySerializers.ranges.serialize(e.ranges, out);
            }
        }

        @Override
        public IdEntry deserialize(DataInputPlus in) throws IOException
        {
            byte flags = in.readByte();
            TxnId txnId = CommandSerializers.txnId.deserialize(in);
            int encoded = in.readUnsignedVInt32();
            if (flags == 0)
            {
                Range range = KeySerializers.range.deserialize(in);
                return IdEntry.SerializerSupport.create(txnId, encoded, range);
            }
            else
            {
                Ranges ranges = KeySerializers.ranges.deserialize(in);
                return IdEntry.SerializerSupport.create(txnId, encoded, ranges);
            }
        }

        @Override
        public long serializedSize(IdEntry t)
        {
            return 1 + CommandSerializers.txnId.serializedSize(t)
                   + VIntCoding.sizeOfUnsignedVInt(t.encoded())
                   + (t.getClass() == IdSingleEntry.class ? KeySerializers.range.serializedSize(((IdSingleEntry)t).range)
                                                          : KeySerializers.ranges.serializedSize(((IdMultiEntry)t).ranges));
        }
    }

    static class RangesForEpochSerializer implements UnversionedSerializer<CommandStores.RangesForEpoch>
    {
        @Override
        public void serialize(CommandStores.RangesForEpoch from, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(from.size());
            for (int i = 0; i < from.size(); i++)
            {
                out.writeLong(from.epochAtIndex(i));
                KeySerializers.ranges.serialize(from.rangesAtIndex(i), out);
            }
        }

        @Override
        public CommandStores.RangesForEpoch deserialize(DataInputPlus in) throws IOException
        {
            int size = in.readUnsignedVInt32();
            Ranges[] ranges = new Ranges[size];
            long[] epochs = new long[size];
            for (int i = 0; i < ranges.length; i++)
            {
                epochs[i] = in.readLong();
                ranges[i] = KeySerializers.ranges.deserialize(in);
            }
            return new CommandStores.RangesForEpoch(epochs, ranges);
        }

        @Override
        public long serializedSize(CommandStores.RangesForEpoch from)
        {
            long size = TypeSizes.sizeofUnsignedVInt(from.size());
            for (int i = 0; i < from.size(); i++)
            {
                size += TypeSizes.LONG_SIZE;
                size += KeySerializers.ranges.serializedSize(from.rangesAtIndex(i));
            }
            return size;
        }
    }

}
