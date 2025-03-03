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
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.PartialKeyRoute;
import accord.primitives.PartialRangeRoute;
import accord.primitives.PartialRoute;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable;
import accord.primitives.RoutableKey;
import accord.primitives.Routables;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Unseekables;
import accord.primitives.Unseekables.UnseekablesKind;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutableKey.AccordKeySerializer;
import org.apache.cassandra.service.accord.api.AccordRoutableKey.AccordSearchableKeySerializer;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.NullableSerializer;

import static accord.utils.ArrayBuffers.cachedInts;

public class KeySerializers
{
    public static final AccordKeySerializer<Key> key;
    public static final IVersionedSerializer<RoutingKey> routingKey;

    public static final IVersionedSerializer<RoutingKey> nullableRoutingKey;
    public static final AbstractSearchableKeysSerializer<RoutingKey, RoutingKeys> routingKeys;
    public static final IVersionedSerializer<Keys> keys;

    public static final AbstractSearchableKeysSerializer<?, PartialKeyRoute> partialKeyRoute;
    public static final AbstractSearchableKeysSerializer<?, FullKeyRoute> fullKeyRoute;

    public static final IVersionedSerializer<Range> range;
    public static final AbstractRangesSerializer<Ranges> ranges;
    public static final AbstractRangesSerializer<PartialRangeRoute> partialRangeRoute;
    public static final AbstractRangesSerializer<FullRangeRoute> fullRangeRoute;

    public static final AbstractRoutablesSerializer<Route<?>> route;
    public static final IVersionedSerializer<Route<?>> nullableRoute;
    public static final IVersionedSerializer<PartialRoute<?>> partialRoute;

    public static final AbstractRoutablesSerializer<FullRoute<?>> fullRoute;
    public static final IVersionedSerializer<Seekables<?, ?>> seekables;
    public static final IVersionedSerializer<FullRoute<?>> nullableFullRoute;
    public static final AbstractRoutablesSerializer<Unseekables<?>> unseekables;
    public static final AbstractRoutablesSerializer<Participants<?>> participants;
    public static final IVersionedSerializer<Participants<?>> nullableParticipants;

    static
    {
        Impl impl = new Impl();
        key = impl.key;
        routingKey = impl.routingKey;

        nullableRoutingKey = impl.nullableRoutingKey;
        routingKeys = impl.routingKeys;
        keys = impl.keys;

        partialKeyRoute = impl.partialKeyRoute;
        fullKeyRoute = impl.fullKeyRoute;

        range = impl.range;
        ranges = impl.ranges;
        partialRangeRoute = impl.partialRangeRoute;
        fullRangeRoute = impl.fullRangeRoute;

        route = impl.route;
        nullableRoute = impl.nullableRoute;
        partialRoute = impl.partialRoute;

        fullRoute = impl.fullRoute;
        seekables = impl.seekables;
        nullableFullRoute = impl.nullableFullRoute;
        unseekables = impl.unseekables;
        participants = impl.participants;
        nullableParticipants = impl.nullableParticipants;
    }

    public static class Impl
    {
        final AccordKeySerializer<Key> key;
        final AccordSearchableKeySerializer<RoutingKey> routingKey;

        final IVersionedSerializer<RoutingKey> nullableRoutingKey;
        final AbstractSearchableKeysSerializer<RoutingKey, RoutingKeys> routingKeys;
        final IVersionedSerializer<Keys> keys;

        final AbstractSearchableKeysSerializer<?, PartialKeyRoute> partialKeyRoute;
        final AbstractSearchableKeysSerializer<?, FullKeyRoute> fullKeyRoute;

        final IVersionedSerializer<Range> range;
        final AbstractRangesSerializer<Ranges> ranges;
        final AbstractRangesSerializer<PartialRangeRoute> partialRangeRoute;
        final AbstractRangesSerializer<FullRangeRoute> fullRangeRoute;

        final AbstractRoutablesSerializer<Route<?>> route;
        final IVersionedSerializer<Route<?>> nullableRoute;
        final IVersionedSerializer<PartialRoute<?>> partialRoute;

        final AbstractRoutablesSerializer<FullRoute<?>> fullRoute;
        final AbstractSeekablesSerializer seekables;
        final IVersionedSerializer<FullRoute<?>> nullableFullRoute;
        final AbstractRoutablesSerializer<Unseekables<?>> unseekables;
        final AbstractRoutablesSerializer<Participants<?>> participants;
        final IVersionedSerializer<Participants<?>> nullableParticipants;
        private Impl()
        {
            this((AccordKeySerializer<Key>) (AccordKeySerializer<?>) PartitionKey.serializer,
                 (AccordSearchableKeySerializer<RoutingKey>) (AccordSearchableKeySerializer<?>) TokenKey.serializer,
                 (IVersionedSerializer<Range>) (IVersionedSerializer<?>) TokenRange.serializer);
        }

        @VisibleForTesting
        public Impl(AccordKeySerializer<Key> key,
                    AccordSearchableKeySerializer<RoutingKey> routingKey,
                    IVersionedSerializer<Range> range)
        {
            this.key = key;
            this.routingKey = routingKey;
            this.range = range;

            this.nullableRoutingKey = NullableSerializer.wrap(routingKey);
            this.routingKeys = new AbstractSearchableKeysSerializer<>(routingKey, RoutingKey[]::new)
            {
                @Override RoutingKeys deserialize(DataInputPlus in, int version, RoutingKey[] keys)
                {
                    return RoutingKeys.SerializationSupport.create(keys);
                }
            };

            this.keys = new AbstractKeysSerializer<>(key, Key[]::new)
            {
                @Override Keys deserialize(DataInputPlus in, int version, Key[] keys)
                {
                    return Keys.SerializationSupport.create(keys);
                }
            };

            this.partialKeyRoute = new AbstractSearchableKeysSerializer<>(routingKey, RoutingKey[]::new)
            {
                @Override PartialKeyRoute deserialize(DataInputPlus in, int version, RoutingKey[] keys) throws IOException
                {
                    RoutingKey homeKey = routingKey.deserialize(in, version);
                    return PartialKeyRoute.SerializationSupport.create(homeKey, keys);
                }

                @Override
                public void serialize(PartialKeyRoute route, DataOutputPlus out, int version) throws IOException
                {
                    super.serialize(route, out, version);
                    routingKey.serialize(route.homeKey, out, version);
                }

                @Override
                public long serializedSize(PartialKeyRoute routables, int version)
                {
                    return super.serializedSize(routables, version)
                           + routingKey.serializedSize(routables.homeKey, version);
                }
            };

            this.fullKeyRoute = new AbstractSearchableKeysSerializer<>(routingKey, RoutingKey[]::new)
            {
                @Override FullKeyRoute deserialize(DataInputPlus in, int version, RoutingKey[] keys) throws IOException
                {
                    RoutingKey homeKey = routingKey.deserialize(in, version);
                    return FullKeyRoute.SerializationSupport.create(homeKey, keys);
                }

                @Override
                public void serialize(FullKeyRoute route, DataOutputPlus out, int version) throws IOException
                {
                    super.serialize(route, out, version);
                    routingKey.serialize(route.homeKey, out, version);
                }

                @Override
                public long serializedSize(FullKeyRoute routables, int version)
                {
                    return super.serializedSize(routables, version)
                           + routingKey.serializedSize(routables.homeKey, version);
                }
            };

            this.ranges = new AbstractRangesSerializer<>(routingKey)
            {
                @Override
                public Ranges deserialize(DataInputPlus in, int version, Range[] ranges)
                {
                    return Ranges.ofSortedAndDeoverlapped(ranges);
                }
            };


            this.partialRangeRoute = new AbstractRangesSerializer<>(routingKey)
            {
                @Override PartialRangeRoute deserialize(DataInputPlus in, int version, Range[] rs) throws IOException
                {
                    RoutingKey homeKey = routingKey.deserialize(in, version);
                    return PartialRangeRoute.SerializationSupport.create(homeKey, rs);
                }

                @Override
                public void serialize(PartialRangeRoute route, DataOutputPlus out, int version) throws IOException
                {
                    super.serialize(route, out, version);
                    routingKey.serialize(route.homeKey, out, version);
                }

                @Override
                public long serializedSize(PartialRangeRoute rs, int version)
                {
                    return super.serializedSize(rs, version)
                           + routingKey.serializedSize(rs.homeKey, version);
                }
            };

            this.fullRangeRoute = new AbstractRangesSerializer<>(routingKey)
            {
                @Override FullRangeRoute deserialize(DataInputPlus in, int version, Range[] Ranges) throws IOException
                {
                    RoutingKey homeKey = routingKey.deserialize(in, version);
                    return FullRangeRoute.SerializationSupport.create(homeKey, Ranges);
                }

                @Override
                public void serialize(FullRangeRoute route, DataOutputPlus out, int version) throws IOException
                {
                    super.serialize(route, out, version);
                    routingKey.serialize(route.homeKey, out, version);
                }

                @Override
                public long serializedSize(FullRangeRoute ranges, int version)
                {
                    return super.serializedSize(ranges, version)
                           + routingKey.serializedSize(ranges.homeKey(), version);
                }
            };

            Function<EnumSet<UnseekablesKind>, AbstractRoutablesSerializer<?>> factory = (a) -> new AbstractRoutablesSerializer<>(a, routingKeys, partialKeyRoute, fullKeyRoute, ranges, partialRangeRoute, fullRangeRoute);

            this.route = (AbstractRoutablesSerializer<Route<?>>) factory.apply(EnumSet.of(UnseekablesKind.PartialKeyRoute, UnseekablesKind.FullKeyRoute, UnseekablesKind.PartialRangeRoute, UnseekablesKind.FullRangeRoute));
            this.nullableRoute = NullableSerializer.wrap(route);

            this.partialRoute = (AbstractRoutablesSerializer<PartialRoute<?>>) factory.apply(EnumSet.of(UnseekablesKind.PartialKeyRoute, UnseekablesKind.PartialRangeRoute));
            this.fullRoute = (AbstractRoutablesSerializer<FullRoute<?>>) factory.apply(EnumSet.of(UnseekablesKind.FullKeyRoute, UnseekablesKind.FullRangeRoute));
            this.nullableFullRoute = NullableSerializer.wrap(fullRoute);

            this.unseekables = (AbstractRoutablesSerializer<Unseekables<?>>) factory.apply(EnumSet.allOf(UnseekablesKind.class));
            this.participants = (AbstractRoutablesSerializer<Participants<?>>) factory.apply(EnumSet.allOf(UnseekablesKind.class));

            this.nullableParticipants = NullableSerializer.wrap(participants);
            this.seekables = new AbstractSeekablesSerializer(keys, ranges);
        }
    }

    public static class AbstractRoutablesSerializer<RS extends Unseekables<?>> implements IVersionedSerializer<RS>
    {
        final EnumSet<UnseekablesKind> permitted;
        final AbstractSearchableKeysSerializer<RoutingKey, RoutingKeys> routingKeys;
        final AbstractSearchableKeysSerializer<?, PartialKeyRoute> partialKeyRoute;
        final AbstractSearchableKeysSerializer<?, FullKeyRoute> fullKeyRoute;
        final AbstractRangesSerializer<Ranges> ranges;
        final AbstractRangesSerializer<PartialRangeRoute> partialRangeRoute;
        final AbstractRangesSerializer<FullRangeRoute> fullRangeRoute;

        protected AbstractRoutablesSerializer(EnumSet<UnseekablesKind> permitted,
                                              AbstractSearchableKeysSerializer<RoutingKey, RoutingKeys> routingKeys,
                                              AbstractSearchableKeysSerializer<?, PartialKeyRoute> partialKeyRoute,
                                              AbstractSearchableKeysSerializer<?, FullKeyRoute> fullKeyRoute,
                                              AbstractRangesSerializer<Ranges> ranges,
                                              AbstractRangesSerializer<PartialRangeRoute> partialRangeRoute,
                                              AbstractRangesSerializer<FullRangeRoute> fullRangeRoute)
        {
            this.permitted = permitted;
            this.routingKeys = routingKeys;
            this.partialKeyRoute = partialKeyRoute;
            this.fullKeyRoute = fullKeyRoute;
            this.ranges = ranges;
            this.partialRangeRoute = partialRangeRoute;
            this.fullRangeRoute = fullRangeRoute;
        }

        @Override
        public void serialize(RS t, DataOutputPlus out, int version) throws IOException
        {
            UnseekablesKind kind = t.kind();
            if (!permitted.contains(kind))
                throw new IllegalArgumentException();

            switch (kind)
            {
                default: throw new AssertionError();
                case RoutingKeys:
                    out.writeByte(1);
                    routingKeys.serialize((RoutingKeys)t, out, version);
                    break;
                case PartialKeyRoute:
                    out.writeByte(2);
                    partialKeyRoute.serialize((PartialKeyRoute)t, out, version);
                    break;
                case FullKeyRoute:
                    out.writeByte(3);
                    fullKeyRoute.serialize((FullKeyRoute)t, out, version);
                    break;
                case RoutingRanges:
                    out.writeByte(4);
                    ranges.serialize((Ranges)t, out, version);
                    break;
                case PartialRangeRoute:
                    out.writeByte(5);
                    partialRangeRoute.serialize((PartialRangeRoute)t, out, version);
                    break;
                case FullRangeRoute:
                    out.writeByte(6);
                    fullRangeRoute.serialize((FullRangeRoute)t, out, version);
                    break;
            }
        }

        @Override
        public RS deserialize(DataInputPlus in, int version) throws IOException
        {
            byte b = in.readByte();
            UnseekablesKind kind;
            RS result;
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1, 2, 3, 4 or 5; received " + b);
                case 1: kind = UnseekablesKind.RoutingKeys; result = (RS)routingKeys.deserialize(in, version); break;
                case 2: kind = UnseekablesKind.PartialKeyRoute; result = (RS)partialKeyRoute.deserialize(in, version); break;
                case 3: kind = UnseekablesKind.FullKeyRoute; result = (RS)fullKeyRoute.deserialize(in, version); break;
                case 4: kind = UnseekablesKind.RoutingRanges; result = (RS)ranges.deserialize(in, version); break;
                case 5: kind = UnseekablesKind.PartialRangeRoute; result = (RS)partialRangeRoute.deserialize(in, version); break;
                case 6: kind = UnseekablesKind.FullRangeRoute; result = (RS)fullRangeRoute.deserialize(in, version); break;
            }
            Invariants.require(permitted.contains(kind));
            return result;
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1, 2, 3, 4 or 5; received " + b);
                case 1: routingKeys.skip(in, version); break;
                case 2: partialKeyRoute.skip(in, version); break;
                case 3: fullKeyRoute.skip(in, version); break;
                case 4: ranges.skip(in, version); break;
                case 5: partialRangeRoute.skip(in, version); break;
                case 6: fullRangeRoute.skip(in, version); break;
            }
        }

        @Override
        public long serializedSize(RS t, int version)
        {
            switch (t.kind())
            {
                default: throw new AssertionError();
                case RoutingKeys:
                    return 1 + routingKeys.serializedSize((RoutingKeys)t, version);
                case PartialKeyRoute:
                    return 1 + partialKeyRoute.serializedSize((PartialKeyRoute)t, version);
                case FullKeyRoute:
                    return 1 + fullKeyRoute.serializedSize((FullKeyRoute)t, version);
                case RoutingRanges:
                    return 1 + ranges.serializedSize((Ranges)t, version);
                case PartialRangeRoute:
                    return 1 + partialRangeRoute.serializedSize((PartialRangeRoute)t, version);
                case FullRangeRoute:
                    return 1 + fullRangeRoute.serializedSize((FullRangeRoute)t, version);
            }
        }
    }

    public static final IVersionedSerializer<Seekable> seekable = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Seekable seekable, DataOutputPlus out, int version) throws IOException
        {
            switch (seekable.domain())
            {
                default: throw new AssertionError();
                case Key:
                    out.writeByte(0);
                    PartitionKey.serializer.serialize((PartitionKey) seekable, out, version);
                    break;
                case Range:
                    out.writeByte(1);
                    TokenRange.serializer.serialize((TokenRange) seekable, out, version);
                    break;
            }
        }

        @Override
        public Seekable deserialize(DataInputPlus in, int version) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1 or 2, received " + b);
                case 0: return PartitionKey.serializer.deserialize(in, version);
                case 1: return TokenRange.serializer.deserialize(in, version);
            }
        }

        @Override
        public long serializedSize(Seekable seekable, int version)
        {
            switch (seekable.domain())
            {
                default: throw new AssertionError();
                case Key:
                    return 1 + PartitionKey.serializer.serializedSize((PartitionKey) seekable, version);
                case Range:
                    return 1 + TokenRange.serializer.serializedSize((TokenRange) seekable, version);
            }
        }
    };

    public static class AbstractSeekablesSerializer implements IVersionedSerializer<Seekables<?, ?>>
    {
        final IVersionedSerializer<Keys> keys;
        final AbstractRangesSerializer<Ranges> ranges;

        public AbstractSeekablesSerializer(IVersionedSerializer<Keys> keys, AbstractRangesSerializer<Ranges> ranges)
        {
            this.keys = keys;
            this.ranges = ranges;
        }

        @Override
        public void serialize(Seekables<?, ?> t, DataOutputPlus out, int version) throws IOException
        {
            switch (t.domain())
            {
                default: throw new AssertionError();
                case Key:
                    out.writeByte(1);
                    keys.serialize((Keys)t, out, version);
                    break;
                case Range:
                    out.writeByte(2);
                    ranges.serialize((Ranges)t, out, version);
                    break;
            }
        }

        @Override
        public Seekables<?, ?> deserialize(DataInputPlus in, int version) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1 or 2, received " + b);
                case 1: return keys.deserialize(in, version);
                case 2: return ranges.deserialize(in, version);
            }
        }

        @Override
        public long serializedSize(Seekables<?, ?> t, int version)
        {
            switch (t.domain())
            {
                default: throw new AssertionError();
                case Key:
                    return 1 + keys.serializedSize((Keys)t, version);
                case Range:
                    return 1 + ranges.serializedSize((Ranges)t, version);
            }
        }
    }

    // this serializer is designed to permits using the collection in its serialized form with minimal in-memory state.
    // it also saves some memory by avoiding duplicating prefixes (which happens to also assist faster lookups)
    public abstract static class AbstractKeysSerializer<K extends RoutableKey, KS extends AbstractKeys<K>> implements IVersionedSerializer<KS>
    {
        final AccordKeySerializer<K> keySerializer;
        final IntFunction<K[]> allocate;

        public AbstractKeysSerializer(AccordKeySerializer<K> keySerializer, IntFunction<K[]> allocate)
        {
            this.keySerializer = keySerializer;
            this.allocate = allocate;
        }

        @Override
        public void serialize(KS keys, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(keys.size());
            for (int i=0, mi=keys.size(); i<mi; i++)
                keySerializer.serialize(keys.get(i), out, version);
        }

        abstract KS deserialize(DataInputPlus in, int version, K[] keys) throws IOException;

        public void skip(DataInputPlus in, int version) throws IOException
        {
            int count = in.readUnsignedVInt32();
            for (int i = 0; i < count ; i++)
                keySerializer.deserialize(in, version);
        }

        @Override
        public KS deserialize(DataInputPlus in, int version) throws IOException
        {
            K[] keys = allocate.apply(in.readUnsignedVInt32());
            for (int i=0; i<keys.length; i++)
                keys[i] = keySerializer.deserialize(in, version);
            return deserialize(in, version, keys);
        }

        @Override
        public long serializedSize(KS keys, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(keys.size());
            for (int i=0, mi=keys.size(); i<mi; i++)
                size += keySerializer.serializedSize(keys.get(i), version);
            return size;
        }
    }

    // this serializer is designed to permits using the collection in its serialized form with minimal in-memory state.
    // it also saves some memory by avoiding duplicating prefixes (which happens to also assist faster lookups)
    public abstract static class AbstractSearchableSerializer<K extends RoutableKey, R extends Routable, RS extends Routables<R>> implements IVersionedSerializer<RS>
    {
        final AccordSearchableKeySerializer<K> keySerializer;
        final IntFunction<R[]> allocate;

        public AbstractSearchableSerializer(AccordSearchableKeySerializer<K> keySerializer, IntFunction<R[]> allocate)
        {
            this.keySerializer = keySerializer;
            this.allocate = allocate;
        }

        private int serializedSizeOfPrefix(Object prefix)
        {
            return keySerializer.serializedSizeOfPrefix(prefix);
        }

        private void serializePrefix(Object prefix, DataOutputPlus out, int version) throws IOException
        {
            keySerializer.serializePrefix(prefix, out, version);
        }

        private Object deserializePrefix(DataInputPlus in, int version) throws IOException
        {
            return keySerializer.deserializePrefix(in, version);
        }

        // if we store Ranges, we have twice as many indexes
        abstract int recordCountToLengthCount(int recordCount);
        abstract int fixedKeyLengthForPrefix(Object prefix);
        abstract int serializedSizeWithoutPrefix(R routable);
        abstract void serializeWithoutPrefixOrLength(R routable, DataOutputPlus out, int version) throws IOException;
        abstract void serializeOffsets(RS unseekables, int start, int end, DataOutputPlus out) throws IOException;

        abstract R deserializeWithPrefix(Object prefix, int length, DataInputPlus in, int version) throws IOException;
        abstract R deserializeWithPrefix(Object prefix, int lengthIndex, int[] lengths, DataInputPlus in, int version) throws IOException;

        abstract RS deserialize(DataInputPlus in, int version, R[] keys) throws IOException;

        @Override
        public long serializedSize(RS routables, int version)
        {
            int count = routables.size();
            long size = TypeSizes.sizeofUnsignedVInt(count);
            if (count == 0)
                return size;

            Object prefix = routables.get(0).prefix();
            int prefixStart = 0;
            for (int i = 1 ; i <= count ; ++i)
            {
                Object nextPrefix = null;
                if (i < count)
                {
                    nextPrefix = routables.get(i).prefix();
                    if (Objects.equals(prefix, nextPrefix))
                        continue;
                }

                size += TypeSizes.sizeofUnsignedVInt(count - i);
                size += serializedSizeOfPrefix(prefix);
                int fixedLength = fixedKeyLengthForPrefix(prefix);
                if (fixedLength < 0)
                {
                    size += 4L * recordCountToLengthCount(i - prefixStart);
                    size += serializedSizeOfKeysWithoutPrefix(routables, prefixStart, i);
                }
                else
                {
                    size += fixedLength * (long)(i - prefixStart);
                }
                prefixStart = i;
                prefix = nextPrefix;
            }

            return size;
        }

        @Override
        public void serialize(RS keys, DataOutputPlus out, int version) throws IOException
        {
            int size = keys.size();
            out.writeUnsignedVInt32(size);
            if (size == 0)
                return;

            Object prefix = keys.get(0).prefix();
            int prefixStart = 0;
            for (int i = 1 ; i <= size ; ++i)
            {
                Object nextPrefix = null;
                if (i < size)
                {
                    nextPrefix = keys.get(i).prefix();
                    if (Objects.equals(prefix, nextPrefix))
                        continue;
                }

                out.writeUnsignedVInt32(size - i);
                serializePrefix(prefix, out, version);
                int fixedLength = fixedKeyLengthForPrefix(prefix);
                if (fixedLength < 0)
                    serializeOffsets(keys, prefixStart, i, out);
                serializeKeysWithoutPrefix(keys, prefixStart, i, out, version);
                prefixStart = i;
                prefix = nextPrefix;
            }
        }

        private long serializedSizeOfKeysWithoutPrefix(RS keys, int start, int end)
        {
            long size = 0;
            for (int i = start; i < end; ++i)
                size += serializedSizeWithoutPrefix(keys.get(i));
            return size;
        }

        private void serializeKeysWithoutPrefix(RS keys, int start, int end, DataOutputPlus out, int version) throws IOException
        {
            for (int i = start; i < end; ++i)
                serializeWithoutPrefixOrLength(keys.get(i), out, version);
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            int remaining = in.readUnsignedVInt32();
            if (remaining == 0)
                return;

            while (remaining > 0)
            {
                int count = remaining - in.readUnsignedVInt32();
                remaining -= count;
                Object prefix = deserializePrefix(in, version);
                int fixedLength = fixedKeyLengthForPrefix(prefix);
                if (fixedLength >= 0)
                {
                    in.skipBytesFully(count * fixedLength);
                }
                else
                {
                    in.skipBytesFully(4 * (recordCountToLengthCount(count) - 1));
                    int end = in.readInt();
                    in.skipBytesFully(end);
                }
            }
        }

        @Override
        public RS deserialize(DataInputPlus in, int version) throws IOException
        {
            int remaining = in.readUnsignedVInt32();
            R[] out = allocate.apply(remaining);
            int outCount = 0;
            while (remaining > 0)
            {
                int count = remaining - in.readUnsignedVInt32();
                remaining -= count;
                Object prefix = deserializePrefix(in, version);
                int fixedLength = fixedKeyLengthForPrefix(prefix);
                if (fixedLength >= 0)
                {
                    for (int i = 0 ; i < count ; ++i)
                        out[outCount++] = deserializeWithPrefix(prefix, fixedLength, in, version);
                }
                else
                {
                    int lengthCount = recordCountToLengthCount(count);
                    if (lengthCount == 1)
                    {
                        int end = in.readInt();
                        out[outCount++] = deserializeWithPrefix(prefix, end, in, version);
                    }
                    else
                    {
                        int[] lengths = cachedInts().getInts(lengthCount);
                        int prev = 0;
                        for (int i = 0 ; i < lengthCount ; ++i)
                        {
                            int end = in.readInt();
                            lengths[i] = end - prev;
                            prev = end;
                        }
                        for (int i = 0 ; i < count ; ++i)
                            out[outCount++] = deserializeWithPrefix(prefix, i, lengths, in, version);
                        cachedInts().forceDiscard(lengths);
                    }
                }
            }

            return deserialize(in, version, out);
        }
    }

    // this serializer is designed to permits using the collection in its serialized form with minimal in-memory state.
    // it also saves some memory by avoiding duplicating prefixes (which happens to also assist faster lookups)
    public abstract static class AbstractSearchableKeysSerializer<K extends RoutableKey, KS extends AbstractKeys<K>> extends AbstractSearchableSerializer<K, K, KS> implements IVersionedSerializer<KS>
    {
        public AbstractSearchableKeysSerializer(AccordSearchableKeySerializer<K> keySerializer, IntFunction<K[]> allocate)
        {
            super(keySerializer, allocate);
        }

        @Override
        final int fixedKeyLengthForPrefix(Object prefix)
        {
            return keySerializer.fixedKeyLengthForPrefix(prefix);
        }

        @Override
        final int recordCountToLengthCount(int recordCount)
        {
            return recordCount;
        }

        @Override
        final int serializedSizeWithoutPrefix(K routable)
        {
            return keySerializer.serializedSizeWithoutPrefix(routable);
        }

        @Override
        final void serializeWithoutPrefixOrLength(K routable, DataOutputPlus out, int version) throws IOException
        {
            keySerializer.serializeWithoutPrefixOrLength(routable, out, version);
        }

        @Override
        final void serializeOffsets(KS keys, int startIndex, int endIndex, DataOutputPlus out) throws IOException
        {
            int endOffset = 0;
            for (int i = startIndex; i < endIndex; ++i)
            {
                endOffset += serializedSizeWithoutPrefix(keys.get(i));
                out.writeInt(endOffset);
            }
        }

        @Override
        final K deserializeWithPrefix(Object prefix, int length, DataInputPlus in, int version) throws IOException
        {
            return keySerializer.deserializeWithPrefix(prefix, length, in, version);
        }

        @Override
        final K deserializeWithPrefix(Object prefix, int lengthIndex, int[] lengths, DataInputPlus in, int version) throws IOException
        {
            return keySerializer.deserializeWithPrefix(prefix, lengths[lengthIndex], in, version);
        }
    }

    public abstract static class AbstractRangesSerializer<RS extends AbstractRanges> extends AbstractSearchableSerializer<RoutingKey, Range, RS> implements IVersionedSerializer<RS>
    {
        public AbstractRangesSerializer(AccordSearchableKeySerializer<RoutingKey> keySerializer)
        {
            super(keySerializer, Range[]::new);
        }

        @Override
        int fixedKeyLengthForPrefix(Object prefix)
        {
            return keySerializer.fixedKeyLengthForPrefix(prefix) * 2;
        }

        @Override
        int recordCountToLengthCount(int recordCount)
        {
            return recordCount * 2;
        }

        @Override
        final int serializedSizeWithoutPrefix(Range range)
        {
            return keySerializer.serializedSizeWithoutPrefix(range.start())
                   + keySerializer.serializedSizeWithoutPrefix(range.end());
        }

        @Override
        final void serializeWithoutPrefixOrLength(Range key, DataOutputPlus out, int version) throws IOException
        {
            keySerializer.serializeWithoutPrefixOrLength(key.start(), out, version);
            keySerializer.serializeWithoutPrefixOrLength(key.end(), out, version);
        }

        @Override
        final void serializeOffsets(RS ranges, int startIndex, int endIndex, DataOutputPlus out) throws IOException
        {
            int endOffset = 0;
            for (int i = startIndex; i < endIndex; ++i)
            {
                Range r = ranges.get(i);
                endOffset += keySerializer.serializedSizeWithoutPrefix(r.start());
                out.writeInt(endOffset);
                endOffset += keySerializer.serializedSizeWithoutPrefix(r.end());
                out.writeInt(endOffset);
            }
        }

        @Override
        final Range deserializeWithPrefix(Object prefix, int length, DataInputPlus in, int version) throws IOException
        {
            RoutingKey start = keySerializer.deserializeWithPrefix(prefix, length/2, in, version);
            RoutingKey end = keySerializer.deserializeWithPrefix(prefix, length/2, in, version);
            return start.rangeFactory().newRange(start, end);
        }

        @Override
        final Range deserializeWithPrefix(Object prefix, int lengthIndex, int[] lengths, DataInputPlus in, int version) throws IOException
        {
            RoutingKey start = keySerializer.deserializeWithPrefix(prefix, lengths[lengthIndex * 2], in, version);
            RoutingKey end = keySerializer.deserializeWithPrefix(prefix, lengths[lengthIndex * 2 + 1], in, version);
            return start.rangeFactory().newRange(start, end);
        }
    }

    public static Map<ByteBuffer, ByteBuffer> rangesToBlobMap(Ranges ranges)
    {
        TreeMap<ByteBuffer, ByteBuffer> result = new TreeMap<>();
        for (Range range : ranges)
        {
            result.put(TokenKey.serializer.serialize((TokenKey) range.start()),
                       TokenKey.serializer.serialize((TokenKey) range.end()));
        }
        return result;
    }

    public static Ranges blobMapToRanges(Map<ByteBuffer, ByteBuffer> blobMap)
    {
        int i = 0;
        Range[] ranges = new Range[blobMap.size()];
        for (Map.Entry<ByteBuffer, ByteBuffer> e : blobMap.entrySet())
        {
            ranges[i++] = TokenRange.create(TokenKey.serializer.deserialize(e.getKey()),
                                            TokenKey.serializer.deserialize(e.getValue()));
        }
        return Ranges.of(ranges);
    }
}
