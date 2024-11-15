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
import accord.primitives.RoutableKey;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import accord.primitives.Seekables;
import accord.primitives.Unseekables;
import accord.primitives.Unseekables.UnseekablesKind;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutableKey.AccordKeySerializer;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.NullableSerializer;

public class KeySerializers
{
    public static final AccordKeySerializer<Key> key;
    public static final IVersionedSerializer<RoutingKey> routingKey;

    public static final IVersionedSerializer<RoutingKey> nullableRoutingKey;
    public static final AbstractKeysSerializer<RoutingKey, RoutingKeys> routingKeys;
    public static final IVersionedSerializer<Keys> keys;

    public static final AbstractKeysSerializer<?, PartialKeyRoute> partialKeyRoute;
    public static final AbstractKeysSerializer<?, FullKeyRoute> fullKeyRoute;

    public static final IVersionedSerializer<Range> range;
    public static final AbstractRangesSerializer<Ranges> ranges;
    public static final AbstractRangesSerializer<PartialRangeRoute> partialRangeRoute;
    public static final AbstractRangesSerializer<FullRangeRoute> fullRangeRoute;

    public static final AbstractRoutablesSerializer<Route<?>> route;
    public static final IVersionedSerializer<Route<?>> nullableRoute;
    public static final IVersionedSerializer<PartialRoute<?>> partialRoute;

    public static final IVersionedSerializer<FullRoute<?>> fullRoute;
    public static final IVersionedSerializer<Seekables<?, ?>> seekables;
    public static final IVersionedSerializer<FullRoute<?>> nullableFullRoute;
    public static final IVersionedSerializer<Unseekables<?>> unseekables;
    public static final IVersionedSerializer<Participants<?>> participants;
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
        final IVersionedSerializer<RoutingKey> routingKey;

        final IVersionedSerializer<RoutingKey> nullableRoutingKey;
        final AbstractKeysSerializer<RoutingKey, RoutingKeys> routingKeys;
        final IVersionedSerializer<Keys> keys;

        final AbstractKeysSerializer<?, PartialKeyRoute> partialKeyRoute;
        final AbstractKeysSerializer<?, FullKeyRoute> fullKeyRoute;

        final IVersionedSerializer<Range> range;
        final AbstractRangesSerializer<Ranges> ranges;
        final AbstractRangesSerializer<PartialRangeRoute> partialRangeRoute;
        final AbstractRangesSerializer<FullRangeRoute> fullRangeRoute;

        final AbstractRoutablesSerializer<Route<?>> route;
        final IVersionedSerializer<Route<?>> nullableRoute;
        final IVersionedSerializer<PartialRoute<?>> partialRoute;

        final IVersionedSerializer<FullRoute<?>> fullRoute;
        final IVersionedSerializer<Seekables<?, ?>> seekables;
        final IVersionedSerializer<FullRoute<?>> nullableFullRoute;
        final IVersionedSerializer<Unseekables<?>> unseekables;
        final IVersionedSerializer<Participants<?>> participants;
        final IVersionedSerializer<Participants<?>> nullableParticipants;
        private Impl()
        {
            this((AccordKeySerializer<Key>) (AccordKeySerializer<?>) PartitionKey.serializer,
                 (AccordKeySerializer<RoutingKey>) (AccordKeySerializer<?>) AccordRoutingKey.serializer,
                 (IVersionedSerializer<Range>) (IVersionedSerializer<?>) TokenRange.serializer);
        }

        @VisibleForTesting
        public Impl(AccordKeySerializer<Key> key,
                    IVersionedSerializer<RoutingKey> routingKey,
                    IVersionedSerializer<Range> range)
        {
            this.key = key;
            this.routingKey = routingKey;
            this.range = range;

            this.nullableRoutingKey = NullableSerializer.wrap(routingKey);
            this.routingKeys = new AbstractKeysSerializer<>(routingKey, RoutingKey[]::new)
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


            this.partialKeyRoute = new AbstractKeysSerializer<>(routingKey, RoutingKey[]::new)
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
                public long serializedSize(PartialKeyRoute keys, int version)
                {
                    return super.serializedSize(keys, version)
                           + routingKey.serializedSize(keys.homeKey, version);
                }
            };

            this.fullKeyRoute = new AbstractKeysSerializer<>(routingKey, RoutingKey[]::new)
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
                public long serializedSize(FullKeyRoute route, int version)
                {
                    return super.serializedSize(route, version)
                           + routingKey.serializedSize(route.homeKey, version);
                }
            };

            this.ranges = new AbstractRangesSerializer<Ranges>(range)
            {
                @Override
                public Ranges deserialize(DataInputPlus in, int version, Range[] ranges)
                {
                    return Ranges.ofSortedAndDeoverlapped(ranges);
                }
            };


            this.partialRangeRoute = new AbstractRangesSerializer<>(range)
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

            this.fullRangeRoute = new AbstractRangesSerializer<>(range)
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

            this.partialRoute = (IVersionedSerializer<PartialRoute<?>>) factory.apply(EnumSet.of(UnseekablesKind.PartialKeyRoute, UnseekablesKind.PartialRangeRoute));
            this.fullRoute = (IVersionedSerializer<FullRoute<?>>) factory.apply(EnumSet.of(UnseekablesKind.FullKeyRoute, UnseekablesKind.FullRangeRoute));
            this.nullableFullRoute = NullableSerializer.wrap(fullRoute);

            this.unseekables = (IVersionedSerializer<Unseekables<?>>) factory.apply(EnumSet.allOf(UnseekablesKind.class));
            this.participants = (IVersionedSerializer<Participants<?>>) factory.apply(EnumSet.allOf(UnseekablesKind.class));

            this.nullableParticipants = NullableSerializer.wrap(participants);
            this.seekables = new AbstractSeekablesSerializer(keys, ranges);
        }
    }

    static class AbstractRoutablesSerializer<RS extends Unseekables<?>> implements IVersionedSerializer<RS>
    {
        final EnumSet<UnseekablesKind> permitted;
        final AbstractKeysSerializer<RoutingKey, RoutingKeys> routingKeys;
        final AbstractKeysSerializer<?, PartialKeyRoute> partialKeyRoute;
        final AbstractKeysSerializer<?, FullKeyRoute> fullKeyRoute;
        final AbstractRangesSerializer<Ranges> ranges;
        final AbstractRangesSerializer<PartialRangeRoute> partialRangeRoute;
        final AbstractRangesSerializer<FullRangeRoute> fullRangeRoute;

        protected AbstractRoutablesSerializer(EnumSet<UnseekablesKind> permitted,
                                              AbstractKeysSerializer<RoutingKey, RoutingKeys> routingKeys,
                                              AbstractKeysSerializer<?, PartialKeyRoute> partialKeyRoute,
                                              AbstractKeysSerializer<?, FullKeyRoute> fullKeyRoute,
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
            if (!permitted.contains(kind))
                throw new IllegalStateException();
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

    public abstract static class AbstractKeysSerializer<K extends RoutableKey, KS extends AbstractKeys<K>> implements IVersionedSerializer<KS>
    {
        final IVersionedSerializer<K> keySerializer;
        final IntFunction<K[]> allocate;

        public AbstractKeysSerializer(IVersionedSerializer<K> keySerializer, IntFunction<K[]> allocate)
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

    public abstract static class AbstractRangesSerializer<RS extends AbstractRanges> implements IVersionedSerializer<RS>
    {
        private final IVersionedSerializer<Range> rangeSerializer;

        public AbstractRangesSerializer(IVersionedSerializer<Range> rangeSerializer)
        {
            this.rangeSerializer = rangeSerializer;
        }

        @Override
        public void serialize(RS ranges, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(ranges.size());
            for (int i=0, mi=ranges.size(); i<mi; i++)
                rangeSerializer.serialize(ranges.get(i), out, version);
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            int count = in.readUnsignedVInt32();
            for (int i = 0; i < count ; i++)
                rangeSerializer.deserialize(in, version);
        }

        @Override
        public RS deserialize(DataInputPlus in, int version) throws IOException
        {
            Range[] ranges = new Range[in.readUnsignedVInt32()];
            for (int i=0; i<ranges.length; i++)
                ranges[i] = rangeSerializer.deserialize(in, version);
            return deserialize(in, version, ranges);
        }

        abstract RS deserialize(DataInputPlus in, int version, Range[] ranges) throws IOException;

        @Override
        public long serializedSize(RS ranges, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(ranges.size());
            for (int i=0, mi=ranges.size(); i<mi; i++)
                size += TokenRange.serializer.serializedSize((TokenRange) ranges.get(i), version);
            return size;
        }
    }

    // TODO: port these to burn test serializers / journal
    public static Map<ByteBuffer, ByteBuffer> rangesToBlobMap(Ranges ranges)
    {
        TreeMap<ByteBuffer, ByteBuffer> result = new TreeMap<>();
        for (Range range : ranges)
        {
            result.put(AccordRoutingKey.serializer.serialize((AccordRoutingKey) range.start()),
                       AccordRoutingKey.serializer.serialize((AccordRoutingKey) range.end()));
        }
        return result;
    }

    public static Ranges blobMapToRanges(Map<ByteBuffer, ByteBuffer> blobMap)
    {
        int i = 0;
        Range[] ranges = new Range[blobMap.size()];
        for (Map.Entry<ByteBuffer, ByteBuffer> e : blobMap.entrySet())
        {
            ranges[i++] = new TokenRange(AccordRoutingKey.serializer.deserialize(e.getKey()),
                                         AccordRoutingKey.serializer.deserialize(e.getValue()));
        }
        return Ranges.of(ranges);
    }
}
