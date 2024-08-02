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
import java.util.function.IntFunction;

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
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.NullableSerializer;

public class KeySerializers
{
    private KeySerializers() {}

    public static final IVersionedSerializer<Key> key = (IVersionedSerializer<Key>) (IVersionedSerializer<?>) PartitionKey.serializer;
    public static final IVersionedSerializer<RoutingKey> routingKey = (IVersionedSerializer<RoutingKey>) (IVersionedSerializer<?>) AccordRoutingKey.serializer;
    public static final IVersionedSerializer<RoutingKey> nullableRoutingKey = NullableSerializer.wrap(routingKey);

    public static final IVersionedSerializer<RoutingKeys> routingKeys = new AbstractKeysSerializer<RoutingKey, RoutingKeys>(routingKey, RoutingKey[]::new)
    {
        @Override RoutingKeys deserialize(DataInputPlus in, int version, RoutingKey[] keys)
        {
            return RoutingKeys.SerializationSupport.create(keys);
        }
    };

    public static final IVersionedSerializer<Keys> keys = new AbstractKeysSerializer<Key, Keys>(key, Key[]::new)
    {
        @Override Keys deserialize(DataInputPlus in, int version, Key[] keys)
        {
            return Keys.SerializationSupport.create(keys);
        }
    };

    public static final IVersionedSerializer<Ranges> ranges = new AbstractRangesSerializer<Ranges>()
    {
        @Override
        public Ranges deserialize(DataInputPlus in, int version, Range[] ranges)
        {
            return Ranges.ofSortedAndDeoverlapped(ranges);
        }
    };

    public static final IVersionedSerializer<PartialKeyRoute> partialKeyRoute = new AbstractKeysSerializer<RoutingKey, PartialKeyRoute>(routingKey, RoutingKey[]::new)
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

    public static final IVersionedSerializer<FullKeyRoute> fullKeyRoute = new AbstractKeysSerializer<>(routingKey, RoutingKey[]::new)
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

    public static final IVersionedSerializer<PartialRangeRoute> partialRangeRoute = new AbstractRangesSerializer<PartialRangeRoute>()
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

    public static final IVersionedSerializer<FullRangeRoute> fullRangeRoute = new AbstractRangesSerializer<FullRangeRoute>()
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

    public static final IVersionedSerializer<Route<?>> route = new AbstractRoutablesSerializer<>(
        EnumSet.of(UnseekablesKind.PartialKeyRoute, UnseekablesKind.FullKeyRoute, UnseekablesKind.PartialRangeRoute, UnseekablesKind.FullRangeRoute)
    );
    public static final IVersionedSerializer<Route<?>> nullableRoute = NullableSerializer.wrap(route);

    public static final IVersionedSerializer<PartialRoute<?>> partialRoute = new AbstractRoutablesSerializer<>(
        EnumSet.of(UnseekablesKind.PartialKeyRoute, UnseekablesKind.PartialRangeRoute)
    );

    public static final IVersionedSerializer<FullRoute<?>> fullRoute = new AbstractRoutablesSerializer<>(
        EnumSet.of(UnseekablesKind.FullKeyRoute, UnseekablesKind.FullRangeRoute)
    );

    public static final IVersionedSerializer<FullRoute<?>> nullableFullRoute = NullableSerializer.wrap(fullRoute);

    public static final IVersionedSerializer<Unseekables<?>> unseekables = new AbstractRoutablesSerializer<>(
        EnumSet.allOf(UnseekablesKind.class)
    );

    public static final IVersionedSerializer<Participants<?>> participants = new AbstractRoutablesSerializer<>(
        EnumSet.allOf(UnseekablesKind.class)
    );

    static class AbstractRoutablesSerializer<RS extends Unseekables<?>> implements IVersionedSerializer<RS>
    {
        final EnumSet<UnseekablesKind> permitted;
        protected AbstractRoutablesSerializer(EnumSet<UnseekablesKind> permitted)
        {
            this.permitted = permitted;
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

    public static final IVersionedSerializer<Seekables<?, ?>> seekables = new IVersionedSerializer<Seekables<?, ?>>()
    {
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
    };

    public static final IVersionedSerializer<Seekables<?, ?>> nullableSeekables = NullableSerializer.wrap(seekables);

    public static abstract class AbstractKeysSerializer<K extends RoutableKey, KS extends AbstractKeys<K>> implements IVersionedSerializer<KS>
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

    public static abstract class AbstractRangesSerializer<RS extends AbstractRanges> implements IVersionedSerializer<RS>
    {
        @Override
        public void serialize(RS ranges, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(ranges.size());
            for (int i=0, mi=ranges.size(); i<mi; i++)
                TokenRange.serializer.serialize((TokenRange) ranges.get(i), out, version);
        }

        @Override
        public RS deserialize(DataInputPlus in, int version) throws IOException
        {
            Range[] ranges = new Range[in.readUnsignedVInt32()];
            for (int i=0; i<ranges.length; i++)
                ranges[i] = TokenRange.serializer.deserialize(in, version);
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
