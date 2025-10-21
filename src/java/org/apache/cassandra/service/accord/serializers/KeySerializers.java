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
import java.util.Objects;
import java.util.function.Function;
import java.util.function.IntFunction;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRanges;
import accord.primitives.AbstractUnseekableKeys;
import accord.primitives.FullKeyRoute;
import accord.primitives.FullRangeRoute;
import accord.primitives.FullRoute;
import accord.primitives.KeyRoute;
import accord.primitives.Keys;
import accord.primitives.PartialKeyRoute;
import accord.primitives.PartialRangeRoute;
import accord.primitives.PartialRoute;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeRoute;
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
import accord.utils.TinyEnumSet;
import accord.utils.UnhandledEnum;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.UnversionedSerializer;
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
    public static final AccordSearchableKeySerializer<RoutingKey> routingKey;

    public static final UnversionedSerializer<RoutingKey> nullableRoutingKey;
    public static final AbstractKeyRoutablesSerializer<RoutingKeys> routingKeys;
    public static final UnversionedSerializer<Keys> keys;

    public static final AbstractKeyRoutablesSerializer<PartialKeyRoute> partialKeyRoute;
    public static final AbstractKeyRoutablesSerializer<FullKeyRoute> fullKeyRoute;

    public static final UnversionedSerializer<Range> range;
    public static final AbstractRangesSerializer<Range[]> rangeArray;
    public static final AbstractRangeRoutablesSerializer<Ranges> ranges;
    public static final AbstractRangeRoutablesSerializer<PartialRangeRoute> partialRangeRoute;
    public static final AbstractRangeRoutablesSerializer<FullRangeRoute> fullRangeRoute;

    public static final AbstractRoutablesSerializer<Route<?>> route;
    public static final UnversionedSerializer<Route<?>> nullableRoute;
    public static final UnversionedSerializer<PartialRoute<?>> partialRoute;

    public static final AbstractRoutablesSerializer<FullRoute<?>> fullRoute;
    public static final UnversionedSerializer<Seekables<?, ?>> seekables;
    public static final UnversionedSerializer<FullRoute<?>> nullableFullRoute;
    public static final AbstractRoutablesSerializer<Unseekables<?>> unseekables;
    public static final AbstractRoutablesSerializer<Participants<?>> participants;
    public static final UnversionedSerializer<Participants<?>> nullableParticipants;

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
        rangeArray = impl.rangeArray;
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

        final UnversionedSerializer<RoutingKey> nullableRoutingKey;
        final AbstractKeyRoutablesSerializer<RoutingKeys> routingKeys;
        final UnversionedSerializer<Keys> keys;

        final AbstractKeyRoutablesSerializer<PartialKeyRoute> partialKeyRoute;
        final AbstractKeyRoutablesSerializer<FullKeyRoute> fullKeyRoute;

        final UnversionedSerializer<Range> range;
        final AbstractRangesSerializer<Range[]> rangeArray;
        final AbstractRangeRoutablesSerializer<Ranges> ranges;
        final AbstractRangeRoutablesSerializer<PartialRangeRoute> partialRangeRoute;
        final AbstractRangeRoutablesSerializer<FullRangeRoute> fullRangeRoute;

        final AbstractRoutablesSerializer<Route<?>> route;
        final UnversionedSerializer<Route<?>> nullableRoute;
        final UnversionedSerializer<PartialRoute<?>> partialRoute;

        final AbstractRoutablesSerializer<FullRoute<?>> fullRoute;
        final AbstractSeekablesSerializer seekables;
        final UnversionedSerializer<FullRoute<?>> nullableFullRoute;
        final AbstractRoutablesSerializer<Unseekables<?>> unseekables;
        final AbstractRoutablesSerializer<Participants<?>> participants;
        final UnversionedSerializer<Participants<?>> nullableParticipants;
        private Impl()
        {
            this((AccordKeySerializer<Key>) (AccordKeySerializer<?>) PartitionKey.serializer,
                 (AccordSearchableKeySerializer<RoutingKey>) (AccordSearchableKeySerializer<?>) TokenKey.serializer,
                 (UnversionedSerializer<Range>) (UnversionedSerializer<?>) TokenRange.serializer);
        }

        @VisibleForTesting
        public Impl(AccordKeySerializer<Key> key,
                    AccordSearchableKeySerializer<RoutingKey> routingKey,
                    UnversionedSerializer<Range> range)
        {
            this.key = key;
            this.routingKey = routingKey;
            this.range = range;

            this.nullableRoutingKey = NullableSerializer.wrap(routingKey);
            this.routingKeys = new AbstractKeyRoutablesSerializer<>()
            {
                @Override RoutingKeys deserialize(DataInputPlus in, RoutingKey[] keys)
                {
                    return RoutingKeys.SerializationSupport.create(keys);
                }
            };

            this.keys = new AbstractKeysSerializer<>(key, Key[]::new)
            {
                @Override Keys deserialize(DataInputPlus in, Key[] keys)
                {
                    return Keys.SerializationSupport.create(keys);
                }
            };

            this.partialKeyRoute = new AbstractKeyRouteSerializer<>()
            {
                @Override
                PartialKeyRoute construct(RoutingKey homeKey, RoutingKey[] keys)
                {
                    return PartialKeyRoute.SerializationSupport.create(homeKey, keys);
                }
            };

            this.fullKeyRoute = new AbstractKeyRouteSerializer<>()
            {
                @Override
                FullKeyRoute construct(RoutingKey homeKey, RoutingKey[] keys)
                {
                    return FullKeyRoute.SerializationSupport.create(homeKey, keys);
                }
            };

            this.ranges = new AbstractRangeRoutablesSerializer<>()
            {
                @Override
                public Ranges deserialize(DataInputPlus in, Range[] ranges)
                {
                    return Ranges.ofSortedAndDeoverlapped(ranges);
                }
            };

            this.rangeArray = new AbstractRangesSerializer<>()
            {
                @Override Range[] getArray(Range[] ranges) { return ranges; }
                @Override public Range[] deserialize(DataInputPlus in, Range[] ranges) { return ranges; }
            };

            this.partialRangeRoute = new AbstractRangeRouteSerializer<>()
            {
                @Override
                PartialRangeRoute construct(RoutingKey homeKey, Range[] rs)
                {
                    return PartialRangeRoute.SerializationSupport.create(homeKey, rs);
                }
            };

            this.fullRangeRoute = new AbstractRangeRouteSerializer<>()
            {
                @Override
                FullRangeRoute construct(RoutingKey homeKey, Range[] Ranges)
                {
                    return FullRangeRoute.SerializationSupport.create(homeKey, Ranges);
                }
            };

            Function<TinyEnumSet<UnseekablesKind>, AbstractRoutablesSerializer<?>> factory = (a) -> new AbstractRoutablesSerializer<>(a, routingKeys, partialKeyRoute, fullKeyRoute, ranges, partialRangeRoute, fullRangeRoute);

            this.route = (AbstractRoutablesSerializer<Route<?>>) factory.apply(TinyEnumSet.of(UnseekablesKind.PartialKeyRoute, UnseekablesKind.FullKeyRoute, UnseekablesKind.PartialRangeRoute, UnseekablesKind.FullRangeRoute));
            this.nullableRoute = NullableSerializer.wrap(route);

            this.partialRoute = (AbstractRoutablesSerializer<PartialRoute<?>>) factory.apply(TinyEnumSet.of(UnseekablesKind.PartialKeyRoute, UnseekablesKind.PartialRangeRoute));
            this.fullRoute = (AbstractRoutablesSerializer<FullRoute<?>>) factory.apply(TinyEnumSet.of(UnseekablesKind.FullKeyRoute, UnseekablesKind.FullRangeRoute));
            this.nullableFullRoute = NullableSerializer.wrap(fullRoute);

            this.unseekables = (AbstractRoutablesSerializer<Unseekables<?>>) factory.apply(TinyEnumSet.allOf(UnseekablesKind.class));
            this.participants = (AbstractRoutablesSerializer<Participants<?>>) factory.apply(TinyEnumSet.allOf(UnseekablesKind.class));

            this.nullableParticipants = NullableSerializer.wrap(participants);
            this.seekables = new AbstractSeekablesSerializer(keys, ranges);
        }
    }

    public static class AbstractRoutablesSerializer<RS extends Unseekables<?>> implements UnversionedSerializer<RS>
    {
        final TinyEnumSet<UnseekablesKind> permitted;
        final AbstractKeyRoutablesSerializer<RoutingKeys> routingKeys;
        final AbstractKeyRoutablesSerializer<PartialKeyRoute> partialKeyRoute;
        final AbstractKeyRoutablesSerializer<FullKeyRoute> fullKeyRoute;
        final AbstractRangeRoutablesSerializer<Ranges> ranges;
        final AbstractRangeRoutablesSerializer<PartialRangeRoute> partialRangeRoute;
        final AbstractRangeRoutablesSerializer<FullRangeRoute> fullRangeRoute;

        protected AbstractRoutablesSerializer(TinyEnumSet<UnseekablesKind> permitted,
                                              AbstractKeyRoutablesSerializer<RoutingKeys> routingKeys,
                                              AbstractKeyRoutablesSerializer<PartialKeyRoute> partialKeyRoute,
                                              AbstractKeyRoutablesSerializer<FullKeyRoute> fullKeyRoute,
                                              AbstractRangeRoutablesSerializer<Ranges> ranges,
                                              AbstractRangeRoutablesSerializer<PartialRangeRoute> partialRangeRoute,
                                              AbstractRangeRoutablesSerializer<FullRangeRoute> fullRangeRoute)
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
        public void serialize(RS t, DataOutputPlus out) throws IOException
        {
            UnseekablesKind kind = t.kind();
            Invariants.requireArgument(permitted.contains(kind));

            switch (kind)
            {
                default: throw new AssertionError();
                case RoutingKeys:
                    out.writeByte(1);
                    routingKeys.serialize((RoutingKeys)t, out);
                    break;
                case PartialKeyRoute:
                    out.writeByte(2);
                    partialKeyRoute.serialize((PartialKeyRoute)t, out);
                    break;
                case FullKeyRoute:
                    out.writeByte(3);
                    fullKeyRoute.serialize((FullKeyRoute)t, out);
                    break;
                case RoutingRanges:
                    out.writeByte(4);
                    ranges.serialize((Ranges)t, out);
                    break;
                case PartialRangeRoute:
                    out.writeByte(5);
                    partialRangeRoute.serialize((PartialRangeRoute)t, out);
                    break;
                case FullRangeRoute:
                    out.writeByte(6);
                    fullRangeRoute.serialize((FullRangeRoute)t, out);
                    break;
            }
        }

        public void serializeSubset(RS t, Unseekables<?> superset, DataOutputPlus out) throws IOException
        {
            UnseekablesKind kind = t.kind();
            Invariants.requireArgument(permitted.contains(kind));

            switch (kind)
            {
                default: throw new AssertionError();
                case RoutingKeys:
                    out.writeByte(1);
                    routingKeys.serializeSubset((RoutingKeys)t, superset, out);
                    break;
                case PartialKeyRoute:
                    out.writeByte(2);
                    partialKeyRoute.serializeSubset((PartialKeyRoute)t, superset, out);
                    break;
                case FullKeyRoute:
                    out.writeByte(3);
                    fullKeyRoute.serializeSubset((FullKeyRoute)t, superset, out);
                    break;
                case RoutingRanges:
                    out.writeByte(4);
                    ranges.serializeSubset((Ranges)t, superset, out);
                    break;
                case PartialRangeRoute:
                    out.writeByte(5);
                    partialRangeRoute.serializeSubset((PartialRangeRoute)t, superset, out);
                    break;
                case FullRangeRoute:
                    out.writeByte(6);
                    fullRangeRoute.serializeSubset((FullRangeRoute)t, superset, out);
                    break;
            }
        }

        @Override
        public RS deserialize(DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            UnseekablesKind kind;
            RS result;
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1, 2, 3, 4, 5 or 6; received " + b);
                case 1: kind = UnseekablesKind.RoutingKeys; result = (RS)routingKeys.deserialize(in); break;
                case 2: kind = UnseekablesKind.PartialKeyRoute; result = (RS)partialKeyRoute.deserialize(in); break;
                case 3: kind = UnseekablesKind.FullKeyRoute; result = (RS)fullKeyRoute.deserialize(in); break;
                case 4: kind = UnseekablesKind.RoutingRanges; result = (RS)ranges.deserialize(in); break;
                case 5: kind = UnseekablesKind.PartialRangeRoute; result = (RS)partialRangeRoute.deserialize(in); break;
                case 6: kind = UnseekablesKind.FullRangeRoute; result = (RS)fullRangeRoute.deserialize(in); break;
            }
            Invariants.require(permitted.contains(kind));
            return result;
        }

        public RS deserializeSubset(Unseekables<?> superset, DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            UnseekablesKind kind;
            RS result;
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1, 2, 3, 4 or 5; received " + b);
                case 1: kind = UnseekablesKind.RoutingKeys; result = (RS)routingKeys.deserializeSubset((AbstractUnseekableKeys) superset, in); break;
                case 2: kind = UnseekablesKind.PartialKeyRoute; result = (RS)partialKeyRoute.deserializeSubset((AbstractUnseekableKeys) superset, in); break;
                case 3: kind = UnseekablesKind.FullKeyRoute; result = (RS)fullKeyRoute.deserializeSubset((AbstractUnseekableKeys) superset, in); break;
                case 4: kind = UnseekablesKind.RoutingRanges; result = (RS)ranges.deserializeSubset((AbstractRanges) superset, in); break;
                case 5: kind = UnseekablesKind.PartialRangeRoute; result = (RS)partialRangeRoute.deserializeSubset((AbstractRanges) superset, in); break;
                case 6: kind = UnseekablesKind.FullRangeRoute; result = (RS)fullRangeRoute.deserializeSubset((AbstractRanges) superset, in); break;
            }
            Invariants.require(permitted.contains(kind));
            return result;
        }

        public void skip(DataInputPlus in) throws IOException
        {
            countAndSkip(in);
        }

        public void skip(UnseekablesKind kind, DataInputPlus in) throws IOException
        {
            countAndSkip(kind, in);
        }

        // return number of elements skipped
        public int countAndSkip(DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1, 2, 3, 4 or 5; received " + b);
                case 1: return routingKeys.countAndSkip(in);
                case 2: return partialKeyRoute.countAndSkip(in);
                case 3: return fullKeyRoute.countAndSkip(in);
                case 4: return ranges.countAndSkip(in);
                case 5: return partialRangeRoute.countAndSkip(in);
                case 6: return fullRangeRoute.countAndSkip(in);
            }
        }

        public int countAndSkip(UnseekablesKind kind, DataInputPlus in) throws IOException
        {
            switch (kind)
            {
                default: throw UnhandledEnum.unknown(kind);
                case RoutingKeys: return routingKeys.countAndSkip(in);
                case PartialKeyRoute: return partialKeyRoute.countAndSkip(in);
                case FullKeyRoute: return fullKeyRoute.countAndSkip(in);
                case RoutingRanges: return ranges.countAndSkip(in);
                case PartialRangeRoute: return partialRangeRoute.countAndSkip(in);
                case FullRangeRoute: return fullRangeRoute.countAndSkip(in);
            }
        }

        public Unseekables.UnseekablesKind readKind(DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1, 2, 3, 4 or 5; received " + b);
                case 1: return UnseekablesKind.RoutingKeys;
                case 2: return UnseekablesKind.PartialKeyRoute;
                case 3: return UnseekablesKind.FullKeyRoute;
                case 4: return UnseekablesKind.RoutingRanges;
                case 5: return UnseekablesKind.PartialRangeRoute;
                case 6: return UnseekablesKind.FullRangeRoute;
            }
        }

        public void skipSubset(int supersetCount, DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1, 2, 3, 4 or 5; received " + b);
                case 1: routingKeys.skipSubset(supersetCount, in); break;
                case 2: partialKeyRoute.skipSubset(supersetCount, in); break;
                case 3: fullKeyRoute.skipSubset(supersetCount, in); break;
                case 4: ranges.skipSubset(supersetCount, in); break;
                case 5: partialRangeRoute.skipSubset(supersetCount, in); break;
                case 6: fullRangeRoute.skipSubset(supersetCount, in); break;
            }
        }

        @Override
        public long serializedSize(RS t)
        {
            switch (t.kind())
            {
                default: throw new AssertionError();
                case RoutingKeys:
                    return 1 + routingKeys.serializedSize((RoutingKeys)t);
                case PartialKeyRoute:
                    return 1 + partialKeyRoute.serializedSize((PartialKeyRoute)t);
                case FullKeyRoute:
                    return 1 + fullKeyRoute.serializedSize((FullKeyRoute)t);
                case RoutingRanges:
                    return 1 + ranges.serializedSize((Ranges)t);
                case PartialRangeRoute:
                    return 1 + partialRangeRoute.serializedSize((PartialRangeRoute)t);
                case FullRangeRoute:
                    return 1 + fullRangeRoute.serializedSize((FullRangeRoute)t);
            }
        }

        public long serializedSubsetSize(RS t, Unseekables<?> superset)
        {
            switch (t.kind())
            {
                default: throw new AssertionError();
                case RoutingKeys:
                    return 1 + routingKeys.serializedSubsetSize((RoutingKeys)t, superset);
                case PartialKeyRoute:
                    return 1 + partialKeyRoute.serializedSubsetSize((PartialKeyRoute)t, superset);
                case FullKeyRoute:
                    return 1 + fullKeyRoute.serializedSubsetSize((FullKeyRoute)t, superset);
                case RoutingRanges:
                    return 1 + ranges.serializedSubsetSize((Ranges)t, superset);
                case PartialRangeRoute:
                    return 1 + partialRangeRoute.serializedSubsetSize((PartialRangeRoute)t, superset);
                case FullRangeRoute:
                    return 1 + fullRangeRoute.serializedSubsetSize((FullRangeRoute)t, superset);
            }
        }
    }

    public static final UnversionedSerializer<Seekable> seekable = new UnversionedSerializer<>()
    {
        @Override
        public void serialize(Seekable seekable, DataOutputPlus out) throws IOException
        {
            switch (seekable.domain())
            {
                default: throw new AssertionError();
                case Key:
                    out.writeByte(0);
                    PartitionKey.serializer.serialize((PartitionKey) seekable, out);
                    break;
                case Range:
                    out.writeByte(1);
                    TokenRange.serializer.serialize((TokenRange) seekable, out);
                    break;
            }
        }

        @Override
        public Seekable deserialize(DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1 or 2, received " + b);
                case 0: return PartitionKey.serializer.deserialize(in);
                case 1: return TokenRange.serializer.deserialize(in);
            }
        }

        @Override
        public void skip(DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1 or 2, received " + b);
                case 0: PartitionKey.serializer.skip(in); break;
                case 1: TokenRange.serializer.skip(in); break;
            }
        }

        @Override
        public long serializedSize(Seekable seekable)
        {
            switch (seekable.domain())
            {
                default: throw new AssertionError();
                case Key:
                    return 1 + PartitionKey.serializer.serializedSize((PartitionKey) seekable);
                case Range:
                    return 1 + TokenRange.serializer.serializedSize((TokenRange) seekable);
            }
        }
    };

    public static class AbstractSeekablesSerializer implements UnversionedSerializer<Seekables<?, ?>>
    {
        final UnversionedSerializer<Keys> keys;
        final AbstractRangeRoutablesSerializer<Ranges> ranges;

        public AbstractSeekablesSerializer(UnversionedSerializer<Keys> keys, AbstractRangeRoutablesSerializer<Ranges> ranges)
        {
            this.keys = keys;
            this.ranges = ranges;
        }

        @Override
        public void serialize(Seekables<?, ?> t, DataOutputPlus out) throws IOException
        {
            switch (t.domain())
            {
                default: throw new AssertionError();
                case Key:
                    out.writeByte(1);
                    keys.serialize((Keys)t, out);
                    break;
                case Range:
                    out.writeByte(2);
                    ranges.serialize((Ranges)t, out);
                    break;
            }
        }

        @Override
        public Seekables<?, ?> deserialize(DataInputPlus in) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1 or 2, received " + b);
                case 1: return keys.deserialize(in);
                case 2: return ranges.deserialize(in);
            }
        }

        @Override
        public long serializedSize(Seekables<?, ?> t)
        {
            switch (t.domain())
            {
                default: throw new AssertionError();
                case Key:
                    return 1 + keys.serializedSize((Keys)t);
                case Range:
                    return 1 + ranges.serializedSize((Ranges)t);
            }
        }
    }

    // this serializer is designed to permits using the collection in its serialized form with minimal in-memory state.
    // it also saves some memory by avoiding duplicating prefixes (which happens to also assist faster lookups)
    public abstract static class AbstractKeysSerializer<K extends RoutableKey, KS extends AbstractKeys<K>> implements UnversionedSerializer<KS>
    {
        final AccordKeySerializer<K> keySerializer;
        final IntFunction<K[]> allocate;

        public AbstractKeysSerializer(AccordKeySerializer<K> keySerializer, IntFunction<K[]> allocate)
        {
            this.keySerializer = keySerializer;
            this.allocate = allocate;
        }

        @Override
        public void serialize(KS keys, DataOutputPlus out) throws IOException
        {
            out.writeUnsignedVInt32(keys.size());
            for (int i=0, mi=keys.size(); i<mi; i++)
                keySerializer.serialize(keys.get(i), out);
        }

        abstract KS deserialize(DataInputPlus in, K[] keys) throws IOException;

        public void skip(DataInputPlus in) throws IOException
        {
            int count = in.readUnsignedVInt32();
            for (int i = 0; i < count ; i++)
                keySerializer.deserialize(in);
        }

        @Override
        public KS deserialize(DataInputPlus in) throws IOException
        {
            K[] keys = allocate.apply(in.readUnsignedVInt32());
            for (int i=0; i<keys.length; i++)
                keys[i] = keySerializer.deserialize(in);
            return deserialize(in, keys);
        }

        @Override
        public long serializedSize(KS keys)
        {
            long size = TypeSizes.sizeofUnsignedVInt(keys.size());
            for (int i=0, mi=keys.size(); i<mi; i++)
                size += keySerializer.serializedSize(keys.get(i));
            return size;
        }
    }

    // this serializer is designed to permits using the collection in its serialized form with minimal in-memory state.
    // it also saves some memory by avoiding duplicating prefixes (which happens to also assist faster lookups)
    public abstract static class AbstractSearchableSerializer<R extends Routable, RS> extends IVersionedWithKeysSerializer.AbstractWithKeysSerializer implements UnversionedSerializer<RS>
    {
        final IntFunction<R[]> allocate;

        public AbstractSearchableSerializer(IntFunction<R[]> allocate)
        {
            this.allocate = allocate;
        }

        private int serializedSizeOfPrefix(Object prefix)
        {
            return routingKey.serializedSizeOfPrefix(prefix);
        }

        private void serializePrefix(Object prefix, DataOutputPlus out) throws IOException
        {
            routingKey.serializePrefix(prefix, out);
        }

        private Object deserializePrefix(DataInputPlus in) throws IOException
        {
            return routingKey.deserializePrefix(in);
        }

        // if we store Ranges, we have twice as many indexes
        abstract int recordCountToLengthCount(int recordCount);
        abstract int fixedKeyLengthForPrefix(Object prefix);
        abstract int serializedSizeWithoutPrefix(R routable);
        abstract void serializeWithoutPrefixOrLength(R routable, DataOutputPlus out) throws IOException;
        abstract void serializeOffsets(R[] keys, int start, int end, DataOutputPlus out) throws IOException;

        abstract R deserializeWithPrefix(Object prefix, int length, DataInputPlus in) throws IOException;
        abstract R deserializeWithPrefix(Object prefix, int lengthIndex, int[] lengths, DataInputPlus in) throws IOException;

        abstract R[] getArray(RS routables);
        abstract RS deserialize(DataInputPlus in, R[] keys) throws IOException;

        @Override
        public long serializedSize(RS routables)
        {
            return serializedArraySize(getArray(routables));
        }

        protected long serializedArraySize(R[] rs)
        {
            int count = rs.length;
            long size = TypeSizes.sizeofUnsignedVInt(count);
            if (count == 0)
                return size;

            Object prefix = rs[0].prefix();
            int prefixStart = 0;
            for (int i = 1 ; i <= count ; ++i)
            {
                Object nextPrefix = null;
                if (i < count)
                {
                    nextPrefix = rs[i].prefix();
                    if (Objects.equals(prefix, nextPrefix))
                        continue;
                }

                size += TypeSizes.sizeofUnsignedVInt(count - i);
                size += serializedSizeOfPrefix(prefix);
                int fixedLength = fixedKeyLengthForPrefix(prefix);
                if (fixedLength < 0)
                {
                    size += 4L * recordCountToLengthCount(i - prefixStart);
                    size += serializedSizeOfKeysWithoutPrefix(rs, prefixStart, i);
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
        public void serialize(RS keys, DataOutputPlus out) throws IOException
        {
            serializeArray(getArray(keys), out);
        }

        public void serializeArray(R[] rs, DataOutputPlus out) throws IOException
        {
            int size = rs.length;
            out.writeUnsignedVInt32(size);
            if (size == 0)
                return;

            Object prefix = rs[0].prefix();
            int prefixStart = 0;
            for (int i = 1 ; i <= size ; ++i)
            {
                Object nextPrefix = null;
                if (i < size)
                {
                    nextPrefix = rs[i].prefix();
                    if (Objects.equals(prefix, nextPrefix))
                        continue;
                }

                out.writeUnsignedVInt32(size - i);
                serializePrefix(prefix, out);
                int fixedLength = fixedKeyLengthForPrefix(prefix);
                if (fixedLength < 0)
                    serializeOffsets(rs, prefixStart, i, out);
                serializeKeysWithoutPrefix(rs, prefixStart, i, out);
                prefixStart = i;
                prefix = nextPrefix;
            }
        }

        private long serializedSizeOfKeysWithoutPrefix(R[] keys, int start, int end)
        {
            long size = 0;
            for (int i = start; i < end; ++i)
                size += serializedSizeWithoutPrefix(keys[i]);
            return size;
        }

        private void serializeKeysWithoutPrefix(R[] rs, int start, int end, DataOutputPlus out) throws IOException
        {
            for (int i = start; i < end; ++i)
                serializeWithoutPrefixOrLength(rs[i], out);
        }

        public void skip(DataInputPlus in) throws IOException
        {
            countAndSkip(in);
        }

        // return number of elements skipped
        public int countAndSkip(DataInputPlus in) throws IOException
        {
            int remaining = in.readUnsignedVInt32();
            if (remaining == 0)
                return 0;

            int total = 0;
            while (remaining > 0)
            {
                int count = remaining - in.readUnsignedVInt32();
                remaining -= count;
                Object prefix = deserializePrefix(in);
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
                total += count;
            }
            return total;
        }

        public void skipSubset(int supersetCount, DataInputPlus in) throws IOException
        {
            skipSubsetInternal(supersetCount, in);
        }

        @Override
        public RS deserialize(DataInputPlus in) throws IOException
        {
            int remaining = in.readUnsignedVInt32();
            R[] out = allocate.apply(remaining);
            int outCount = 0;
            while (remaining > 0)
            {
                int count = remaining - in.readUnsignedVInt32();
                remaining -= count;
                Object prefix = deserializePrefix(in);
                int fixedLength = fixedKeyLengthForPrefix(prefix);
                if (fixedLength >= 0)
                {
                    for (int i = 0 ; i < count ; ++i)
                        out[outCount++] = deserializeWithPrefix(prefix, fixedLength, in);
                }
                else
                {
                    int lengthCount = recordCountToLengthCount(count);
                    if (lengthCount == 1)
                    {
                        int end = in.readInt();
                        out[outCount++] = deserializeWithPrefix(prefix, end, in);
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
                            out[outCount++] = deserializeWithPrefix(prefix, i, lengths, in);
                        cachedInts().forceDiscard(lengths);
                    }
                }
            }

            return deserialize(in, out);
        }
    }

    // this serializer is designed to permits using the collection in its serialized form with minimal in-memory state.
    // it also saves some memory by avoiding duplicating prefixes (which happens to also assist faster lookups)
    public abstract static class AbstractKeyRoutablesSerializer<KS extends AbstractUnseekableKeys> extends AbstractSearchableSerializer<RoutingKey, KS> implements UnversionedSerializer<KS>
    {
        public AbstractKeyRoutablesSerializer()
        {
            super(RoutingKey[]::new);
        }

        @Override
        RoutingKey[] getArray(KS keys)
        {
            return keys.unsafeKeys();
        }

        @Override
        final int fixedKeyLengthForPrefix(Object prefix)
        {
            return routingKey.fixedKeyLengthForPrefix(prefix);
        }

        @Override
        final int recordCountToLengthCount(int recordCount)
        {
            return recordCount;
        }

        @Override
        final int serializedSizeWithoutPrefix(RoutingKey routable)
        {
            return routingKey.serializedSizeWithoutPrefix(routable);
        }

        @Override
        final void serializeWithoutPrefixOrLength(RoutingKey routable, DataOutputPlus out) throws IOException
        {
            routingKey.serializeWithoutPrefixOrLength(routable, out);
        }

        @Override
        final void serializeOffsets(RoutingKey[] keys, int startIndex, int endIndex, DataOutputPlus out) throws IOException
        {
            int endOffset = 0;
            for (int i = startIndex; i < endIndex; ++i)
            {
                endOffset += serializedSizeWithoutPrefix(keys[i]);
                out.writeInt(endOffset);
            }
        }

        @Override
        final RoutingKey deserializeWithPrefix(Object prefix, int length, DataInputPlus in) throws IOException
        {
            return routingKey.deserializeWithPrefix(prefix, length, in);
        }

        @Override
        final RoutingKey deserializeWithPrefix(Object prefix, int lengthIndex, int[] lengths, DataInputPlus in) throws IOException
        {
            return routingKey.deserializeWithPrefix(prefix, lengths[lengthIndex], in);
        }

        public KS deserializeSubset(AbstractUnseekableKeys superset, DataInputPlus in) throws IOException
        {
            RoutingKey[] keys = deserializeSubset(superset, in, (ks, s) -> ks == null ? s.unsafeKeys() : ks, RoutingKey[]::new);
            return deserialize(in, keys);
        }

        public long serializedSubsetSize(KS keys, Routables<?> superset)
        {
            return serializedSubsetSizeInternal(keys, superset);
        }

        public void serializeSubset(KS keys, Routables<?> superset, DataOutputPlus out) throws IOException
        {
            serializeSubsetInternal(keys, superset, out);
        }
    }

    public abstract static class AbstractKeyRouteSerializer<KS extends KeyRoute> extends AbstractKeyRoutablesSerializer<KS>
    {
        public AbstractKeyRouteSerializer()
        {
            super();
        }

        abstract KS construct(RoutingKey homeKey, RoutingKey[] keys);

        @Override
        KS deserialize(DataInputPlus in, RoutingKey[] keys) throws IOException
        {
            int i = in.readUnsignedVInt32();
            RoutingKey homeKey = i == 0 ? routingKey.deserialize(in) : keys[i - 1];
            return construct(homeKey, keys);
        }

        @Override
        public int countAndSkip(DataInputPlus in) throws IOException
        {
            int count = super.countAndSkip(in);
            completeSkip(in);
            return count;
        }

        @Override
        public void skipSubset(int supersetCount, DataInputPlus in) throws IOException
        {
            skipSubsetInternal(supersetCount, in);
            completeSkip(in);
        }

        @Override
        public void serialize(KS route, DataOutputPlus out) throws IOException
        {
            super.serialize(route, out);
            completeSerialize(route, out);
        }

        @Override
        public void serializeSubset(KS route, Routables<?> superset, DataOutputPlus out) throws IOException
        {
            super.serializeSubset(route, superset, out);
            completeSerialize(route, out);
        }

        @Override
        public long serializedSize(KS route)
        {
            return super.serializedSize(route)
                   + completeSerializedSize(route);
        }

        @Override
        public long serializedSubsetSize(KS route, Routables<?> superset)
        {
            return super.serializedSubsetSize(route, superset)
                   + completeSerializedSize(route);
        }

        private void completeSerialize(KS route, DataOutputPlus out) throws IOException
        {
            int i = route.indexOf(route.homeKey());
            out.writeUnsignedVInt32(Math.max(0, 1 + i));
            if (i < 0) routingKey.serialize(route.homeKey, out);
        }

        private void completeSkip(DataInputPlus in) throws IOException
        {
            int i = in.readUnsignedVInt32();
            if (i == 0) routingKey.skip(in);
        }

        private long completeSerializedSize(KS route)
        {
            int i = route.indexOf(route.homeKey());
            long size = TypeSizes.sizeofUnsignedVInt(Math.max(0, 1 + i));
            if (i < 0) size += routingKey.serializedSize(route.homeKey);
            return size;
        }
    }

    public abstract static class AbstractRangesSerializer<RS> extends AbstractSearchableSerializer<Range, RS> implements UnversionedSerializer<RS>
    {
        public AbstractRangesSerializer()
        {
            super(Range[]::new);
        }

        @Override
        int fixedKeyLengthForPrefix(Object prefix)
        {
            return routingKey.fixedKeyLengthForPrefix(prefix) * 2;
        }

        @Override
        int recordCountToLengthCount(int recordCount)
        {
            return recordCount * 2;
        }

        @Override
        final int serializedSizeWithoutPrefix(Range range)
        {
            return routingKey.serializedSizeWithoutPrefix(range.start())
                   + routingKey.serializedSizeWithoutPrefix(range.end());
        }

        @Override
        final void serializeWithoutPrefixOrLength(Range key, DataOutputPlus out) throws IOException
        {
            routingKey.serializeWithoutPrefixOrLength(key.start(), out);
            routingKey.serializeWithoutPrefixOrLength(key.end(), out);
        }

        @Override
        final void serializeOffsets(Range[] ranges, int startIndex, int endIndex, DataOutputPlus out) throws IOException
        {
            int endOffset = 0;
            for (int i = startIndex; i < endIndex; ++i)
            {
                Range r = ranges[i];
                endOffset += routingKey.serializedSizeWithoutPrefix(r.start());
                out.writeInt(endOffset);
                endOffset += routingKey.serializedSizeWithoutPrefix(r.end());
                out.writeInt(endOffset);
            }
        }

        @Override
        final Range deserializeWithPrefix(Object prefix, int length, DataInputPlus in) throws IOException
        {
            RoutingKey start = routingKey.deserializeWithPrefix(prefix, length/2, in);
            RoutingKey end = routingKey.deserializeWithPrefix(prefix, length/2, in);
            return start.rangeFactory().newRange(start, end);
        }

        @Override
        final Range deserializeWithPrefix(Object prefix, int lengthIndex, int[] lengths, DataInputPlus in) throws IOException
        {
            RoutingKey start = routingKey.deserializeWithPrefix(prefix, lengths[lengthIndex * 2], in);
            RoutingKey end = routingKey.deserializeWithPrefix(prefix, lengths[lengthIndex * 2 + 1], in);
            return start.rangeFactory().newRange(start, end);
        }
    }

    public abstract static class AbstractRangeRoutablesSerializer<RS extends AbstractRanges> extends AbstractRangesSerializer<RS> implements UnversionedSerializer<RS>
    {
        @Override
        Range[] getArray(RS ranges)
        {
            return ranges.unsafeRanges();
        }

        public long serializedSubsetSize(RS ranges, Routables<?> superset)
        {
            return serializedSubsetSizeInternal(ranges, superset);
        }

        public void serializeSubset(RS ranges, Routables<?> superset, DataOutputPlus out) throws IOException
        {
            serializeSubsetInternal(ranges, superset, out);
        }

        public RS deserializeSubset(AbstractRanges superset, DataInputPlus in) throws IOException
        {
            Range[] ranges = deserializeSubset(superset, in, (rs, s) -> rs == null ? s.unsafeRanges() : rs, Range[]::new);
            return deserialize(in, ranges);
        }
    }

    public abstract static class AbstractRangeRouteSerializer<RS extends RangeRoute> extends AbstractRangeRoutablesSerializer<RS>
    {
        public AbstractRangeRouteSerializer()
        {
            super();
        }

        abstract RS construct(RoutingKey homeKey, Range[] ranges);

        @Override
        RS deserialize(DataInputPlus in, Range[] ranges) throws IOException
        {
            RoutingKey homeKey = routingKey.deserialize(in);
            return construct(homeKey, ranges);
        }

        @Override
        public int countAndSkip(DataInputPlus in) throws IOException
        {
            int count = super.countAndSkip(in);
            routingKey.skip(in);
            return count;
        }

        @Override
        public void skipSubset(int supersetCount, DataInputPlus in) throws IOException
        {
            super.skipSubset(supersetCount, in);
            routingKey.skip(in);
        }

        @Override
        public void serialize(RS route, DataOutputPlus out) throws IOException
        {
            super.serialize(route, out);
            routingKey.serialize(route.homeKey, out);
        }

        @Override
        public void serializeSubset(RS route, Routables<?> superset, DataOutputPlus out) throws IOException
        {
            super.serializeSubset(route, superset, out);
            routingKey.serialize(route.homeKey, out);
        }

        @Override
        public long serializedSize(RS route)
        {
            return super.serializedSize(route)
                   + routingKey.serializedSize(route.homeKey);
        }

        @Override
        public long serializedSubsetSize(RS route, Routables<?> superset)
        {
            return super.serializedSubsetSize(route, superset) + routingKey.serializedSize(route.homeKey);
        }
    }
}
