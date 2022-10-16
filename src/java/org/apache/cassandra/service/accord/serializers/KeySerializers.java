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
import java.util.function.IntFunction;

import accord.api.Key;
import accord.api.RoutingKey;
import accord.primitives.AbstractKeys;
import accord.primitives.AbstractRoute;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import accord.primitives.PartialRoute;
import accord.primitives.Route;
import accord.primitives.RoutingKeys;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

public class KeySerializers
{
    private KeySerializers() {}

    public static final IVersionedSerializer<Key> key = (IVersionedSerializer<Key>) (IVersionedSerializer<?>) AccordKey.serializer;
    public static final IVersionedSerializer<RoutingKey> routingKey = (IVersionedSerializer<RoutingKey>) (IVersionedSerializer<?>) AccordRoutingKey.serializer;


    public static final IVersionedSerializer<KeyRanges> ranges = new IVersionedSerializer<KeyRanges>()
    {
        @Override
        public void serialize(KeyRanges ranges, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt(ranges.size());
            for (int i=0, mi=ranges.size(); i<mi; i++)
                TokenRange.serializer.serialize((TokenRange) ranges.get(i), out, version);
        }

        @Override
        public KeyRanges deserialize(DataInputPlus in, int version) throws IOException
        {
            KeyRange[] ranges = new KeyRange[(int)in.readUnsignedVInt()];
            for (int i=0; i<ranges.length; i++)
                ranges[i] = TokenRange.serializer.deserialize(in, version);
            return KeyRanges.ofSortedAndDeoverlapped(ranges);
        }

        @Override
        public long serializedSize(KeyRanges ranges, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(ranges.size());
            for (int i=0, mi=ranges.size(); i<mi; i++)
                size += TokenRange.serializer.serializedSize((TokenRange) ranges.get(i), version);
            return size;
        }
    };

    public static final IVersionedSerializer<Keys> keys = new AbstractKeysSerializer<Key, Keys>(key, Key[]::new)
    {
        @Override Keys deserialize(DataInputPlus in, int version, Key[] keys)
        {
            return Keys.SerializationSupport.create(keys);
        }
    };
    public static final IVersionedSerializer<RoutingKeys> routingKeys = new AbstractKeysSerializer<RoutingKey, RoutingKeys>(routingKey, RoutingKey[]::new)
    {
        @Override RoutingKeys deserialize(DataInputPlus in, int version, RoutingKey[] keys)
        {
            return RoutingKeys.SerializationSupport.create(keys);
        }
    };
    public static final IVersionedSerializer<PartialRoute> partialRoute = new AbstractKeysSerializer<RoutingKey, PartialRoute>(routingKey, RoutingKey[]::new)
    {
        @Override PartialRoute deserialize(DataInputPlus in, int version, RoutingKey[] keys) throws IOException
        {
            KeyRanges covering = ranges.deserialize(in, version);
            RoutingKey homeKey = routingKey.deserialize(in, version);
            return PartialRoute.SerializationSupport.create(covering, homeKey, keys);
        }

        @Override
        public void serialize(PartialRoute keys, DataOutputPlus out, int version) throws IOException
        {
            super.serialize(keys, out, version);
            ranges.serialize(keys.covering, out, version);
            routingKey.serialize(keys.homeKey, out, version);
        }

        @Override
        public long serializedSize(PartialRoute keys, int version)
        {
            return super.serializedSize(keys, version)
                   + ranges.serializedSize(keys.covering, version)
                   + routingKey.serializedSize(keys.homeKey, version);
        }
    };
    public static final IVersionedSerializer<Route> route = new AbstractKeysSerializer<RoutingKey, Route>(routingKey, RoutingKey[]::new)
    {
        @Override Route deserialize(DataInputPlus in, int version, RoutingKey[] keys) throws IOException
        {
            RoutingKey homeKey = routingKey.deserialize(in, version);
            return Route.SerializationSupport.create(homeKey, keys);
        }

        @Override
        public void serialize(Route keys, DataOutputPlus out, int version) throws IOException
        {
            super.serialize(keys, out, version);
            routingKey.serialize(keys.homeKey, out, version);
        }

        @Override
        public long serializedSize(Route keys, int version)
        {
            return super.serializedSize(keys, version)
                 + routingKey.serializedSize(keys.homeKey, version);
        }
    };

    public static final IVersionedSerializer<AbstractRoute> abstractRoute = new IVersionedSerializer<AbstractRoute>()
    {
        @Override
        public void serialize(AbstractRoute t, DataOutputPlus out, int version) throws IOException
        {
            if (t instanceof Route)
            {
                out.writeByte(1);
                route.serialize((Route)t, out, version);
            }
            else
            {
                out.writeByte(2);
                partialRoute.serialize((PartialRoute)t, out, version);
            }
        }

        @Override
        public AbstractRoute deserialize(DataInputPlus in, int version) throws IOException
        {
            byte b = in.readByte();
            switch (b)
            {
                default: throw new IOException("Corrupted input: expected byte 1 or 2, received " + b);
                case 1: return route.deserialize(in, version);
                case 2: return partialRoute.deserialize(in, version);
            }
        }

        @Override
        public long serializedSize(AbstractRoute t, int version)
        {
            if (t instanceof Route)
            {
                return 1 + route.serializedSize((Route)t, version);
            }
            else
            {
                return 1 + partialRoute.serializedSize((PartialRoute)t, version);
            }
        }
    };

    public static abstract class AbstractKeysSerializer<K extends RoutingKey, KS extends AbstractKeys<K, ?>> implements IVersionedSerializer<KS>
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
            out.writeUnsignedVInt(keys.size());
            for (int i=0, mi=keys.size(); i<mi; i++)
                keySerializer.serialize(keys.get(i), out, version);
        }

        abstract KS deserialize(DataInputPlus in, int version, K[] keys) throws IOException;

        @Override
        public KS deserialize(DataInputPlus in, int version) throws IOException
        {
            K[] keys = allocate.apply((int)in.readUnsignedVInt());
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
    };
}
