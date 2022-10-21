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

import accord.api.Key;
import accord.primitives.KeyRange;
import accord.primitives.KeyRanges;
import accord.primitives.Keys;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordKey;

public class KeySerializers
{
    private KeySerializers() {}

    public static final IVersionedSerializer<Key> key = (IVersionedSerializer<Key>) (IVersionedSerializer<?>) AccordKey.serializer;

    public static final IVersionedSerializer<Keys> keys = new IVersionedSerializer<Keys>()
    {
        @Override
        public void serialize(Keys keys, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(keys.size());
            for (int i=0, mi=keys.size(); i<mi; i++)
                key.serialize(keys.get(i), out, version);
        }

        @Override
        public Keys deserialize(DataInputPlus in, int version) throws IOException
        {
            Key[] keys = new Key[in.readInt()];
            for (int i=0; i<keys.length; i++)
                keys[i] = key.deserialize(in, version);
            return Keys.of(keys);
        }

        @Override
        public long serializedSize(Keys keys, int version)
        {
            long size = TypeSizes.sizeof(keys.size());
            for (int i=0, mi=keys.size(); i<mi; i++)
                size += key.serializedSize(keys.get(i), version);
            return size;
        }
    };

    public static final IVersionedSerializer<KeyRanges> ranges = new IVersionedSerializer<KeyRanges>()
    {
        @Override
        public void serialize(KeyRanges ranges, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(ranges.size());
            for (int i=0, mi=ranges.size(); i<mi; i++)
                TokenRange.serializer.serialize((TokenRange) ranges.get(i), out, version);
        }

        @Override
        public KeyRanges deserialize(DataInputPlus in, int version) throws IOException
        {
            KeyRange[] ranges = new KeyRange[in.readInt()];
            for (int i=0; i<ranges.length; i++)
                ranges[i] = TokenRange.serializer.deserialize(in, version);
            return KeyRanges.ofSortedAndDeoverlapped(ranges);
        }

        @Override
        public long serializedSize(KeyRanges ranges, int version)
        {
            long size = TypeSizes.sizeof(ranges.size());
            for (int i=0, mi=ranges.size(); i<mi; i++)
                size += TokenRange.serializer.serializedSize((TokenRange) ranges.get(i), version);
            return size;
        }
    };
}
