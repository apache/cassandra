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

import accord.api.RoutingKey;
import accord.primitives.Ranges;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.ReducingRangeMap;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.CollectionSerializers;

public class CommandStoreSerializers
{
    private CommandStoreSerializers() {}

    public static IVersionedSerializer<ReducingRangeMap<Timestamp>> rejectBefore = new IVersionedSerializer<ReducingRangeMap<Timestamp>>()
    {
        public void serialize(ReducingRangeMap<Timestamp> map, DataOutputPlus out, int version) throws IOException
        {
            out.writeBoolean(map.inclusiveEnds());
            int size = map.size();
            out.writeUnsignedVInt32(size);

            for (int i=0; i<size; i++)
            {
                KeySerializers.routingKey.serialize(map.key(i), out, version);
                CommandSerializers.timestamp.serialize(map.value(i), out, version);
            }
            CommandSerializers.timestamp.serialize(map.value(size), out, version);
        }

        public ReducingRangeMap<Timestamp> deserialize(DataInputPlus in, int version) throws IOException
        {
            boolean inclusiveEnds = in.readBoolean();
            int size = in.readUnsignedVInt32();
            RoutingKey[] keys = new RoutingKey[size];
            Timestamp[] values = new Timestamp[size + 1];
            for (int i=0; i<size; i++)
            {
                keys[i] = KeySerializers.routingKey.deserialize(in, version);
                values[i] = CommandSerializers.timestamp.deserialize(in, version);
            }
            values[size] = CommandSerializers.timestamp.deserialize(in, version);
            return ReducingRangeMap.SerializerSupport.create(inclusiveEnds, keys, values);
        }

        public long serializedSize(ReducingRangeMap<Timestamp> map, int version)
        {
            long size = TypeSizes.BOOL_SIZE;
            size += TypeSizes.sizeofUnsignedVInt(size);
            int mapSize = map.size();
            for (int i=0; i<mapSize; i++)
            {
                size += KeySerializers.routingKey.serializedSize(map.key(i), version);
                size += CommandSerializers.timestamp.serializedSize(map.value(i), version);
            }
            size += CommandSerializers.timestamp.serializedSize(map.value(mapSize), version);

            return size;
        }
    };

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
