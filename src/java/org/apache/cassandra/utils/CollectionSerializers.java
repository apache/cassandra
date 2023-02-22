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

package org.apache.cassandra.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;
import javax.annotation.Nonnull;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IPartitionerDependentSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static com.google.common.primitives.Ints.checkedCast;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public class CollectionSerializers
{

    public static <V> void serializeCollection(Collection<V> values, DataOutputPlus out, int version, IVersionedSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(values.size());
        for (V value : values)
            valueSerializer.serialize(value, out, version);
    }

    public static <V> void serializeCollection(Collection<V> values, DataOutputPlus out, int version, IPartitionerDependentSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(values.size());
        for (V value : values)
            valueSerializer.serialize(value, out, version);
    }

    public static <V, L extends List<V>> void serializeList(L values, DataOutputPlus out, int version, IVersionedSerializer<V> valueSerializer) throws IOException
    {
        int size = values.size();
        out.writeUnsignedVInt32(size);
        for (int i = 0 ; i < size ; ++i)
            valueSerializer.serialize(values.get(i), out, version);
    }

    public static <K, V> void serializeMap(Map<K, V> map, DataOutputPlus out, int version, IVersionedSerializer<K> keySerializer, IVersionedSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
        {
            keySerializer.serialize(e.getKey(), out, version);
            valueSerializer.serialize(e.getValue(), out, version);
        }
    }

    public static <K, V> void serializeMap(Map<K, V> map, DataOutputPlus out, int version, IVersionedSerializer<K> keySerializer, IPartitionerDependentSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
        {
            keySerializer.serialize(e.getKey(), out, version);
            valueSerializer.serialize(e.getValue(), out, version);
        }
    }

    public static <V> List<V> deserializeList(DataInputPlus in, int version, IVersionedSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newArrayList());
    }

    public static <V> Set<V> deserializeSet(DataInputPlus in, int version, IVersionedSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newHashSet());
    }

    public static <V> Set<V> deserializeSet(DataInputPlus in, IPartitioner partitioner, int version, IPartitionerDependentSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, partitioner, version, serializer, newHashSet());
    }

    public static <K, V> Map<K,V> deserializeMap(DataInputPlus in, int version, IVersionedSerializer<K> keySerializer, IVersionedSerializer<V> valueSerializer, IntFunction<Map<K,V>> factory) throws IOException
    {
        int size = checkedCast(in.readUnsignedVInt32());
        Map<K,V> result = factory.apply(size);
        while (size-- > 0)
        {
            K key = keySerializer.deserialize(in, version);
            V value = valueSerializer.deserialize(in, version);
            result.put(key, value);
        }
        return result;
    }

    public static <K, V> Map<K,V> deserializeMap(DataInputPlus in, IPartitioner partitioner, int version, IVersionedSerializer<K> keySerializer, IPartitionerDependentSerializer<V> valueSerializer, IntFunction<Map<K,V>> factory) throws IOException
    {
        int size = checkedCast(in.readUnsignedVInt());
        Map<K,V> result = factory.apply(size);
        while (size-- > 0)
        {
            K key = keySerializer.deserialize(in, version);
            V value = valueSerializer.deserialize(in, partitioner, version);
            result.put(key, value);
        }
        return result;
    }

    public static <K, V> Map<K, V> deserializeMap(DataInputPlus in, int version, IVersionedSerializer<K> keySerializer, IVersionedSerializer<V> valueSerializer) throws IOException
    {
        return deserializeMap(in, version, keySerializer, valueSerializer, newHashMap());
    }

    public static <V> long serializedCollectionSize(Collection<V> values, int version, IVersionedSerializer<V> valueSerializer)
    {
        long size = sizeofUnsignedVInt(values.size());
        for (V value : values)
            size += valueSerializer.serializedSize(value, version);
        return size;
    }

    public static <V> long serializedCollectionSize(Collection<V> values, int version, IPartitionerDependentSerializer<V> valueSerializer)
    {
        long size = sizeofUnsignedVInt(values.size());
        for (V value : values)
            size += valueSerializer.serializedSize(value, version);
        return size;
    }

    public static <V, L extends List<V>> long serializedListSize(L values, int version, IVersionedSerializer<V> valueSerializer)
    {
        int items = values.size();
        long size = sizeofUnsignedVInt(items);
        for (int i = 0 ; i < items ; ++i)
            size += valueSerializer.serializedSize(values.get(i), version);
        return size;
    }

    public static <K, V> long serializedMapSize(Map<K, V> map, int version, IVersionedSerializer<K> keySerializer, IVersionedSerializer<V> valueSerializer)
    {
        long size = sizeofUnsignedVInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
            size += keySerializer.serializedSize(e.getKey(), version)
                  + valueSerializer.serializedSize(e.getValue(), version);
        return size;
    }

    public static <K, V> long serializedMapSize(Map<K, V> map, int version, IVersionedSerializer<K> keySerializer, IPartitionerDependentSerializer<V> valueSerializer)
    {
        long size = sizeofUnsignedVInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
            size += keySerializer.serializedSize(e.getKey(), version)
                    + valueSerializer.serializedSize(e.getValue(), version);
        return size;
    }

    public static <V> IntFunction<Set<V>> newHashSet()
    {
        return i -> i == 0 ? Collections.emptySet() : Sets.newHashSetWithExpectedSize(i);
    }

    public static <K, V> IntFunction<Map<K, V>> newHashMap()
    {
        return i -> i == 0 ? Collections.emptyMap() : Maps.newHashMapWithExpectedSize(i);
    }

    public static <V> IntFunction<List<V>> newArrayList()
    {
        return i -> i == 0 ? Collections.emptyList() : new ArrayList<>(i);
    }

    public static int readCollectionSize(DataInputPlus in, int version) throws IOException
    {
        return checkedCast(in.readUnsignedVInt());
    }

    /*
     * Private to push auto-complete to the convenience methods
     * Feel free to make public if there is a weird collection you want to use
     */
    private static <V, C extends Collection<? super V>> C deserializeCollection(DataInputPlus in, int version, IVersionedSerializer<V> serializer, IntFunction<C> factory) throws IOException
    {
        int size = checkedCast(in.readUnsignedVInt32());
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(in, version));
        return result;
    }

    private static <V, C extends Collection<? super V>> C deserializeCollection(DataInputPlus in, IPartitioner partitioner, int version, IPartitionerDependentSerializer<V> serializer, IntFunction<C> factory) throws IOException
    {
        int size = checkedCast(in.readUnsignedVInt());
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(in, partitioner, version));
        return result;
    }


    public static <T> IPartitionerDependentSerializer<Collection<T>> newCollectionSerializer(@Nonnull final IPartitionerDependentSerializer<T> serializer)
    {
        return new IPartitionerDependentSerializer<Collection<T>>()
        {
            @Override
            public void serialize(Collection<T> t, DataOutputPlus out, int version) throws IOException
            {
                serializeCollection(t, out, version, serializer);
            }

            @Override
            public Collection<T> deserialize(DataInputPlus in, IPartitioner p, int version) throws IOException
            {
                return deserializeCollection(in, p, version, serializer, newArrayList());
            }

            @Override
            public long serializedSize(Collection<T> t, int version)
            {
                return serializedCollectionSize(t, version, serializer);
            }
        };
    }
}
