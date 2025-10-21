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

import accord.utils.SortedArrays.SortedArrayList;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.IPartitionerDependentSerializer;
import org.apache.cassandra.io.AsymmetricParameterisedUnversionedSerializer;
import org.apache.cassandra.io.AsymmetricParameterisedVersionedSerializer;
import org.apache.cassandra.io.AsymmetricVersionedSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.ParameterisedUnversionedSerializer;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public class CollectionSerializers
{
    public static <V> void serializeCollection(Collection<V> values, DataOutputPlus out, UnversionedSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(values.size());
        for (V value : values)
            valueSerializer.serialize(value, out);
    }

    public static <V> void serializeCollection(Collection<V> values, DataOutputPlus out, int version, IVersionedSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(values.size());
        for (V value : values)
            valueSerializer.serialize(value, out, version);
    }

    public static <V, Version> void serializeCollection(Collection<V> values, DataOutputPlus out, Version version, AsymmetricVersionedSerializer<V, ?, Version> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(values.size());
        for (V value : values)
            valueSerializer.serialize(value, out, version);
    }

    public static <V, P, Version> void serializeCollection(Collection<V> values, P p, DataOutputPlus out, Version version, AsymmetricParameterisedVersionedSerializer<V, P, ?, Version> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(values.size());
        for (V value : values)
            valueSerializer.serialize(value, p, out, version);
    }

    public static <V, P> void serializeCollection(Collection<V> values, P p, DataOutputPlus out, AsymmetricParameterisedUnversionedSerializer<V, P, ?> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(values.size());
        for (V value : values)
            valueSerializer.serialize(value, p, out);
    }

    public static <V> void serializeCollection(Collection<V> values, DataOutputPlus out, Version version, MetadataSerializer<V> valueSerializer) throws IOException
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

    public static <V, L extends List<V>> void serializeList(L values, DataOutputPlus out, UnversionedSerializer<V> valueSerializer) throws IOException
    {
        int size = values.size();
        out.writeUnsignedVInt32(size);
        for (int i = 0 ; i < size ; ++i)
            valueSerializer.serialize(values.get(i), out);
    }

    public static <V, P, L extends List<V>> void serializeList(L values, P p, DataOutputPlus out, ParameterisedUnversionedSerializer<V, P> valueSerializer) throws IOException
    {
        int size = values.size();
        out.writeUnsignedVInt32(size);
        for (int i = 0 ; i < size ; ++i)
            valueSerializer.serialize(values.get(i), p, out);
    }

    public static <V, P, Version, L extends List<V>> void serializeList(L values, P p, DataOutputPlus out, Version version, ParameterisedVersionedSerializer<V, P, Version> valueSerializer) throws IOException
    {
        int size = values.size();
        out.writeUnsignedVInt32(size);
        for (int i = 0 ; i < size ; ++i)
            valueSerializer.serialize(values.get(i), p, out, version);
    }

    public static <V, L extends List<V>> void serializeList(L values, DataOutputPlus out, int version, IVersionedSerializer<V> valueSerializer) throws IOException
    {
        int size = values.size();
        out.writeUnsignedVInt32(size);
        for (int i = 0 ; i < size ; ++i)
            valueSerializer.serialize(values.get(i), out, version);
    }

    public static <V, L extends List<V>, Version> void serializeList(L values, DataOutputPlus out, Version version, AsymmetricVersionedSerializer<V, ?, Version> valueSerializer) throws IOException
    {
        int size = values.size();
        out.writeUnsignedVInt32(size);
        for (int i = 0 ; i < size ; ++i)
            valueSerializer.serialize(values.get(i), out, version);
    }

    public static <V, L extends List<V>> void serializeList(L values, DataOutputPlus out, Version version, MetadataSerializer<V> valueSerializer) throws IOException
    {
        int size = values.size();
        out.writeUnsignedVInt32(size);
        for (int i = 0 ; i < size ; ++i)
            valueSerializer.serialize(values.get(i), out, version);
    }

    public static <K, V> void serializeMap(Map<K, V> map, DataOutputPlus out, UnversionedSerializer<K> keySerializer, UnversionedSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
        {
            keySerializer.serialize(e.getKey(), out);
            valueSerializer.serialize(e.getValue(), out);
        }
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

    public static <K, V, Version> void serializeMap(Map<K, V> map, DataOutputPlus out, Version version, AsymmetricVersionedSerializer<K, ?, Version> keySerializer, AsymmetricVersionedSerializer<V, ?, Version> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
        {
            keySerializer.serialize(e.getKey(), out, version);
            valueSerializer.serialize(e.getValue(), out, version);
        }
    }

    public static <K, V> void serializeMap(Map<K, V> map, DataOutputPlus out, Version version, MetadataSerializer<K> keySerializer, MetadataSerializer<V> valueSerializer) throws IOException
    {
        out.writeUnsignedVInt32(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
        {
            keySerializer.serialize(e.getKey(), out, version);
            valueSerializer.serialize(e.getValue(), out, version);
        }
    }

    public static <V extends Comparable<? super V>> SortedArrayList<V> deserializeSortedArrayList(DataInputPlus in, UnversionedSerializer<V> serializer, IntFunction<V[]> allocator) throws IOException
    {
        int size = in.readUnsignedVInt32();
        V[] array = allocator.apply(size);
        for (int i = 0 ; i < array.length ; ++i)
            array[i] = serializer.deserialize(in);
        return new SortedArrayList<>(array);
    }

    public static <V> List<V> deserializeList(DataInputPlus in, UnversionedSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, serializer, newArrayList());
    }

    public static <V> List<V> deserializeList(DataInputPlus in, int version, IVersionedSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newArrayList());
    }

    public static <V, Version> List<V> deserializeList(DataInputPlus in, Version version, AsymmetricVersionedSerializer<?, V, Version> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newArrayList());
    }

    public static <V, P, Version> List<V> deserializeList(P p, DataInputPlus in, Version version, AsymmetricParameterisedVersionedSerializer<?, P, V, Version> serializer) throws IOException
    {
        return deserializeCollection(p, in, version, serializer, newArrayList());
    }

    public static <V, P> List<V> deserializeList(P p, DataInputPlus in, AsymmetricParameterisedUnversionedSerializer<?, P, V> serializer) throws IOException
    {
        return deserializeCollection(p, in, serializer, newArrayList());
    }

    public static <V> List<V> deserializeList(DataInputPlus in, Version version, MetadataSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newArrayList());
    }

    public static <V> List<V> deserializeList(DataInputPlus in, IPartitioner partitioner, int version, IPartitionerDependentSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, partitioner, version, serializer, newArrayList());
    }

    public static <V> Set<V> deserializeSet(DataInputPlus in, UnversionedSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, serializer, newHashSet());
    }

    public static <V> Set<V> deserializeSet(DataInputPlus in, int version, IVersionedSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newHashSet());
    }

    public static <V> Set<V> deserializeSet(DataInputPlus in, IPartitioner partitioner, int version, IPartitionerDependentSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, partitioner, version, serializer, newHashSet());
    }

    public static <V, Version> Set<V> deserializeSet(DataInputPlus in, Version version, AsymmetricVersionedSerializer<?, V, Version> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newHashSet());
    }

    public static <V> Set<V> deserializeSet(DataInputPlus in, Version version, MetadataSerializer<V> serializer) throws IOException
    {
        return deserializeCollection(in, version, serializer, newHashSet());
    }

    public static <K, V, M extends Map<K, V>> M deserializeMap(DataInputPlus in, UnversionedSerializer<K> keySerializer, UnversionedSerializer<V> valueSerializer, IntFunction<M> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        M result = factory.apply(size);
        while (size-- > 0)
        {
            K key = keySerializer.deserialize(in);
            V value = valueSerializer.deserialize(in);
            result.put(key, value);
        }
        return result;
    }

    public static <K, V, M extends Map<K, V>> M deserializeMap(DataInputPlus in, int version, IVersionedSerializer<K> keySerializer, IVersionedSerializer<V> valueSerializer, IntFunction<M> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        M result = factory.apply(size);
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
        int size = in.readUnsignedVInt32();
        Map<K,V> result = factory.apply(size);
        while (size-- > 0)
        {
            K key = keySerializer.deserialize(in, version);
            V value = valueSerializer.deserialize(in, partitioner, version);
            result.put(key, value);
        }
        return result;
    }

    public static <K, V, M extends Map<K, V>, Version> M deserializeMap(DataInputPlus in, Version version, AsymmetricVersionedSerializer<?, K, Version> keySerializer, AsymmetricVersionedSerializer<?, V, Version> valueSerializer, IntFunction<M> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        M result = factory.apply(size);
        while (size-- > 0)
        {
            K key = keySerializer.deserialize(in, version);
            V value = valueSerializer.deserialize(in, version);
            result.put(key, value);
        }
        return result;
    }

    public static <K, V, M extends Map<K, V>> M deserializeMap(DataInputPlus in, Version version, MetadataSerializer<K> keySerializer, MetadataSerializer<V> valueSerializer, IntFunction<M> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        M result = factory.apply(size);
        while (size-- > 0)
        {
            K key = keySerializer.deserialize(in, version);
            V value = valueSerializer.deserialize(in, version);
            result.put(key, value);
        }
        return result;
    }

    public static <K, V> Map<K, V> deserializeMap(DataInputPlus in, UnversionedSerializer<K> keySerializer, UnversionedSerializer<V> valueSerializer) throws IOException
    {
        return deserializeMap(in, keySerializer, valueSerializer, Maps::newHashMapWithExpectedSize);
    }

    public static <K, V> Map<K, V> deserializeMap(DataInputPlus in, int version, IVersionedSerializer<K> keySerializer, IVersionedSerializer<V> valueSerializer) throws IOException
    {
        return deserializeMap(in, version, keySerializer, valueSerializer, Maps::newHashMapWithExpectedSize);
    }

    public static <K, V, Version> Map<K, V> deserializeMap(DataInputPlus in, Version version, AsymmetricVersionedSerializer<?, K, Version> keySerializer, AsymmetricVersionedSerializer<?, V, Version> valueSerializer) throws IOException
    {
        return deserializeMap(in, version, keySerializer, valueSerializer, Maps::newHashMapWithExpectedSize);
    }

    public static <K, V> Map<K, V> deserializeMap(DataInputPlus in, Version version, MetadataSerializer<K> keySerializer, MetadataSerializer<V> valueSerializer) throws IOException
    {
        return deserializeMap(in, version, keySerializer, valueSerializer, Maps::newHashMapWithExpectedSize);
    }

    public static <V> long serializedCollectionSize(Collection<V> values, UnversionedSerializer<V> valueSerializer)
    {
        long size = sizeofUnsignedVInt(values.size());
        for (V value : values)
            size += valueSerializer.serializedSize(value);
        return size;
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

    public static <V, Version> long serializedCollectionSize(Collection<V> values, Version version, AsymmetricVersionedSerializer<V, ?, Version> valueSerializer)
    {
        long size = sizeofUnsignedVInt(values.size());
        for (V value : values)
            size += valueSerializer.serializedSize(value, version);
        return size;
    }

    public static <V, P, Version> long serializedCollectionSize(P p, Collection<V> values, Version version, AsymmetricParameterisedVersionedSerializer<V, P, ?, Version> valueSerializer)
    {
        long size = sizeofUnsignedVInt(values.size());
        for (V value : values)
            size += valueSerializer.serializedSize(value, p, version);
        return size;
    }

    public static <V, P> long serializedCollectionSize(Collection<V> values, P p, AsymmetricParameterisedUnversionedSerializer<V, P, ?> valueSerializer)
    {
        long size = sizeofUnsignedVInt(values.size());
        for (V value : values)
            size += valueSerializer.serializedSize(value, p);
        return size;
    }

    public static <V> long serializedCollectionSize(Collection<V> values, Version version, MetadataSerializer<V> valueSerializer)
    {
        long size = sizeofUnsignedVInt(values.size());
        for (V value : values)
            size += valueSerializer.serializedSize(value, version);
        return size;
    }

    public static <V, L extends List<V>> long serializedListSize(L values, UnversionedSerializer<V> valueSerializer)
    {
        int items = values.size();
        long size = sizeofUnsignedVInt(items);
        for (int i = 0 ; i < items ; ++i)
            size += valueSerializer.serializedSize(values.get(i));
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

    public static <V, L extends List<V>, Version> long serializedListSize(L values, Version version, AsymmetricVersionedSerializer<V, ?, Version> valueSerializer)
    {
        int items = values.size();
        long size = sizeofUnsignedVInt(items);
        for (int i = 0 ; i < items ; ++i)
            size += valueSerializer.serializedSize(values.get(i), version);
        return size;
    }

    public static <V, P, L extends List<V>> long serializedListSize(L values, P p, AsymmetricParameterisedUnversionedSerializer<V, P, ?> valueSerializer)
    {
        int items = values.size();
        long size = sizeofUnsignedVInt(items);
        for (int i = 0 ; i < items ; ++i)
            size += valueSerializer.serializedSize(values.get(i), p);
        return size;
    }

    public static <V, P, L extends List<V>, Version> long serializedListSize(L values, P p, Version version, AsymmetricParameterisedVersionedSerializer<V, P, ?, Version> valueSerializer)
    {
        int items = values.size();
        long size = sizeofUnsignedVInt(items);
        for (int i = 0 ; i < items ; ++i)
            size += valueSerializer.serializedSize(values.get(i), p, version);
        return size;
    }

    public static <V, L extends List<V>> long serializedListSize(L values, Version version, MetadataSerializer<V> valueSerializer)
    {
        int items = values.size();
        long size = sizeofUnsignedVInt(items);
        for (int i = 0 ; i < items ; ++i)
            size += valueSerializer.serializedSize(values.get(i), version);
        return size;
    }

    public static <K, V> long serializedMapSize(Map<K, V> map, UnversionedSerializer<K> keySerializer, UnversionedSerializer<V> valueSerializer)
    {
        long size = sizeofUnsignedVInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
            size += keySerializer.serializedSize(e.getKey())
                    + valueSerializer.serializedSize(e.getValue());
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

    public static <K, V, Version> long serializedMapSize(Map<K, V> map, Version version, AsymmetricVersionedSerializer<K, ?, Version> keySerializer, AsymmetricVersionedSerializer<V, ?, Version> valueSerializer)
    {
        long size = sizeofUnsignedVInt(map.size());
        for (Map.Entry<K, V> e : map.entrySet())
            size += keySerializer.serializedSize(e.getKey(), version)
                    + valueSerializer.serializedSize(e.getValue(), version);
        return size;
    }

    public static <K, V> long serializedMapSize(Map<K, V> map, Version version, MetadataSerializer<K> keySerializer, MetadataSerializer<V> valueSerializer)
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
        return in.readUnsignedVInt32();
    }

    /*
     * Private to push auto-complete to the convenience methods
     * Feel free to make public if there is a weird collection you want to use
     */
    private static <V, C extends Collection<? super V>> C deserializeCollection(DataInputPlus in, UnversionedSerializer<V> serializer, IntFunction<C> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(in));
        return result;
    }

    /*
     * Private to push auto-complete to the convenience methods
     * Feel free to make public if there is a weird collection you want to use
     */
    private static <V, C extends Collection<? super V>> C deserializeCollection(DataInputPlus in, int version, IVersionedSerializer<V> serializer, IntFunction<C> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(in, version));
        return result;
    }

    private static <V, C extends Collection<? super V>> C deserializeCollection(DataInputPlus in, IPartitioner partitioner, int version, IPartitionerDependentSerializer<V> serializer, IntFunction<C> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(in, partitioner, version));
        return result;
    }

    private static <V, C extends Collection<? super V>, Version> C deserializeCollection(DataInputPlus in, Version version, AsymmetricVersionedSerializer<?, V, Version> serializer, IntFunction<C> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(in, version));
        return result;
    }

    private static <V, P, C extends Collection<? super V>, Version> C deserializeCollection(P p, DataInputPlus in, Version version, AsymmetricParameterisedVersionedSerializer<?, P, V, Version> serializer, IntFunction<C> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(p, in, version));
        return result;
    }

    private static <V, P, C extends Collection<? super V>, Version> C deserializeCollection(P p, DataInputPlus in, AsymmetricParameterisedUnversionedSerializer<?, P, V> serializer, IntFunction<C> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(p, in));
        return result;
    }

    private static <V, C extends Collection<? super V>> C deserializeCollection(DataInputPlus in, Version version, MetadataSerializer<V> serializer, IntFunction<C> factory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        C result = factory.apply(size);
        while (size-- > 0)
            result.add(serializer.deserialize(in, version));
        return result;
    }

    public static <V> UnversionedSerializer<List<V>> newListSerializer(UnversionedSerializer<V> itemSerializer)
    {
        return new UnversionedSerializer<List<V>>()
        {
            @Override
            public void serialize(List<V> list, DataOutputPlus out) throws IOException
            {
                serializeList(list, out, itemSerializer);
            }

            @Override
            public List<V> deserialize(DataInputPlus in) throws IOException
            {
                return deserializeList(in, itemSerializer);
            }

            @Override
            public long serializedSize(List<V> t)
            {
                return serializedListSize(t, itemSerializer);
            }
        };
    }

    public static <V> IVersionedSerializer<List<V>> newListSerializer(IVersionedSerializer<V> itemSerializer)
    {
        return new IVersionedSerializer<List<V>>()
        {
            @Override
            public void serialize(List<V> list, DataOutputPlus out, int version) throws IOException
            {
                serializeList(list, out, version, itemSerializer);
            }

            @Override
            public List<V> deserialize(DataInputPlus in, int version) throws IOException
            {
                return deserializeList(in, version, itemSerializer);
            }

            @Override
            public long serializedSize(List<V> t, int version)
            {
                return serializedListSize(t, version, itemSerializer);
            }
        };
    }

    public static <T, Version> VersionedSerializer<List<T>, Version> newListSerializer(@Nonnull final VersionedSerializer<T, Version> serializer)
    {
        return new VersionedSerializer<List<T>, Version>()
        {
            @Override
            public void serialize(List<T> t, DataOutputPlus out, Version version) throws IOException
            {
                serializeCollection(t, out, version, serializer);
            }

            @Override
            public List<T> deserialize(DataInputPlus in, Version version) throws IOException
            {
                return deserializeList(in, version, serializer);
            }

            @Override
            public long serializedSize(List<T> t, Version version)
            {
                return serializedCollectionSize(t, version, serializer);
            }
        };
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
