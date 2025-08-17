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
import java.util.function.IntFunction;

import org.apache.cassandra.io.AsymmetricVersionedSerializer;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public class ArraySerializers
{
    public static <T> void serializeArray(T[] items, DataOutputPlus out, UnversionedSerializer<T> serializer) throws IOException
    {
        out.writeUnsignedVInt32(items.length);
        for (T item : items)
            serializer.serialize(item, out);
    }

    public static <T> void serializeArray(T[] items, DataOutputPlus out, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        out.writeUnsignedVInt32(items.length);
        for (T item : items)
            serializer.serialize(item, out, version);
    }

    public static <T, Version> void serializeArray(T[] items, DataOutputPlus out, Version version, AsymmetricVersionedSerializer<T, ?, Version> serializer) throws IOException
    {
        out.writeUnsignedVInt32(items.length);
        for (T item : items)
            serializer.serialize(item, out, version);
    }

    public static <T, P, Version> void serializeArray(T[] items, P p, DataOutputPlus out, Version version, ParameterisedVersionedSerializer<T, P, Version> serializer) throws IOException
    {
        out.writeUnsignedVInt32(items.length);
        for (T item : items)
            serializer.serialize(item, p, out, version);
    }

    public static <T> T[] deserializeArray(DataInputPlus in, UnversionedSerializer<T> serializer, IntFunction<T[]> arrayFactory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        T[] items = arrayFactory.apply(size);
        for (int i = 0; i < size; i++)
            items[i] = serializer.deserialize(in);
        return items;
    }

    public static <T, P, Version> void skipArray(DataInputPlus in, UnversionedSerializer<T> serializer) throws IOException
    {
        int size = in.readUnsignedVInt32();
        for (int i = 0; i < size; i++)
            serializer.skip(in);
    }

    public static <T> T[] deserializeArray(DataInputPlus in, int version, IVersionedSerializer<T> serializer, IntFunction<T[]> arrayFactory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        T[] items = arrayFactory.apply(size);
        for (int i = 0; i < size; i++)
            items[i] = serializer.deserialize(in, version);
        return items;
    }

    public static <T, Version> T[] deserializeArray(DataInputPlus in, Version version, AsymmetricVersionedSerializer<?, T, Version> serializer, IntFunction<T[]> arrayFactory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        T[] items = arrayFactory.apply(size);
        for (int i = 0; i < size; i++)
            items[i] = serializer.deserialize(in, version);
        return items;
    }

    public static <T, Version> void skipArray(DataInputPlus in, Version version, AsymmetricVersionedSerializer<T, ?, Version> serializer) throws IOException
    {
        int size = in.readUnsignedVInt32();
        for (int i = 0; i < size; i++)
            serializer.skip(in, version);
    }


    public static <T, P, Version> T[] deserializeArray(P p, DataInputPlus in, Version version, ParameterisedVersionedSerializer<T, P, Version> serializer, IntFunction<T[]> arrayFactory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        T[] items = arrayFactory.apply(size);
        for (int i = 0; i < size; i++)
            items[i] = serializer.deserialize(p, in, version);
        return items;
    }

    public static <T, P, Version> void skipArray(P p, DataInputPlus in, Version version, ParameterisedVersionedSerializer<T, P, Version> serializer) throws IOException
    {
        int size = in.readUnsignedVInt32();
        for (int i = 0; i < size; i++)
            serializer.skip(p, in, version);
    }

    public static <T> long serializedArraySize(T[] array, UnversionedSerializer<T> serializer)
    {
        long size = sizeofUnsignedVInt(array.length);
        for (T item : array)
            size += serializer.serializedSize(item);
        return size;
    }

    public static <T> long serializedArraySize(T[] array, int version, IVersionedSerializer<T> serializer)
    {
        long size = sizeofUnsignedVInt(array.length);
        for (T item : array)
            size += serializer.serializedSize(item, version);
        return size;
    }

    public static <T, Version> long serializedArraySize(T[] array, Version version, AsymmetricVersionedSerializer<T, ?, Version> serializer)
    {
        long size = sizeofUnsignedVInt(array.length);
        for (T item : array)
            size += serializer.serializedSize(item, version);
        return size;
    }

    public static <T, P, Version> long serializedArraySize(T[] array, P p, Version version, ParameterisedVersionedSerializer<T, P, Version> serializer)
    {
        long size = sizeofUnsignedVInt(array.length);
        for (T item : array)
            size += serializer.serializedSize(item, p, version);
        return size;
    }
}
