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

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public class ArraySerializers
{
    public static <T> void serializeArray(T[] items, DataOutputPlus out, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        out.writeUnsignedVInt32(items.length);
        for (T item : items)
            serializer.serialize(item, out, version);
    }

    public static <T> T[] deserializeArray(DataInputPlus in, int version, IVersionedSerializer<T> serializer, IntFunction<T[]> arrayFactory) throws IOException
    {
        int size = in.readUnsignedVInt32();
        T[] items = arrayFactory.apply(size);
        for (int i = 0; i < size; i++)
            items[i] = serializer.deserialize(in, version);
        return items;
    }

    public static <T> long serializedArraySize(T[] array, int version, IVersionedSerializer<T> serializer)
    {
        long size = sizeofUnsignedVInt(array.length);
        for (T item : array)
            size += serializer.serializedSize(item, version);
        return size;
    }
}
