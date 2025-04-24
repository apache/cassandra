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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.VersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

public class NullableSerializer
{
    public static <T> void serializeNullable(T value, DataOutputPlus out, UnversionedSerializer<T> serializer) throws IOException
    {
        out.writeBoolean(value != null);
        if (value != null)
            serializer.serialize(value, out);
    }

    public static <T> void serializeNullable(T value, DataOutputPlus out, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        out.writeBoolean(value != null);
        if (value != null)
            serializer.serialize(value, out, version);
    }

    public static <T, Version> void serializeNullable(T value, DataOutputPlus out, Version version, VersionedSerializer<T, Version> serializer) throws IOException
    {
        out.writeBoolean(value != null);
        if (value != null)
            serializer.serialize(value, out, version);
    }

    public static <T> T deserializeNullable(DataInputPlus in, UnversionedSerializer<T> serializer) throws IOException
    {
        return in.readBoolean() ? serializer.deserialize(in) : null;
    }

    public static <T> T deserializeNullable(DataInputPlus in, int version, IVersionedSerializer<T> serializer) throws IOException
    {
        return in.readBoolean() ? serializer.deserialize(in, version) : null;
    }

    public static <T, Version> T deserializeNullable(DataInputPlus in, Version version, VersionedSerializer<T, Version> serializer) throws IOException
    {
        return in.readBoolean() ? serializer.deserialize(in, version) : null;
    }

    public static <T> long serializedNullableSize(T value, UnversionedSerializer<T> serializer)
    {
        return value != null
               ? TypeSizes.sizeof(true) + serializer.serializedSize(value)
               : TypeSizes.sizeof(false);
    }

    public static <T> long serializedNullableSize(T value, int version, IVersionedSerializer<T> serializer)
    {
        return value != null
                ? TypeSizes.sizeof(true) + serializer.serializedSize(value, version)
                : TypeSizes.sizeof(false);
    }

    public static <T, Version> long serializedNullableSize(T value, Version version, VersionedSerializer<T, Version> serializer)
    {
        return value != null
               ? TypeSizes.sizeof(true) + serializer.serializedSize(value, version)
               : TypeSizes.sizeof(false);
    }

    public static <T> UnversionedSerializer<T> wrap(UnversionedSerializer<T> wrap)
    {
        return new UnversionedSerializer<>()
        {
            @Override
            public void serialize(T t, DataOutputPlus out) throws IOException
            {
                serializeNullable(t, out, wrap);
            }

            @Override
            public T deserialize(DataInputPlus in) throws IOException
            {
                return deserializeNullable(in, wrap);
            }

            @Override
            public long serializedSize(T t)
            {
                return serializedNullableSize(t, wrap);
            }
        };
    }

    public static <T> IVersionedSerializer<T> wrap(IVersionedSerializer<T> wrap)
    {
        return new IVersionedSerializer<T>() {
            public void serialize(T t, DataOutputPlus out, int version) throws IOException
            {
                serializeNullable(t, out, version, wrap);
            }

            public T deserialize(DataInputPlus in, int version) throws IOException
            {
                return deserializeNullable(in, version, wrap);
            }

            public long serializedSize(T t, int version)
            {
                return serializedNullableSize(t, version, wrap);
            }
        };
    }

    public static <T, Version> VersionedSerializer<T, Version> wrap(VersionedSerializer<T, Version> wrap)
    {
        return new VersionedSerializer<>() {
            @Override
            public void serialize(T t, DataOutputPlus out, Version version) throws IOException
            {
                serializeNullable(t, out, version, wrap);
            }

            @Override
            public T deserialize(DataInputPlus in, Version version) throws IOException
            {
                return deserializeNullable(in, version, wrap);
            }

            @Override
            public long serializedSize(T t, Version version)
            {
                return serializedNullableSize(t, version, wrap);
            }
        };
    }
}
