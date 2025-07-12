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

package org.apache.cassandra.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import accord.utils.LazyToString;
import accord.utils.ReflectionUtils;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.assertj.core.api.Assertions;

public class Serializers
{
    // When using a shard buffer the following is the recommend thing to copy/paste
    // @SuppressWarnings({ "resource", "IOResourceOpenedButNotSafelyClosed" }) DataOutputBuffer output = new DataOutputBuffer();

    public static <T> void testSerde(DataOutputBuffer output, AsymmetricUnversionedSerializer<T, T> serializer, T input) throws IOException
    {
        output.clear();
        long expectedSize = serializer.serializedSize(input);
        serializer.serialize(input, output);
        Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
        ByteBuffer buffer = output.unsafeGetBufferAndFlip();
        DataInputBuffer in = new DataInputBuffer(buffer, false);
        T read = serializer.deserialize(in);
        Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
        Assertions.assertThat(buffer.remaining()).describedAs("deserialize did not consume all the serialized input").isEqualTo(0);
        buffer.flip();
        buffer.mark();
        serializer.skip(in);
        Assertions.assertThat(buffer.remaining()).describedAs("skip did not consume all the serialized input").isEqualTo(0);
        boolean testByteBufferMethods;
        try
        {
            testByteBufferMethods = serializer.getClass().getMethod("serialize", Object.class).getDeclaringClass() != AsymmetricUnversionedSerializer.class
                                 || serializer.getClass().getMethod("deserialize", ByteBuffer.class).getDeclaringClass() != AsymmetricUnversionedSerializer.class;
        }
        catch (NoSuchMethodException e)
        {
            throw new AssertionError(e);
        }
        if (testByteBufferMethods)
        {
            ByteBuffer serialized2 = serializer.serialize(input);
            T read2 = serializer.deserialize(serialized2);
            Assertions.assertThat(read2).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read2, input).toString())).isEqualTo(input);
        }
    }

    public static <T, P> void testSerde(DataOutputBuffer output, ParameterisedUnversionedSerializer<T, P> serializer, T input, P p) throws IOException
    {
        output.clear();
        long expectedSize = serializer.serializedSize(input, p);
        serializer.serialize(input, p, output);
        Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
        DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        T read = serializer.deserialize(p, in);
        Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
    }

    public static <T, P, Version> void testSerde(DataOutputBuffer output, ParameterisedVersionedSerializer<T, P, Version> serializer, T input, P p, Version version) throws IOException
    {
        output.clear();
        long expectedSize = serializer.serializedSize(input, p, version);
        serializer.serialize(input, p, output, version);
        Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
        DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        T read = serializer.deserialize(p, in, version);
        Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
    }

    public static <T> void testSerde(AsymmetricUnversionedSerializer<T, T> serializer, T input) throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer(Math.toIntExact(serializer.serializedSize(input))))
        {
            testSerde(output, serializer, input);
        }
    }

    public static <T> void testSerde(DataOutputBuffer output, IVersionedAsymmetricSerializer<T, T> serializer, T input, int version) throws IOException
    {
        output.clear();
        long expectedSize = serializer.serializedSize(input, version);
        serializer.serialize(input, output, version);
        Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
        DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        T read = serializer.deserialize(in, version);
        Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
    }

    public static <T> void testSerde(IVersionedAsymmetricSerializer<T, T> serializer, T input, int version) throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer(Math.toIntExact(serializer.serializedSize(input, version))))
        {
            testSerde(output, serializer, input, version);
        }
    }

    public static <T, Version> void testSerde(DataOutputBuffer output, AsymmetricVersionedSerializer<T, T, Version> serializer, T input, Version version) throws IOException
    {
        output.clear();
        long expectedSize = serializer.serializedSize(input, version);
        serializer.serialize(input, output, version);
        Assertions.assertThat(output.getLength()).describedAs("The serialized size and bytes written do not match").isEqualTo(expectedSize);
        DataInputBuffer in = new DataInputBuffer(output.unsafeGetBufferAndFlip(), false);
        T read = serializer.deserialize(in, version);
        Assertions.assertThat(read).describedAs("The deserialized output does not match the serialized input; difference %s", new LazyToString(() -> ReflectionUtils.recursiveEquals(read, input).toString())).isEqualTo(input);
    }

    public static <T, Version> void testSerde(AsymmetricVersionedSerializer<T, T, Version> serializer, T input, Version version) throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer(Math.toIntExact(serializer.serializedSize(input, version))))
        {
            testSerde(output, serializer, input, version);
        }
    }

    public static <T, P> void testSerde(ParameterisedUnversionedSerializer<T, P> serializer, T input, P param) throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer(Math.toIntExact(serializer.serializedSize(input, param))))
        {
            testSerde(output, serializer, input, param);
        }
    }

    public static <T, P, Version> void testSerde(ParameterisedVersionedSerializer<T, P, Version> serializer, T input, P param, Version version) throws IOException
    {
        try (DataOutputBuffer output = new DataOutputBuffer(Math.toIntExact(serializer.serializedSize(input, param, version))))
        {
            testSerde(output, serializer, input, param, version);
        }
    }
}