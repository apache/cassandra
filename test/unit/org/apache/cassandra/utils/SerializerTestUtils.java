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

import org.junit.Assert;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

public class SerializerTestUtils
{
    private static int MS_VERSION = MessagingService.current_version;

    public static <T> T serdes(IVersionedSerializer<T> serializer, T message)
    {
        int expectedSize = (int) serializer.serializedSize(message, MS_VERSION);
        try (DataOutputBuffer out = new DataOutputBuffer(expectedSize))
        {
            serializer.serialize(message, out, MS_VERSION);
            Assert.assertEquals(expectedSize, out.buffer().limit());
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return serializer.deserialize(in, MS_VERSION);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public static <T> void assertSerializerIOEquality(T expected, IVersionedSerializer<T> serializer, int version)
    {
        int expectedSize = (int) serializer.serializedSize(expected, version);
        try (DataOutputBuffer out = new DataOutputBuffer(expectedSize))
        {
            serializer.serialize(expected, out, version);
            Assert.assertEquals(expectedSize, out.buffer().limit());
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                Assert.assertEquals(expected, serializer.deserialize(in, version));
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public static <T> void assertSerializerIOEquality(T expected, IVersionedSerializer<T> serializer)
    {
        assertSerializerIOEquality(expected, serializer, MS_VERSION);
    }
}
