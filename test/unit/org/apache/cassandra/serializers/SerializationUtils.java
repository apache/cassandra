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

package org.apache.cassandra.serializers;

import java.io.IOException;

import org.junit.Assert;

import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;

public class SerializationUtils
{

    public static <T> T cycleSerialization(T src, IVersionedSerializer<T> serializer, int version)
    {
        int expectedSize = (int) serializer.serializedSize(src, version);

        try (DataOutputBuffer out = new DataOutputBuffer(expectedSize))
        {
            serializer.serialize(src, out, version);
            Assert.assertEquals(expectedSize, out.buffer().limit());
            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), false))
            {
                return serializer.deserialize(in, version);
            }
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    public static <T> T cycleSerialization(T src, IVersionedSerializer<T> serializer)
    {
        return cycleSerialization(src, serializer, MessagingService.current_version);
    }

    public static <T> void assertSerializationCycle(T src, IVersionedSerializer<T> serializer, int version)
    {
        T dst = cycleSerialization(src, serializer, version);
        Assert.assertEquals(src, dst);
    }

    public static <T> void assertSerializationCycle(T src, IVersionedSerializer<T> serializer)
    {
        assertSerializationCycle(src, serializer, MessagingService.current_version);
    }
}
