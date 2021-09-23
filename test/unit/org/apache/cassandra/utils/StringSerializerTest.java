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
import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.io.util.DataOutputPlus;

public class StringSerializerTest
{
    @Test
    public void testStringSerialization() throws IOException
    {
        serializationRoundTrip("");
        serializationRoundTrip(" ");
        serializationRoundTrip("zxc");
        serializationRoundTrip("óążćź");
    }

    private void serializationRoundTrip(String value) throws IOException
    {
        int protocolVersion = 1;
        StringSerializer serializer = StringSerializer.serializer;
        long size = serializer.serializedSize(value, protocolVersion);

        ByteBuffer buf = ByteBuffer.allocate((int)size);
        DataOutputPlus out = new DataOutputBufferFixed(buf);
        serializer.serialize(value, out, protocolVersion);
        Assert.assertEquals(size, buf.position());

        buf.flip();
        DataInputPlus in = new DataInputBuffer(buf, false);
        String deserialized = serializer.deserialize(in, protocolVersion);
        Assert.assertEquals(value, deserialized);
        Assert.assertEquals(value.hashCode(), deserialized.hashCode());
    }
}