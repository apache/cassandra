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

import org.junit.Test;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class AbstractTypeSerializerTest
{
    private final AbstractTypeSerializer serializer = new AbstractTypeSerializer();

    @Test
    public void testSerializedArraySizeMatchesSerializeArray() throws IOException
    {
        assertRoundTrip();
        assertRoundTrip(Int32Type.instance);
        assertRoundTrip(Int32Type.instance, UTF8Type.instance);
        assertRoundTrip(Int32Type.instance, UTF8Type.instance, Int32Type.instance, UTF8Type.instance);
    }

    private void assertRoundTrip(AbstractType<?>... types) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            serializer.serializeArray(types, out);

            assertEquals("serializedArraySize must match the number of bytes written by serializeArray",
                         out.getLength(), serializer.serializedArraySize(types));

            try (DataInputBuffer in = new DataInputBuffer(out.buffer(), true))
            {
                AbstractType<?>[] deserialized = serializer.deserializeArray(in);
                assertArrayEquals(types, deserialized);
            }
        }
    }
}
