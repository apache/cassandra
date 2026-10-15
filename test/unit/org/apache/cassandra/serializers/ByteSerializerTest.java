/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.serializers;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ByteSerializerTest
{
    @Test
    public void testSerialization()
    {
        Byte value = (byte) 42;
        ByteBuffer buf = ByteSerializer.instance.serialize(value);
        ByteSerializer.instance.validate(buf);

        assertEquals(value, ByteSerializer.instance.deserialize(buf));
    }

    @Test
    public void testDeserializeEmptyBufferReturnsNull()
    {
        assertNull(ByteSerializer.instance.deserialize(ByteBuffer.allocate(0)));
    }

    @Test
    public void testValidateAcceptsEmptyBuffer()
    {
        ByteSerializer.instance.validate(ByteBuffer.allocate(0));
    }

    @Test (expected=MarshalException.class)
    public void testValidateRejectsWrongSizedBuffer()
    {
        ByteSerializer.instance.validate(ByteBuffer.allocate(2));
    }
}
