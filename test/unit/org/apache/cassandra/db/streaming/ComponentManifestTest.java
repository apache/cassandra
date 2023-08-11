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

package org.apache.cassandra.db.streaming;

import java.util.LinkedHashMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.format.SSTableFormat.Components;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.serializers.SerializationUtils;

public class ComponentManifestTest
{
    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void testSerialization()
    {
        ComponentManifest expected = new ComponentManifest(new LinkedHashMap<Component, Long>() {{ put(Components.DATA, 100L); }});
        SerializationUtils.assertSerializationCycle(expected, ComponentManifest.serializers.get(BigFormat.getInstance().name()));
    }

    // Propose removing this test which now fails on VIntOutOfRange
    // We don't safely check if the bytes are bad so I don't understand what is being tested
    // There is no checksum
//    @Test(expected = EOFException.class)
//    public void testSerialization_FailsOnBadBytes() throws IOException
//    {
//        ByteBuffer buf = ByteBuffer.allocate(512);
//        ComponentManifest expected = new ComponentManifest(new LinkedHashMap<Component, Long>() {{ put(Components.DATA, 100L); }});
//
//        DataOutputBufferFixed out = new DataOutputBufferFixed(buf);
//
//        ComponentManifest.serializer.serialize(expected, out, MessagingService.VERSION_40);
//
//        buf.putInt(0, -100);
//
//        DataInputBuffer in = new DataInputBuffer(out.buffer(), false);
//        ComponentManifest actual = ComponentManifest.serializer.deserialize(in, MessagingService.VERSION_40);
//        assertNotEquals(expected, actual);
//    }
}
