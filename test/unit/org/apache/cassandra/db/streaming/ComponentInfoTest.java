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

import java.io.IOException;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.async.ByteBufDataInputPlus;
import org.apache.cassandra.net.async.ByteBufDataOutputPlus;
import org.apache.cassandra.serializers.SerializationUtils;

import static org.junit.Assert.assertNotEquals;

public class ComponentInfoTest
{
    @Test
    public void testSerialization()
    {
        ComponentInfo expected = new ComponentInfo(Component.Type.DATA, 100);
        SerializationUtils.assertSerializationCycle(expected, ComponentInfo.serializer);
    }

    @Test(expected = AssertionError.class)
    public void testSerialization_FailsOnBadBytes() throws IOException
    {
        ByteBuf buf = Unpooled.buffer(512);
        ComponentInfo expected = new ComponentInfo(Component.Type.DATA, 100);

        DataOutputPlus output = new ByteBufDataOutputPlus(buf);
        ComponentInfo.serializer.serialize(expected, output, MessagingService.VERSION_40);

        buf.setInt(0, -100);

        DataInputPlus input = new ByteBufDataInputPlus(buf);
        ComponentInfo actual = ComponentInfo.serializer.deserialize(input, MessagingService.VERSION_40);

        assertNotEquals(expected, actual);
    }
}
