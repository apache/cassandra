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

package org.apache.cassandra.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.locator.InetAddressAndPort;

public class MessageInTest
{
    @BeforeClass
    public static void before()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // make sure deserializing message doesn't crash with an unknown verb
    @Test
    public void read_NullVerb() throws IOException
    {
        read(null);
    }

    @Test
    public void read_NoSerializer() throws IOException
    {
        read(MessagingService.Verb.UNUSED_5);
    }

    private void read(MessagingService.Verb verb) throws IOException
    {
        InetAddressAndPort addr = InetAddressAndPort.getByName("127.0.0.1");
        ByteBuffer buf = ByteBuffer.allocate(64);
        buf.limit(buf.capacity());
        DataInputPlus dataInputBuffer = new DataInputBuffer(buf, false);
        int payloadSize = 27;
        Assert.assertEquals(0, buf.position());
        Assert.assertNotNull(MessageIn.read(dataInputBuffer, 1, 42, 0, addr, payloadSize, verb, Collections.emptyMap()));
        Assert.assertEquals(payloadSize, buf.position());
    }
}
