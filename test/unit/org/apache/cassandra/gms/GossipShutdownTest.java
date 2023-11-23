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

package org.apache.cassandra.gms;

import java.io.IOException;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.NoPayload;
import org.apache.cassandra.net.Verb;
import org.assertj.core.api.Assertions;

public class GossipShutdownTest
{
    private static final int BEFORE_CHANGE = MessagingService.Version.VERSION_40.value;
    private static final int AFTER_CHANGE = MessagingService.Version.VERSION_50.value;

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.clientInitialization();
    }

    @Test
    public void mixedMode() throws IOException
    {
        Message<GossipShutdown> message = Message.out(Verb.GOSSIP_SHUTDOWN, new GossipShutdown(new EndpointState(HeartBeatState.empty())));

        Assertions.assertThat(serde(message, BEFORE_CHANGE)).isNull();
        Assertions.assertThat(serde(message, AFTER_CHANGE)).isInstanceOf(GossipShutdown.class);

        // got from 4.x peer
        Assertions.assertThat(serde(Message.out(Verb.GOSSIP_SHUTDOWN, NoPayload.noPayload), BEFORE_CHANGE)).isNull();
    }

    private Object serde(Message<?> message, int version) throws IOException
    {
        try (DataOutputBuffer out = DataOutputBuffer.scratchBuffer.get())
        {
            Message.serializer.serialize(message, out, version);
            return Message.serializer.deserialize(new DataInputBuffer(out.unsafeGetBufferAndFlip(), false), message.header, version).payload;
        }
    }
}