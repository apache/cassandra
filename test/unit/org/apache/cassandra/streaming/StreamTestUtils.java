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

package org.apache.cassandra.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.net.OutboundConnectionSettings;
import org.apache.cassandra.streaming.messages.StreamMessage;

import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;

public class StreamTestUtils
{
    static StreamSession session()
    {
        StubMessageSender messageSender = new StubMessageSender();
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.REPAIR, 1, new DefaultConnectionFactory(), false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(UUID.randomUUID(), StreamOperation.REPAIR, Collections.<StreamEventHandler>emptyList(), streamCoordinator);

        StreamSession session = new StreamSession(StreamOperation.REPAIR,
                                                  node1,
                                                  connectionFactory,
                                                  true,
                                                  0,
                                                  null,
                                                  PreviewKind.NONE)
        {

            @Override
            public void progress(String filename, ProgressInfo.Direction direction, long bytes, long total)
            {
                //no-op
            }

            @Override
            public StreamingMessageSender getMessageSender()
            {
                return messageSender;
            }
        };
        session.init(future);
        return session;
    }

    static class StubMessageSender implements StreamingMessageSender
    {
        final List<StreamMessage> sentMessages = new ArrayList<>();

        StubMessageSender()
        {
        }

        public void sendMessage(StreamMessage message)
        {
            sentMessages.add(message);
        }

        void reset()
        {
            sentMessages.clear();
        }

        public void initialize() throws IOException
        {

        }

        public boolean connected()
        {
            return false;
        }

        public void close()
        {

        }
    }

    static StreamConnectionFactory connectionFactory = (OutboundConnectionSettings template, int messagingVersion) -> {
        throw new UnsupportedOperationException();
    };
}
