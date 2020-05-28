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

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

import io.netty.util.concurrent.Future;
import org.apache.cassandra.streaming.messages.StreamMessage;
import org.apache.cassandra.utils.concurrent.AsyncPromise;

import static org.apache.cassandra.net.MessagingService.current_version;
import static org.apache.cassandra.streaming.StreamingChannel.Factory.Global.streamingFactory;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.apache.cassandra.utils.TokenRangeTestUtil.node1;

public class StreamTestUtils
{
    static StreamSession session(List<StreamMessage> sentMessages)
    {
        StreamCoordinator streamCoordinator = new StreamCoordinator(StreamOperation.REPAIR, 1, streamingFactory(), false, false, null, PreviewKind.NONE);
        StreamResultFuture future = StreamResultFuture.createInitiator(nextTimeUUID(), StreamOperation.REPAIR, Collections.<StreamEventHandler>emptyList(), streamCoordinator);

        StreamSession session = new StreamSession(StreamOperation.REPAIR,
                                                  node1,
                                                  channelFactory,
                                                  null,
                                                  current_version,
                                                  true,
                                                  0,
                                                  null,
                                                  PreviewKind.NONE)
        {
            @Override
            public void progress(String filename, ProgressInfo.Direction direction, long bytes, long delta, long total)
            {
                //no-op
            }

            @Override
            protected Future<?> sendControlMessage(StreamMessage message) {
                sentMessages.add(message);
                final AsyncPromise<Object> promise = new AsyncPromise<>(null);
                promise.setSuccess(null);
                return promise;
            }

        };
        session.init(future);
        return session;
    }

    static StreamingChannel.Factory channelFactory = (InetSocketAddress to, int messagingVersion, StreamingChannel.Kind kind)  -> {
        throw new UnsupportedOperationException();
    };
}
