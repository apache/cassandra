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

import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.streaming.ProgressInfo;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.streaming.StreamingChannel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class StreamMetricsHandlerTest
{
    @Test
    public void testFileStreamMetricsListenerShouldCountBytesTransferredOutbound()
    {
        StreamMetricsHandler streamMetricsHandler = new StreamMetricsHandler();
        InetAddressAndPort local = FBUtilities.getLocalAddressAndPort();
        ProgressInfo progressInfo = new ProgressInfo(local, 0, "testFile", ProgressInfo.Direction.OUT, 1, 1, 10);
        TimeUUID planId = TimeUUID.maxAtUnixMillis(1L);
        StreamEvent.ProgressEvent event = new StreamEvent.ProgressEvent(planId, progressInfo);
        streamMetricsHandler.handleStreamEvent(event);
        progressInfo = new ProgressInfo(local, 0, "testFile", ProgressInfo.Direction.OUT, 10, 9, 10);
        event = new StreamEvent.ProgressEvent(planId, progressInfo);
        streamMetricsHandler.handleStreamEvent(event);
        assertEquals("Size of the total counter should be equal to the number of bytes streamed", 10L, StreamingMetrics.totalOutgoingBytes.getCount());
        assertEquals("Size of the peer counter should be equal to the number of bytes stteamed", 10L, StreamingMetrics.get(local).outgoingBytes.getCount());
    }

    @Test
    public void testFileStreamMetricsListenerShouldCountBytesTransferredInbound()
    {
        StreamMetricsHandler streamMetricsHandler = new StreamMetricsHandler();
        InetAddressAndPort local = FBUtilities.getLocalAddressAndPort();
        ProgressInfo progressInfo = new ProgressInfo(local, 0, "testFile", ProgressInfo.Direction.IN, 10, 10, 15);
        TimeUUID planId = TimeUUID.maxAtUnixMillis(1L);
        StreamEvent.ProgressEvent event = new StreamEvent.ProgressEvent(planId, progressInfo);
        streamMetricsHandler.handleStreamEvent(event);
        progressInfo = new ProgressInfo(local, 0, "testFile", ProgressInfo.Direction.IN, 15, 5, 15);
        event = new StreamEvent.ProgressEvent(planId, progressInfo);
        streamMetricsHandler.handleStreamEvent(event);
        StreamSession session = mockStreamSession();
        StreamEvent.SessionCompleteEvent sessionCompleteEvent = new StreamEvent.SessionCompleteEvent(session);
        streamMetricsHandler.handleStreamEvent(sessionCompleteEvent);
        assertEquals("Size of the total counter should be equal to the number of bytes streamed", 15L, StreamingMetrics.totalIncomingBytes.getCount());
        assertEquals("Size of the peer counter should be equal to the number of bytes stteamed", 15L, StreamingMetrics.get(local).incomingBytes.getCount());
    }

    private StreamSession mockStreamSession()
    {
        StreamOperation operation = StreamOperation.REPAIR;
        InetAddressAndPort peer = FBUtilities.getLocalAddressAndPort();
        StreamingChannel.Factory factory = StreamingChannel.Factory.Global.streamingFactory();
        StreamingChannel streamingChannel = mock(StreamingChannel.class);
        int messagingVersion = 0;
        boolean isFollower = true;
        int index = 0;
        TimeUUID pendingRepair = TimeUUID.maxAtUnixMillis(1L);
        PreviewKind previewKind = PreviewKind.ALL;
        return new StreamSession(operation, peer, factory, streamingChannel, messagingVersion, isFollower, index,
                                 pendingRepair, previewKind);
    }
}
