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

import java.util.Collection;

import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.streaming.messages.StreamMessageHeader;
import org.apache.cassandra.utils.TimeUUID;

/**
 * The main streaming hook for a storage implementation.
 *
 * From here, the streaming system can get instances of {@link StreamReceiver}, {@link IncomingStream},
 * and {@link OutgoingStream}, which expose the interfaces into the the underlying storage implementation
 * needed to make streaming work.
 */
public interface TableStreamManager
{
    /**
     * Creates a {@link StreamReceiver} for the given session, expecting the given number of streams
     */
    StreamReceiver createStreamReceiver(StreamSession session, int totalStreams);

    /**
     * Creates an {@link IncomingStream} for the given header
     */
    IncomingStream prepareIncomingStream(StreamSession session, StreamMessageHeader header);

    /**
     * Returns a collection of {@link OutgoingStream}s that contains the data selected by the
     * given replicas, pendingRepair, and preview.
     *
     * There aren't any requirements on how data is divided between the outgoing streams
     */
    Collection<OutgoingStream> createOutgoingStreams(StreamSession session,
                                                     RangesAtEndpoint replicas,
                                                     TimeUUID pendingRepair,
                                                     PreviewKind previewKind);
}
