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

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.TimeUUID;

public abstract class StreamEvent
{
    public static enum Type
    {
        STREAM_PREPARED,
        STREAM_COMPLETE,
        FILE_PROGRESS,
    }

    public final Type eventType;
    public final TimeUUID planId;

    protected StreamEvent(Type eventType, TimeUUID planId)
    {
        this.eventType = eventType;
        this.planId = planId;
    }

    public static class SessionCompleteEvent extends StreamEvent
    {
        public final InetAddressAndPort peer;
        public final boolean success;
        public final int sessionIndex;
        public final Set<StreamRequest> requests;
        public final StreamOperation streamOperation;
        public final Map<String, Set<Range<Token>>> transferredRangesPerKeyspace;

        public SessionCompleteEvent(StreamSession session)
        {
            super(Type.STREAM_COMPLETE, session.planId());
            this.peer = session.peer;
            this.success = session.isSuccess();
            this.sessionIndex = session.sessionIndex();
            this.requests = ImmutableSet.copyOf(session.requests);
            this.streamOperation = session.streamOperation();
            this.transferredRangesPerKeyspace = Collections.unmodifiableMap(session.transferredRangesPerKeyspace);
        }
    }

    public static class ProgressEvent extends StreamEvent
    {
        public final ProgressInfo progress;

        public ProgressEvent(TimeUUID planId, ProgressInfo progress)
        {
            super(Type.FILE_PROGRESS, planId);
            this.progress = progress;
        }

        @Override
        public String toString()
        {
            return "<ProgressEvent " + progress + ">";
        }
    }

    public static class SessionPreparedEvent extends StreamEvent
    {
        public final SessionInfo session;
        public final StreamSession.PrepareDirection prepareDirection;

        public SessionPreparedEvent(TimeUUID planId, SessionInfo session, StreamSession.PrepareDirection prepareDirection)
        {
            super(Type.STREAM_PREPARED, planId);
            this.session = session;
            this.prepareDirection = prepareDirection;
        }
    }
}
