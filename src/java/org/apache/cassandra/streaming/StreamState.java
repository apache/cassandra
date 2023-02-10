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

import java.io.Serializable;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import org.apache.cassandra.utils.TimeUUID;

/**
 * Current snapshot of streaming progress.
 */
public class StreamState implements Serializable
{
    public final TimeUUID planId;
    public final StreamOperation streamOperation;
    public final Set<SessionInfo> sessions;

    public StreamState(TimeUUID planId, StreamOperation streamOperation, Set<SessionInfo> sessions)
    {
        this.planId = planId;
        this.sessions = sessions;
        this.streamOperation = streamOperation;
    }

    public boolean hasFailedSession()
    {
        return Iterables.any(sessions, SessionInfo::isFailed);
    }

    public boolean hasAbortedSession()
    {
        return Iterables.any(sessions, SessionInfo::isAborted);
    }

    public List<SessionSummary> createSummaries()
    {
        return Lists.newArrayList(Iterables.transform(sessions, SessionInfo::createSummary));
    }

    public Set<SessionInfo> sessions() {
        return sessions;
    }
}
