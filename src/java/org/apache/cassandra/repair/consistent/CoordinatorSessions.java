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

package org.apache.cassandra.repair.consistent;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;

import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * Container for all consistent repair sessions a node is coordinating
 */
public class CoordinatorSessions
{
    private final Map<UUID, CoordinatorSession> sessions = new HashMap<>();

    protected CoordinatorSession buildSession(CoordinatorSession.Builder builder)
    {
        return new CoordinatorSession(builder);
    }

    public synchronized CoordinatorSession registerSession(UUID sessionId, Set<InetAddress> participants)
    {
        Preconditions.checkArgument(!sessions.containsKey(sessionId), "A coordinator already exists for session %s", sessionId);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(sessionId);
        CoordinatorSession.Builder builder = CoordinatorSession.builder();
        builder.withState(ConsistentSession.State.PREPARING);
        builder.withSessionID(sessionId);
        builder.withCoordinator(prs.coordinator);

        builder.withTableIds(prs.getTableIds());
        builder.withRepairedAt(prs.repairedAt);
        builder.withRanges(prs.getRanges());
        builder.withParticipants(participants);
        CoordinatorSession session = buildSession(builder);
        sessions.put(session.sessionID, session);
        return session;
    }

    public synchronized CoordinatorSession getSession(UUID sessionId)
    {
        return sessions.get(sessionId);
    }

    public void handlePrepareResponse(PrepareConsistentResponse msg)
    {
        CoordinatorSession session = getSession(msg.parentSession);
        if (session != null)
        {
            session.handlePrepareResponse(msg.participant, msg.success);
        }
    }

    public void handleFinalizePromiseMessage(FinalizePromise msg)
    {
        CoordinatorSession session = getSession(msg.sessionID);
        if (session != null)
        {
            session.handleFinalizePromise(msg.participant, msg.promised);
        }
    }

    public void handleFailSessionMessage(FailSession msg)
    {
        CoordinatorSession session = getSession(msg.sessionID);
        if (session != null)
        {
            session.fail();
        }
    }
}
