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

import java.util.Set;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.NoSuchRepairSessionException;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.TimeUUID;

/**
 * makes package private hacks available to compaction tests
 */
public class LocalSessionAccessor
{
    private static final ActiveRepairService ARS = ActiveRepairService.instance();

    public static void startup()
    {
        ARS.consistent.local.start();
    }

    public static void prepareUnsafe(TimeUUID sessionID, InetAddressAndPort coordinator, Set<InetAddressAndPort> peers)
    {
        ActiveRepairService.ParentRepairSession prs = null;
        try
        {
            prs = ARS.getParentRepairSession(sessionID);
        }
        catch (NoSuchRepairSessionException e)
        {
            throw new RuntimeException(e);
        }
        assert prs != null;
        LocalSession session = ARS.consistent.local.createSessionUnsafe(sessionID, prs, peers);
        ARS.consistent.local.putSessionUnsafe(session);
    }

    public static long finalizeUnsafe(TimeUUID sessionID)
    {
        LocalSession session = setState(sessionID, ConsistentSession.State.FINALIZED);
        return session.repairedAt;
    }

    public static void failUnsafe(TimeUUID sessionID)
    {
        setState(sessionID, ConsistentSession.State.FAILED);
    }

    public static LocalSession setState(TimeUUID sessionId, ConsistentSession.State state)
    {
        LocalSession session = ARS.consistent.local.getSession(sessionId);
        assert session != null;
        session.setState(state);
        ARS.consistent.local.save(session);
        return session;
    }
}
