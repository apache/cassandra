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
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.service.ActiveRepairService;

/**
 * makes package private hacks available to compaction tests
 */
public class LocalSessionAccessor
{
    private static final ActiveRepairService ARS = ActiveRepairService.instance;

    public static void startup()
    {
        ARS.consistent.local.start();
    }

    public static void prepareUnsafe(UUID sessionID, InetAddress coordinator, Set<InetAddress> peers)
    {
        ActiveRepairService.ParentRepairSession prs = ARS.getParentRepairSession(sessionID);
        assert prs != null;
        LocalSession session = ARS.consistent.local.createSessionUnsafe(sessionID, prs, peers);
        ARS.consistent.local.putSessionUnsafe(session);
    }

    public static void finalizeUnsafe(UUID sessionID)
    {
        LocalSession session = ARS.consistent.local.getSession(sessionID);
        assert session != null;
        session.setState(ConsistentSession.State.FINALIZED);
        ARS.consistent.local.save(session);
    }

    public static void failUnsafe(UUID sessionID)
    {
        LocalSession session = ARS.consistent.local.getSession(sessionID);
        assert session != null;
        session.setState(ConsistentSession.State.FAILED);
        ARS.consistent.local.save(session);
    }
}
