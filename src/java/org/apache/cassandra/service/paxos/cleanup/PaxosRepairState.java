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

package org.apache.cassandra.service.paxos.cleanup;

import java.io.IOException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Preconditions;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.IntrusiveStack;

import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.NoPayload.noPayload;

/**
 * Tracks the state of paxos repair cleanup work
 */
public class PaxosRepairState
{
    private final SharedContext ctx;
    private final AtomicReference<PendingCleanup> pendingCleanup = new AtomicReference<>();
    private final Map<UUID, PaxosCleanupSession> sessions = new ConcurrentHashMap<>();
    private final ConcurrentMap<TableId, PaxosTableRepairs> tableRepairsMap = new ConcurrentHashMap<>();

    public PaxosRepairState(SharedContext ctx)
    {
        this.ctx = ctx;
    }

    public static PaxosRepairState instance()
    {
        return Holder.instance;
    }

    PaxosTableRepairs getForTable(TableId tableId)
    {
        return tableRepairsMap.computeIfAbsent(tableId, k -> new PaxosTableRepairs());
    }

    public void evictHungRepairs()
    {
        long deadline = ctx.clock().nanoTime() - TimeUnit.MINUTES.toNanos(5);
        for (PaxosTableRepairs repairs : tableRepairsMap.values())
            repairs.evictHungRepairs(deadline);
    }

    public void clearRepairs()
    {
        for (PaxosTableRepairs repairs : tableRepairsMap.values())
            repairs.clear();
    }


    public void setSession(PaxosCleanupSession session)
    {
        Preconditions.checkState(!sessions.containsKey(session.session));
        sessions.put(session.session, session);
    }

    public void removeSession(PaxosCleanupSession session)
    {
        Preconditions.checkState(sessions.containsKey(session.session));
        sessions.remove(session.session);
    }

    public void finishSession(InetAddressAndPort from, PaxosCleanupResponse response)
    {
        PaxosCleanupSession session = sessions.get(response.session);
        if (session != null)
            session.finish(from, response);
    }
    
    public void addCleanupHistory(Message<PaxosCleanupHistory> message)
    {
        PendingCleanup.add(ctx, pendingCleanup, message);
    }

    /**
     * This is not required, but it helps to see what escapes simulation... can put a break point on instance() while running the tests
     */
    private static class Holder
    {
        private static final PaxosRepairState instance = new PaxosRepairState(SharedContext.Global.instance);
    }

    static class PendingCleanup extends IntrusiveStack<PendingCleanup>
    {
        private final Message<PaxosCleanupHistory> message;

        PendingCleanup(Message<PaxosCleanupHistory> message)
        {
            this.message = message;
        }

        private static void add(SharedContext ctx, AtomicReference<PendingCleanup> pendingCleanup, Message<PaxosCleanupHistory> message)
        {
            PendingCleanup next = new PendingCleanup(message);
            PendingCleanup prev = IntrusiveStack.push(AtomicReference::get, AtomicReference::compareAndSet, pendingCleanup, next);
            if (prev == null)
                Stage.MISC.execute(() -> cleanup(ctx, pendingCleanup));
        }

        private static void cleanup(SharedContext ctx, AtomicReference<PendingCleanup> pendingCleanup)
        {
            PendingCleanup list = pendingCleanup.getAndSet(null);
            if (list == null)
                return;

            Ballot highBound = Ballot.none();
            for (PendingCleanup pending : IntrusiveStack.iterable(list))
            {
                PaxosCleanupHistory cleanupHistory = pending.message.payload;
                if (cleanupHistory.highBound.compareTo(highBound) > 0)
                    highBound = cleanupHistory.highBound;
            }
            try
            {
                try
                {
                    PaxosState.ballotTracker().updateLowBound(highBound);
                }
                catch (IOException e)
                {
                    throw new FSWriteError(e);
                }
            }
            catch (Throwable t)
            {
                for (PendingCleanup pending : IntrusiveStack.iterable(list))
                    ctx.messaging().respondWithFailure(UNKNOWN, pending.message);
                throw t;
            }

            Set<PendingCleanup> failed = null;
            Throwable fail = null;
            for (PendingCleanup pending : IntrusiveStack.iterable(list))
            {
                try
                {
                    Schema.instance.getColumnFamilyStoreInstance(pending.message.payload.tableId)
                                   .syncPaxosRepairHistory(pending.message.payload.history, false);
                }
                catch (Throwable t)
                {
                    fail = Throwables.merge(fail, t);
                    if (failed == null)
                        failed = Collections.newSetFromMap(new IdentityHashMap<>());
                    failed.add(pending);
                    ctx.messaging().respondWithFailure(UNKNOWN, pending.message);
                }
            }

            try
            {
                SystemKeyspace.flushPaxosRepairHistory();
                for (PendingCleanup pending : IntrusiveStack.iterable(list))
                {
                    if (failed == null || !failed.contains(pending))
                        ctx.messaging().respond(noPayload, pending.message);
                }
            }
            catch (Throwable t)
            {
                fail = Throwables.merge(fail, t);
                for (PendingCleanup pending : IntrusiveStack.iterable(list))
                {
                    if (failed == null || !failed.contains(pending))
                        ctx.messaging().respondWithFailure(UNKNOWN, pending.message);
                }
            }
            Throwables.maybeFail(fail);
        }
    }
}
