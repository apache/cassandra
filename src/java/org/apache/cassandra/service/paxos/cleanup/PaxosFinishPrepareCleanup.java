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
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.apache.cassandra.utils.concurrent.IntrusiveStack;

import static org.apache.cassandra.exceptions.RequestFailureReason.UNKNOWN;
import static org.apache.cassandra.net.NoPayload.noPayload;

public class PaxosFinishPrepareCleanup extends AsyncFuture<Void> implements RequestCallbackWithFailure<Void>
{
    private final Set<InetAddressAndPort> waitingResponse;

    PaxosFinishPrepareCleanup(Collection<InetAddressAndPort> endpoints)
    {
        this.waitingResponse = new HashSet<>(endpoints);
    }

    public static PaxosFinishPrepareCleanup finish(Collection<InetAddressAndPort> endpoints, PaxosCleanupHistory result)
    {
        PaxosFinishPrepareCleanup callback = new PaxosFinishPrepareCleanup(endpoints);
        synchronized (callback)
        {
            Message<PaxosCleanupHistory> message = Message.out(Verb.PAXOS2_CLEANUP_FINISH_PREPARE_REQ, result);
            for (InetAddressAndPort endpoint : endpoints)
                MessagingService.instance().sendWithCallback(message, endpoint, callback);
        }
        return callback;
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        tryFailure(new PaxosCleanupException(reason + " failure response from " + from));
    }

    public synchronized void onResponse(Message<Void> msg)
    {
        if (isDone())
            return;

        if (!waitingResponse.remove(msg.from()))
            throw new IllegalArgumentException("Received unexpected response from " + msg.from());

        if (waitingResponse.isEmpty())
            trySuccess(null);
    }

    static class PendingCleanup extends IntrusiveStack<PendingCleanup>
    {
        private static final AtomicReference<PendingCleanup> pendingCleanup = new AtomicReference();
        private static final Runnable CLEANUP = () -> {
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
                    MessagingService.instance().respondWithFailure(UNKNOWN, pending.message);
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
                    MessagingService.instance().respondWithFailure(UNKNOWN, pending.message);
                }
            }

            try
            {
                SystemKeyspace.flushPaxosRepairHistory();
                for (PendingCleanup pending : IntrusiveStack.iterable(list))
                {
                    if (failed == null || !failed.contains(pending))
                        MessagingService.instance().respond(noPayload, pending.message);
                }
            }
            catch (Throwable t)
            {
                fail = Throwables.merge(fail, t);
                for (PendingCleanup pending : IntrusiveStack.iterable(list))
                {
                    if (failed == null || !failed.contains(pending))
                        MessagingService.instance().respondWithFailure(UNKNOWN, pending.message);
                }
            }
            Throwables.maybeFail(fail);
        };

        final Message<PaxosCleanupHistory> message;
        PendingCleanup(Message<PaxosCleanupHistory> message)
        {
            this.message = message;
        }

        public static void add(Message<PaxosCleanupHistory> message)
        {
            PendingCleanup next = new PendingCleanup(message);
            PendingCleanup prev = IntrusiveStack.push(AtomicReference::get, AtomicReference::compareAndSet, pendingCleanup, next);
            if (prev == null)
                Stage.MISC.execute(CLEANUP);
        }
    }

    public static final IVerbHandler<PaxosCleanupHistory> verbHandler = PendingCleanup::add;
}
