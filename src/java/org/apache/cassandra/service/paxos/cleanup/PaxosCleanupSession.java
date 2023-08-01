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

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.IEndpointStateChangeSubscriber;
import org.apache.cassandra.gms.IFailureDetectionEventListener;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.RequestCallbackWithFailure;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.concurrent.AsyncFuture;

import static org.apache.cassandra.config.CassandraRelevantProperties.PAXOS_CLEANUP_SESSION_TIMEOUT_SECONDS;
import static org.apache.cassandra.net.Verb.PAXOS2_CLEANUP_REQ;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class PaxosCleanupSession extends AsyncFuture<Void> implements Runnable,
                                                                      IEndpointStateChangeSubscriber,
                                                                      IFailureDetectionEventListener,
                                                                      RequestCallbackWithFailure<Void>
{
    private static final Map<UUID, PaxosCleanupSession> sessions = new ConcurrentHashMap<>();

    static final long TIMEOUT_NANOS;
    static
    {
        long timeoutSeconds = PAXOS_CLEANUP_SESSION_TIMEOUT_SECONDS.getLong();
        TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(timeoutSeconds);
    }

    private static class TimeoutTask implements Runnable
    {
        private final WeakReference<PaxosCleanupSession> ref;

        TimeoutTask(PaxosCleanupSession session)
        {
            this.ref = new WeakReference<>(session);
        }

        @Override
        public void run()
        {
            PaxosCleanupSession session = ref.get();
            if (session == null || session.isDone())
                return;

            long remaining = session.lastMessageSentNanos + TIMEOUT_NANOS - nanoTime();
            if (remaining > 0)
                schedule(remaining);
            else
                session.fail(String.format("Paxos cleanup session %s timed out", session.session));
        }

        ScheduledFuture<?> schedule(long delayNanos)
        {
            return ScheduledExecutors.scheduledTasks.scheduleTimeoutWithDelay(this, delayNanos, TimeUnit.NANOSECONDS);
        }

        private static ScheduledFuture<?> schedule(PaxosCleanupSession session)
        {
            return new TimeoutTask(session).schedule(TIMEOUT_NANOS);
        }
    }

    private final UUID session = UUID.randomUUID();
    private final TableId tableId;
    private final Collection<Range<Token>> ranges;
    private final Queue<InetAddressAndPort> pendingCleanups = new ConcurrentLinkedQueue<>();
    private InetAddressAndPort inProgress = null;
    private volatile long lastMessageSentNanos = nanoTime();
    private ScheduledFuture<?> timeout;

    PaxosCleanupSession(Collection<InetAddressAndPort> endpoints, TableId tableId, Collection<Range<Token>> ranges)
    {
        this.tableId = tableId;
        this.ranges = ranges;

        pendingCleanups.addAll(endpoints);
    }

    private static void setSession(PaxosCleanupSession session)
    {
        Preconditions.checkState(!sessions.containsKey(session.session));
        sessions.put(session.session, session);
    }

    private static void removeSession(PaxosCleanupSession session)
    {
        Preconditions.checkState(sessions.containsKey(session.session));
        sessions.remove(session.session);
    }

    @Override
    public void run()
    {
        setSession(this);
        startNextOrFinish();
        if (!isDone())
            timeout = TimeoutTask.schedule(this);
    }

    private void startCleanup(InetAddressAndPort endpoint)
    {
        lastMessageSentNanos = nanoTime();
        PaxosCleanupRequest completer = new PaxosCleanupRequest(session, tableId, ranges);
        Message<PaxosCleanupRequest> msg = Message.out(PAXOS2_CLEANUP_REQ, completer);
        MessagingService.instance().sendWithCallback(msg, endpoint, this);
    }

    private synchronized void startNextOrFinish()
    {
        InetAddressAndPort endpoint = pendingCleanups.poll();

        if (endpoint == null)
            Preconditions.checkState(inProgress == null, "Unable to complete paxos cleanup session %s, still waiting on %s", session, inProgress);
        else
            Preconditions.checkState(inProgress == null, "Unable to start paxos cleanup on %s for %s, still waiting on response from %s", endpoint, session, inProgress);

        inProgress = endpoint;

        if (endpoint != null)
        {
            startCleanup(endpoint);
        }
        else
        {
            removeSession(this);
            trySuccess(null);
            if (timeout != null)
                timeout.cancel(true);
        }
    }

    private synchronized void fail(String message)
    {
        if (isDone())
            return;
        removeSession(this);
        tryFailure(new PaxosCleanupException(message));
        if (timeout != null)
            timeout.cancel(true);
    }

    private synchronized void finish(InetAddressAndPort from, PaxosCleanupResponse finished)
    {
        Preconditions.checkArgument(from.equals(inProgress), "Received unexpected cleanup complete response from %s for session %s. Expected %s", from, session, inProgress);
        inProgress = null;

        if (finished.wasSuccessful)
        {
            startNextOrFinish();
        }
        else
        {
            fail(String.format("Paxos cleanup session %s failed on %s with message: %s", session, from, finished.message));
        }
    }

    public static void finishSession(InetAddressAndPort from, PaxosCleanupResponse response)
    {
        PaxosCleanupSession session = sessions.get(response.session);
        if (session != null)
            session.finish(from, response);
    }

    private synchronized void maybeKillSession(InetAddressAndPort unavailable, String reason)
    {
        // don't fail if we've already completed the cleanup for the unavailable endpoint,
        // if it's something that affects availability, the ongoing sessions will fail themselves
        if (!pendingCleanups.contains(unavailable))
            return;

        fail(String.format("Paxos cleanup session %s failed after %s %s", session, unavailable, reason));
    }

    @Override
    public void onJoin(InetAddressAndPort endpoint, EndpointState epState)
    {

    }

    @Override
    public void beforeChange(InetAddressAndPort endpoint, EndpointState currentState, ApplicationState newStateKey, VersionedValue newValue)
    {

    }

    @Override
    public void onChange(InetAddressAndPort endpoint, ApplicationState state, VersionedValue value)
    {

    }

    @Override
    public void onAlive(InetAddressAndPort endpoint, EndpointState state)
    {

    }

    @Override
    public void onDead(InetAddressAndPort endpoint, EndpointState state)
    {
        maybeKillSession(endpoint, "marked dead");
    }

    @Override
    public void onRemove(InetAddressAndPort endpoint)
    {
        maybeKillSession(endpoint, "removed from ring");
    }

    @Override
    public void onRestart(InetAddressAndPort endpoint, EndpointState state)
    {
        maybeKillSession(endpoint, "restarted");
    }

    @Override
    public void convict(InetAddressAndPort ep, double phi)
    {
        maybeKillSession(ep, "convicted by failure detector");
    }

    @Override
    public void onFailure(InetAddressAndPort from, RequestFailureReason reason)
    {
        fail(from.toString() + ' ' + reason + " for cleanup request for paxos cleanup session  " + session);
    }

    @Override
    public void onResponse(Message<Void> msg)
    {
        // noop, we're only interested in failures
    }
}
