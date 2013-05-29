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

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.cliffc.high_scale_lib.NonBlockingHashMap;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

/**
 * StreamResultFuture asynchronously returns the final {@link StreamState} of execution of {@link StreamPlan}.
 * <p>
 * You can attach {@link StreamEventHandler} to this object to listen on {@link StreamEvent}s to track progress of the streaming.
 */
public final class StreamResultFuture extends AbstractFuture<StreamState>
{
    // Executor that establish the streaming connection. Once we're connected to the other end, the rest of the streaming
    // is directly handled by the ConnectionHandler incoming and outgoing threads.
    private static final DebuggableThreadPoolExecutor streamExecutor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("StreamConnectionEstablisher",
                                                                                                                            FBUtilities.getAvailableProcessors());

    public final UUID planId;
    public final String description;
    private final List<StreamEventHandler> eventListeners = Collections.synchronizedList(new ArrayList<StreamEventHandler>());
    private final Set<UUID> ongoingSessions;
    private final Map<InetAddress, SessionInfo> sessionStates = new NonBlockingHashMap<>();

    /**
     * Create new StreamResult of given {@code planId} and type.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param description Stream description
     * @param numberOfSessions number of sessions to wait for complete
     */
    private StreamResultFuture(UUID planId, String description, Set<UUID> sessions)
    {
        this.planId = planId;
        this.description = description;
        this.ongoingSessions = sessions;

        // if there is no session to listen to, we immediately set result for returning
        if (sessions.isEmpty())
            set(getCurrentState());
    }

    static StreamResultFuture startStreamingAsync(UUID planId, String description, Collection<StreamSession> sessions)
    {
        Set<UUID> sessionsIds = new HashSet<>(sessions.size());
        for (StreamSession session : sessions)
            sessionsIds.add(session.id);

        StreamResultFuture future = new StreamResultFuture(planId, description, sessionsIds);

        StreamManager.instance.register(future);

        // start sessions
        for (StreamSession session : sessions)
        {
            session.register(future);
            // register to gossiper/FD to fail on node failure
            Gossiper.instance.register(session);
            FailureDetector.instance.registerFailureDetectionEventListener(session);
            streamExecutor.submit(session);
        }
        return future;
    }

    public void addEventListener(StreamEventHandler listener)
    {
        Futures.addCallback(this, listener);
        eventListeners.add(listener);
    }

    /**
     * @return Current snapshot of streaming progress.
     */
    public StreamState getCurrentState()
    {
        return new StreamState(planId, description, ImmutableSet.copyOf(sessionStates.values()));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StreamResultFuture that = (StreamResultFuture) o;
        return planId.equals(that.planId);
    }

    @Override
    public int hashCode()
    {
        return planId.hashCode();
    }

    void handleSessionPrepared(StreamSession session)
    {
        SessionInfo sessionInfo = session.getSessionInfo();
        StreamEvent.SessionPreparedEvent event = new StreamEvent.SessionPreparedEvent(planId, sessionInfo);
        sessionStates.put(sessionInfo.peer, sessionInfo);
        fireStreamEvent(event);
    }

    void handleSessionComplete(StreamSession session)
    {
        Gossiper.instance.unregister(session);
        FailureDetector.instance.unregisterFailureDetectionEventListener(session);

        SessionInfo sessionInfo = session.getSessionInfo();
        sessionStates.put(sessionInfo.peer, sessionInfo);
        fireStreamEvent(new StreamEvent.SessionCompleteEvent(session));
        maybeComplete(session.id);
    }

    public void handleProgress(ProgressInfo progress)
    {
        sessionStates.get(progress.peer).updateProgress(progress);
        fireStreamEvent(new StreamEvent.ProgressEvent(planId, progress));
    }

    void fireStreamEvent(StreamEvent event)
    {
        // delegate to listener
        for (StreamEventHandler listener : eventListeners)
            listener.handleStreamEvent(event);
    }

    private synchronized void maybeComplete(UUID sessionId)
    {
        ongoingSessions.remove(sessionId);
        if (ongoingSessions.isEmpty())
        {
            StreamState finalState = getCurrentState();
            if (finalState.hasFailedSession())
                setException(new StreamException(finalState, "Stream failed"));
            else
                set(finalState);
        }
    }
}
