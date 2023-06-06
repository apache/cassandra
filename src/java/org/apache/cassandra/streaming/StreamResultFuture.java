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

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.AsyncFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.streaming.StreamingChannel.Factory.Global.streamingFactory;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

/**
 * A future on the result ({@link StreamState}) of a streaming plan.
 *
 * In practice, this object also groups all the {@link StreamSession} for the streaming job
 * involved. One StreamSession will be created for every peer involved and said session will
 * handle every streaming (outgoing and incoming) to that peer for this job.
 * <p>
 * The future will return a result once every session is completed (successfully or not). If
 * any session ended up with an error, the future will throw a StreamException.
 * <p>
 * You can attach {@link StreamEventHandler} to this object to listen on {@link StreamEvent}s to
 * track progress of the streaming.
 */
public final class StreamResultFuture extends AsyncFuture<StreamState>
{
    private static final Logger logger = LoggerFactory.getLogger(StreamResultFuture.class);

    public final TimeUUID planId;
    public final StreamOperation streamOperation;
    private final StreamCoordinator coordinator;
    private final Collection<StreamEventHandler> eventListeners = new ConcurrentLinkedQueue<>();
    private final long slowEventsLogTimeoutNanos = DatabaseDescriptor.getStreamingSlowEventsLogTimeout().toNanoseconds();

    /**
     * Create new StreamResult of given {@code planId} and streamOperation.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param streamOperation Stream streamOperation
     */
    public StreamResultFuture(TimeUUID planId, StreamOperation streamOperation, StreamCoordinator coordinator)
    {
        this.planId = planId;
        this.streamOperation = streamOperation;
        this.coordinator = coordinator;

        // if there is no session to listen to, we immediately set result for returning
        if (!coordinator.isFollower() && !coordinator.hasActiveSessions())
            trySuccess(getCurrentState());
    }

    @VisibleForTesting
    public StreamResultFuture(TimeUUID planId, StreamOperation streamOperation, TimeUUID pendingRepair, PreviewKind previewKind)
    {
        this(planId, streamOperation, new StreamCoordinator(streamOperation, 0, streamingFactory(), true, false, pendingRepair, previewKind));
    }

    public static StreamResultFuture createInitiator(TimeUUID planId, StreamOperation streamOperation, Collection<StreamEventHandler> listeners,
                                                     StreamCoordinator coordinator)
    {
        StreamResultFuture future = createAndRegisterInitiator(planId, streamOperation, coordinator);
        if (listeners != null)
        {
            for (StreamEventHandler listener : listeners)
                future.addEventListener(listener);
        }

        logger.info("[Stream #{}] Executing streaming plan for {}", planId,  streamOperation.getDescription());

        // Initialize and start all sessions
        for (final StreamSession session : coordinator.getAllStreamSessions())
        {
            session.init(future);
        }

        coordinator.connect(future);

        return future;
    }

    public static synchronized StreamResultFuture createFollower(int sessionIndex,
                                                                 TimeUUID planId,
                                                                 StreamOperation streamOperation,
                                                                 InetAddressAndPort from,
                                                                 StreamingChannel channel,
                                                                 int messagingVersion,
                                                                 TimeUUID pendingRepair,
                                                                 PreviewKind previewKind)
    {
        StreamResultFuture future = StreamManager.instance.getReceivingStream(planId);
        if (future == null)
        {
            logger.info("[Stream #{} ID#{}] Creating new streaming plan for {} from {} {}", planId, sessionIndex, streamOperation.getDescription(),
                        from, channel.description());

            // The main reason we create a StreamResultFuture on the receiving side is for JMX exposure.
            future = new StreamResultFuture(planId, streamOperation, pendingRepair, previewKind);
            StreamManager.instance.registerFollower(future);
        }
        future.initInbound(from, channel, messagingVersion, sessionIndex);
        logger.info("[Stream #{}, ID#{}] Received streaming plan for {} from {} {}",
                    planId, sessionIndex, streamOperation.getDescription(), from, channel.description());
        return future;
    }

    private static StreamResultFuture createAndRegisterInitiator(TimeUUID planId, StreamOperation streamOperation, StreamCoordinator coordinator)
    {
        StreamResultFuture future = new StreamResultFuture(planId, streamOperation, coordinator);
        StreamManager.instance.registerInitiator(future);
        return future;
    }

    public StreamCoordinator getCoordinator()
    {
        return coordinator;
    }

    private void initInbound(InetAddressAndPort from, StreamingChannel channel, int messagingVersion, int sessionIndex)
    {
        StreamSession session = coordinator.getOrCreateInboundSession(from, channel, messagingVersion, sessionIndex);
        session.init(this);
    }

    @SuppressWarnings("UnstableApiUsage")
    public void addEventListener(StreamEventHandler listener)
    {
        addCallback(listener);
        eventListeners.add(listener);
    }

    /**
     * @return Current snapshot of streaming progress.
     */
    public StreamState getCurrentState()
    {
        return new StreamState(planId, streamOperation, coordinator.getAllSessionInfo());
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

    void handleSessionPrepared(StreamSession session, StreamSession.PrepareDirection prepareDirection)
    {
        SessionInfo sessionInfo = session.getSessionInfo();
        logger.info("[Stream #{} ID#{}] Prepare completed. Receiving {} files({}), sending {} files({})",
                              session.planId(),
                              session.sessionIndex(),
                              sessionInfo.getTotalFilesToReceive(),
                              FBUtilities.prettyPrintMemory(sessionInfo.getTotalSizeToReceive()),
                              sessionInfo.getTotalFilesToSend(),
                              FBUtilities.prettyPrintMemory(sessionInfo.getTotalSizeToSend()));
        StreamEvent.SessionPreparedEvent event = new StreamEvent.SessionPreparedEvent(planId, sessionInfo, prepareDirection);
        coordinator.addSessionInfo(sessionInfo);
        fireStreamEvent(event);
    }

    void handleSessionComplete(StreamSession session)
    {
        logger.info("[Stream #{}] Session with {} is {}", session.planId(), session.peer, session.state().name().toLowerCase());
        fireStreamEvent(new StreamEvent.SessionCompleteEvent(session));
        SessionInfo sessionInfo = session.getSessionInfo();
        coordinator.addSessionInfo(sessionInfo);
        maybeComplete();
    }

    public void handleProgress(ProgressInfo progress)
    {
        coordinator.updateProgress(progress);
        fireStreamEvent(new StreamEvent.ProgressEvent(planId, progress));
    }

    synchronized void fireStreamEvent(StreamEvent event)
    {
        // delegate to listener
        long startNanos = nanoTime();
        for (StreamEventHandler listener : eventListeners)
        {
            try
            {
                listener.handleStreamEvent(event);
            }
            catch (Throwable t)
            {
                logger.warn("Unexpected exception in listern while calling handleStreamEvent", t);
            }
        }
        long totalNanos = nanoTime() - startNanos;
        if (totalNanos > slowEventsLogTimeoutNanos)
            NoSpamLogger.log(logger, NoSpamLogger.Level.WARN, 1, TimeUnit.MINUTES, "Handling streaming events took longer than {}; took {}",
                             () -> new Object[] { Duration.ofNanos(slowEventsLogTimeoutNanos), Duration.ofNanos(totalNanos)});
    }

    private synchronized void maybeComplete()
    {
        if (finishedAllSessions())
        {
            StreamState finalState = getCurrentState();
            if (finalState.hasFailedSession())
            {
                StringBuilder stringBuilder = new StringBuilder();
                stringBuilder.append("Stream failed: ");
                for (SessionInfo info : finalState.sessions())
                {
                    if (info.isFailed())
                        stringBuilder.append("\nSession peer ").append(info.peer).append(' ').append(info.failureReason);
                }
                String message = stringBuilder.toString();
                logger.warn("[Stream #{}] {}", planId, message);
                tryFailure(new StreamException(finalState, message));
            }
            else if (finalState.hasAbortedSession())
            {
                logger.info("[Stream #{}] Stream aborted", planId);
                trySuccess(finalState);
            }
            else
            {
                logger.info("[Stream #{}] All sessions completed", planId);
                trySuccess(finalState);
            }
        }
    }

    public StreamSession getSession(InetAddressAndPort peer, int sessionIndex)
    {
        return coordinator.getSessionById(peer, sessionIndex);
    }

    /**
     * We can't use {@link StreamCoordinator#hasActiveSessions()} directly because {@link this#maybeComplete()}
     * relies on the snapshotted state from {@link StreamCoordinator} and not the {@link StreamSession} state
     * directly (CASSANDRA-15667), otherwise inconsistent snapshotted states may lead to completion races.
     */
    private boolean finishedAllSessions()
    {
        return coordinator.getAllSessionInfo().stream().allMatch(s -> s.state.isFinalState());
    }
}
