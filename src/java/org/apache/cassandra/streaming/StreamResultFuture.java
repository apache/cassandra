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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

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
public final class StreamResultFuture extends AbstractFuture<StreamState>
{
    private static final Logger logger = LoggerFactory.getLogger(StreamResultFuture.class);

    public final UUID planId;
    public final StreamOperation streamOperation;
    private final StreamCoordinator coordinator;
    private final Collection<StreamEventHandler> eventListeners = new ConcurrentLinkedQueue<>();

    /**
     * Create new StreamResult of given {@code planId} and streamOperation.
     *
     * Constructor is package private. You need to use {@link StreamPlan#execute()} to get the instance.
     *
     * @param planId Stream plan ID
     * @param streamOperation Stream streamOperation
     */
    private StreamResultFuture(UUID planId, StreamOperation streamOperation, StreamCoordinator coordinator)
    {
        this.planId = planId;
        this.streamOperation = streamOperation;
        this.coordinator = coordinator;

        // if there is no session to listen to, we immediately set result for returning
        if (!coordinator.isReceiving() && !coordinator.hasActiveSessions())
            set(getCurrentState());
    }

    private StreamResultFuture(UUID planId, StreamOperation streamOperation, boolean keepSSTableLevels, UUID pendingRepair, PreviewKind previewKind)
    {
        this(planId, streamOperation, new StreamCoordinator(0, keepSSTableLevels, new DefaultConnectionFactory(), false, pendingRepair, previewKind));
    }

    static StreamResultFuture init(UUID planId, StreamOperation streamOperation, Collection<StreamEventHandler> listeners,
                                   StreamCoordinator coordinator)
    {
        StreamResultFuture future = createAndRegister(planId, streamOperation, coordinator);
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

    public static synchronized StreamResultFuture initReceivingSide(int sessionIndex,
                                                                    UUID planId,
                                                                    StreamOperation streamOperation,
                                                                    InetAddressAndPort from,
                                                                    Channel channel,
                                                                    boolean keepSSTableLevel,
                                                                    UUID pendingRepair,
                                                                    PreviewKind previewKind)
    {
        StreamResultFuture future = StreamManager.instance.getReceivingStream(planId);
        if (future == null)
        {
            logger.info("[Stream #{} ID#{}] Creating new streaming plan for {}", planId, sessionIndex, streamOperation.getDescription());

            // The main reason we create a StreamResultFuture on the receiving side is for JMX exposure.
            future = new StreamResultFuture(planId, streamOperation, keepSSTableLevel, pendingRepair, previewKind);
            StreamManager.instance.registerReceiving(future);
        }
        future.attachConnection(from, sessionIndex, channel);
        logger.info("[Stream #{}, ID#{}] Received streaming plan for {}", planId, sessionIndex, streamOperation.getDescription());
        return future;
    }

    private static StreamResultFuture createAndRegister(UUID planId, StreamOperation streamOperation, StreamCoordinator coordinator)
    {
        StreamResultFuture future = new StreamResultFuture(planId, streamOperation, coordinator);
        StreamManager.instance.register(future);
        return future;
    }

    public StreamCoordinator getCoordinator()
    {
        return coordinator;
    }

    private void attachConnection(InetAddressAndPort from, int sessionIndex, Channel channel)
    {
        SocketAddress addr = channel.remoteAddress();
        //In the case of unit tests, if you use the EmbeddedChannel, channel.remoteAddress()
        //does not return an InetSocketAddress, but an EmbeddedSocketAddress. Hence why we need the type check here
        InetAddress connecting = (addr instanceof InetSocketAddress ? ((InetSocketAddress) addr).getAddress() : from.address);
        //Need to turn connecting into a InetAddressAndPort with the correct port. I think getting the port from "from"
        //Will work since we don't actually have ports diverge across network interfaces
        StreamSession session = coordinator.getOrCreateSessionById(from, sessionIndex, InetAddressAndPort.getByAddressOverrideDefaults(connecting, from.port));
        session.init(this);
        session.attach(channel);
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

    void handleSessionPrepared(StreamSession session)
    {
        SessionInfo sessionInfo = session.getSessionInfo();
        logger.info("[Stream #{} ID#{}] Prepare completed. Receiving {} files({}), sending {} files({})",
                              session.planId(),
                              session.sessionIndex(),
                              sessionInfo.getTotalFilesToReceive(),
                              FBUtilities.prettyPrintMemory(sessionInfo.getTotalSizeToReceive()),
                              sessionInfo.getTotalFilesToSend(),
                              FBUtilities.prettyPrintMemory(sessionInfo.getTotalSizeToSend()));
        StreamEvent.SessionPreparedEvent event = new StreamEvent.SessionPreparedEvent(planId, sessionInfo);
        coordinator.addSessionInfo(sessionInfo);
        fireStreamEvent(event);
    }

    void handleSessionComplete(StreamSession session)
    {
        logger.info("[Stream #{}] Session with {} is complete", session.planId(), session.peer);
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
        for (StreamEventHandler listener : eventListeners)
            listener.handleStreamEvent(event);
    }

    private synchronized void maybeComplete()
    {
        if (!coordinator.hasActiveSessions())
        {
            StreamState finalState = getCurrentState();
            if (finalState.hasFailedSession())
            {
                logger.warn("[Stream #{}] Stream failed", planId);
                setException(new StreamException(finalState, "Stream failed"));
            }
            else
            {
                logger.info("[Stream #{}] All sessions completed", planId);
                set(finalState);
            }
        }
    }

    StreamSession getSession(InetAddressAndPort peer, int sessionIndex)
    {
        return coordinator.getSessionById(peer, sessionIndex);
    }
}
