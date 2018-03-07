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

import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;

/**
 * {@link StreamCoordinator} is a helper class that abstracts away maintaining multiple
 * StreamSession and ProgressInfo instances per peer.
 *
 * This class coordinates multiple SessionStreams per peer in both the outgoing StreamPlan context and on the
 * inbound StreamResultFuture context.
 */
public class StreamCoordinator
{
    private static final Logger logger = LoggerFactory.getLogger(StreamCoordinator.class);

    /**
     * Executor strictly for establishing the initial connections. Once we're connected to the other end the rest of the
     * streaming is handled directly by the {@link StreamingMessageSender}'s incoming and outgoing threads.
     */
    private static final DebuggableThreadPoolExecutor streamExecutor = DebuggableThreadPoolExecutor.createWithFixedPoolSize("StreamConnectionEstablisher",
                                                                                                                            FBUtilities.getAvailableProcessors());
    private final boolean connectSequentially;

    private Map<InetAddressAndPort, HostStreamingData> peerSessions = new HashMap<>();
    private final StreamOperation streamOperation;
    private final int connectionsPerHost;
    private StreamConnectionFactory factory;
    private Iterator<StreamSession> sessionsToConnect = null;
    private final UUID pendingRepair;
    private final PreviewKind previewKind;

    public StreamCoordinator(StreamOperation streamOperation, int connectionsPerHost, StreamConnectionFactory factory,
                             boolean connectSequentially, UUID pendingRepair, PreviewKind previewKind)
    {
        this.streamOperation = streamOperation;
        this.connectionsPerHost = connectionsPerHost;
        this.factory = factory;
        this.connectSequentially = connectSequentially;
        this.pendingRepair = pendingRepair;
        this.previewKind = previewKind;
    }

    public void setConnectionFactory(StreamConnectionFactory factory)
    {
        this.factory = factory;
    }

    /**
     * @return true if any stream session is active
     */
    public synchronized boolean hasActiveSessions()
    {
        for (HostStreamingData data : peerSessions.values())
        {
            if (data.hasActiveSessions())
                return true;
        }
        return false;
    }

    public synchronized Collection<StreamSession> getAllStreamSessions()
    {
        Collection<StreamSession> results = new ArrayList<>();
        for (HostStreamingData data : peerSessions.values())
        {
            results.addAll(data.getAllStreamSessions());
        }
        return results;
    }

    public boolean isReceiving()
    {
        return connectionsPerHost == 0;
    }

    public void connect(StreamResultFuture future)
    {
        if (this.connectSequentially)
            connectSequentially(future);
        else
            connectAllStreamSessions();
    }

    private void connectAllStreamSessions()
    {
        for (HostStreamingData data : peerSessions.values())
            data.connectAllStreamSessions();
    }

    private void connectSequentially(StreamResultFuture future)
    {
        sessionsToConnect = getAllStreamSessions().iterator();
        future.addEventListener(new StreamEventHandler()
        {
            public void handleStreamEvent(StreamEvent event)
            {
                if (event.eventType == StreamEvent.Type.STREAM_PREPARED || event.eventType == StreamEvent.Type.STREAM_COMPLETE)
                    connectNext();
            }

            public void onSuccess(StreamState result)
            {

            }

            public void onFailure(Throwable t)
            {

            }
        });
        connectNext();
    }

    private void connectNext()
    {
        if (sessionsToConnect == null)
            return;

        if (sessionsToConnect.hasNext())
        {
            StreamSession next = sessionsToConnect.next();
            logger.debug("Connecting next session {} with {}.", next.planId(), next.peer.toString());
            streamExecutor.execute(new StreamSessionConnector(next));
        }
        else
            logger.debug("Finished connecting all sessions");
    }

    public synchronized Set<InetAddressAndPort> getPeers()
    {
        return new HashSet<>(peerSessions.keySet());
    }

    public synchronized StreamSession getOrCreateNextSession(InetAddressAndPort peer, InetAddressAndPort connecting)
    {
        return getOrCreateHostData(peer).getOrCreateNextSession(peer, connecting);
    }

    public synchronized StreamSession getOrCreateSessionById(InetAddressAndPort peer, int id, InetAddressAndPort connecting)
    {
        return getOrCreateHostData(peer).getOrCreateSessionById(peer, id, connecting);
    }

    public StreamSession getSessionById(InetAddressAndPort peer, int id)
    {
        return getHostData(peer).getSessionById(id);
    }

    public synchronized void updateProgress(ProgressInfo info)
    {
        getHostData(info.peer).updateProgress(info);
    }

    public synchronized void addSessionInfo(SessionInfo session)
    {
        HostStreamingData data = getOrCreateHostData(session.peer);
        data.addSessionInfo(session);
    }

    public synchronized Set<SessionInfo> getAllSessionInfo()
    {
        Set<SessionInfo> result = new HashSet<>();
        for (HostStreamingData data : peerSessions.values())
        {
            result.addAll(data.getAllSessionInfo());
        }
        return result;
    }

    public synchronized void transferStreams(InetAddressAndPort to, Collection<OutgoingStream> streams)
    {
        HostStreamingData sessionList = getOrCreateHostData(to);

        if (connectionsPerHost > 1)
        {
            List<Collection<OutgoingStream>> buckets = bucketStreams(streams);

            for (Collection<OutgoingStream> bucket : buckets)
            {
                StreamSession session = sessionList.getOrCreateNextSession(to, to);
                session.addTransferStreams(bucket);
            }
        }
        else
        {
            StreamSession session = sessionList.getOrCreateNextSession(to, to);
            session.addTransferStreams(streams);
        }
    }

    private List<Collection<OutgoingStream>> bucketStreams(Collection<OutgoingStream> streams)
    {
        // There's no point in divvying things up into more buckets than we have sstableDetails
        int targetSlices = Math.min(streams.size(), connectionsPerHost);
        int step = Math.round((float) streams.size() / (float) targetSlices);
        int index = 0;

        List<Collection<OutgoingStream>> result = new ArrayList<>();
        List<OutgoingStream> slice = null;

        for (OutgoingStream stream: streams)
        {
            if (index % step == 0)
            {
                slice = new ArrayList<>();
                result.add(slice);
            }
            slice.add(stream);
            ++index;
        }
        return result;
    }

    private HostStreamingData getHostData(InetAddressAndPort peer)
    {
        HostStreamingData data = peerSessions.get(peer);
        if (data == null)
            throw new IllegalArgumentException("Unknown peer requested: " + peer);
        return data;
    }

    private HostStreamingData getOrCreateHostData(InetAddressAndPort peer)
    {
        HostStreamingData data = peerSessions.get(peer);
        if (data == null)
        {
            data = new HostStreamingData();
            peerSessions.put(peer, data);
        }
        return data;
    }

    public UUID getPendingRepair()
    {
        return pendingRepair;
    }

    private static class StreamSessionConnector implements Runnable
    {
        private final StreamSession session;
        public StreamSessionConnector(StreamSession session)
        {
            this.session = session;
        }

        @Override
        public void run()
        {
            session.start();
            logger.info("[Stream #{}, ID#{}] Beginning stream session with {}", session.planId(), session.sessionIndex(), session.peer);
        }
    }

    private class HostStreamingData
    {
        private final Map<Integer, StreamSession> streamSessions = new HashMap<>();
        private final Map<Integer, SessionInfo> sessionInfos = new HashMap<>();

        private int lastReturned = -1;

        public boolean hasActiveSessions()
        {
            for (StreamSession session : streamSessions.values())
            {
                StreamSession.State state = session.state();
                if (state != StreamSession.State.COMPLETE && state != StreamSession.State.FAILED)
                    return true;
            }
            return false;
        }

        public StreamSession getOrCreateNextSession(InetAddressAndPort peer, InetAddressAndPort connecting)
        {
            // create
            if (streamSessions.size() < connectionsPerHost)
            {
                StreamSession session = new StreamSession(streamOperation, peer, connecting, factory, streamSessions.size(), pendingRepair, previewKind);
                streamSessions.put(++lastReturned, session);
                return session;
            }
            // get
            else
            {
                if (lastReturned >= streamSessions.size() - 1)
                    lastReturned = 0;

                return streamSessions.get(lastReturned++);
            }
        }

        public void connectAllStreamSessions()
        {
            for (StreamSession session : streamSessions.values())
            {
                streamExecutor.execute(new StreamSessionConnector(session));
            }
        }

        public Collection<StreamSession> getAllStreamSessions()
        {
            return Collections.unmodifiableCollection(streamSessions.values());
        }

        public StreamSession getOrCreateSessionById(InetAddressAndPort peer, int id, InetAddressAndPort connecting)
        {
            StreamSession session = streamSessions.get(id);
            if (session == null)
            {
                session = new StreamSession(streamOperation, peer, connecting, factory, id, pendingRepair, previewKind);
                streamSessions.put(id, session);
            }
            return session;
        }

        public StreamSession getSessionById(int id)
        {
            return streamSessions.get(id);
        }

        public void updateProgress(ProgressInfo info)
        {
            sessionInfos.get(info.sessionIndex).updateProgress(info);
        }

        public void addSessionInfo(SessionInfo info)
        {
            sessionInfos.put(info.sessionIndex, info);
        }

        public Collection<SessionInfo> getAllSessionInfo()
        {
            return sessionInfos.values();
        }
    }
}
