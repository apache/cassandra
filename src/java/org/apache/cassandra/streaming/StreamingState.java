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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.InetSocketAddress;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Clock;
import org.assertj.core.util.Throwables;
import org.checkerframework.checker.nullness.qual.Nullable;

public class StreamingState implements StreamEventHandler
{
    public enum State { INIT, START, SUCCESS, FAILURE }

    private final long createdAtMillis = Clock.Global.currentTimeMillis();

    private final UUID id;
    private final boolean follower;
    // TODO is this changed after init?  looks like it is based off sessions which get added later?
    private Set<InetSocketAddress> peers;
    private Sessions sessions = Sessions.EMPTY;

    private State state = State.INIT;
    private String completeMessage = null;

    private final long[] stateTimesNanos;
    private volatile long lastUpdatedAtNanos;

    // API for state changes
    public final Phase phase = new Phase();

    public StreamingState(StreamResultFuture result)
    {
        this.id = result.planId;
        this.follower = result.getCoordinator().isFollower();
        this.peers = result.getCoordinator().getPeers();
        this.stateTimesNanos = new long[State.values().length];
        stateTimesNanos[0] = Clock.Global.nanoTime();
    }

    public UUID getId()
    {
        return id;
    }

    public boolean isFollower()
    {
        return follower;
    }

    public Set<InetSocketAddress> getPeers()
    {
        return peers;
    }

    public State getState()
    {
        return state;
    }

    public boolean isComplete()
    {
        switch (state)
        {
            case SUCCESS:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    public float getProgress()
    {
        switch (state)
        {
            case INIT:
                return 0;
            case START:
                return sessions.progress().floatValue();
            case SUCCESS:
            case FAILURE:
                return 1;
            default:
                throw new AssertionError("unknown state: " + state);
        }
    }

    public EnumMap<State, Long> getStateTimesMillis()
    {
        EnumMap<State, Long> map = new EnumMap<>(State.class);
        for (int i = 0; i < stateTimesNanos.length; i++)
        {
            long nanos = stateTimesNanos[i];
            if (nanos != 0)
                map.put(State.values()[i], nanosToMillis(nanos));
        }
        return map;
    }

    public long getDurationMillis()
    {
        long endNanos = lastUpdatedAtNanos;
        if (!isComplete())
            endNanos = Clock.Global.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(endNanos - stateTimesNanos[0]);
    }

    public long getLastUpdatedAtMillis()
    {
        return nanosToMillis(lastUpdatedAtNanos);
    }

    public long getLastUpdatedAtNanos()
    {
        return lastUpdatedAtNanos;
    }

    public String getFailureCause()
    {
        if (state == State.FAILURE)
            return completeMessage;
        return null;
    }

    public String getSuccessMessage()
    {
        if (state == State.SUCCESS)
            return completeMessage;
        return null;
    }

    @Override
    public String toString()
    {
        TableBuilder table = new TableBuilder();
        table.add("id", id.toString());
        table.add("status", getState().name().toLowerCase());
        table.add("progress", (getProgress() * 100) + "%");
        table.add("duration_ms", Long.toString(getDurationMillis()));
        table.add("last_updated_ms", Long.toString(getLastUpdatedAtMillis()));
        table.add("failure_cause", getFailureCause());
        table.add("success_message", getSuccessMessage());
        for (Map.Entry<State, Long> e : getStateTimesMillis().entrySet())
            table.add("status_" + e.getKey().name().toLowerCase() + "_ms", e.toString());
        return table.toString();
    }

    @Override
    public void handleStreamEvent(StreamEvent event)
    {
        StreamResultFuture stream = StreamManager.instance.getReceivingStream(id);
        if (stream != null)
        {
            peers = stream.getCoordinator().getPeers();
            Set<SessionInfo> sessions = stream.getCoordinator().getAllSessionInfo();
            long bytesToReceive = 0;
            long bytesReceived = 0;
            long filesToReceive = 0;
            long filesReceived = 0;
            long bytesToSend = 0;
            long bytesSent = 0;
            long filesToSend = 0;
            long filesSent = 0;
            for (SessionInfo session : sessions)
            {
                bytesToReceive += session.getTotalSizeToReceive();
                bytesReceived += session.getTotalSizeReceived();

                filesToReceive += session.getTotalFilesToReceive();
                filesReceived += session.getTotalFilesReceived();

                bytesToSend += session.getTotalSizeToSend();
                bytesSent += session.getTotalSizeSent();

                filesToSend += session.getTotalFilesToSend();
                filesSent += session.getTotalFilesSent();
            }
            this.sessions = new Sessions(bytesToReceive, bytesReceived,
                                         bytesToSend, bytesSent,
                                         filesToReceive, filesReceived,
                                         filesToSend, filesSent);
        }
        lastUpdatedAtNanos = Clock.Global.nanoTime();
    }

    @Override
    public void onSuccess(@Nullable StreamState state)
    {
        if (state != null)
        {
            peers = state.sessions.stream().map(a -> a.peer).collect(Collectors.toSet());
        }
        updateState(State.SUCCESS);
    }

    @Override
    public void onFailure(Throwable throwable)
    {
        completeMessage = Throwables.getStackTrace(throwable);
        updateState(State.FAILURE);
    }

    private synchronized void updateState(State state)
    {
        this.state = state;
        lastUpdatedAtNanos = Clock.Global.nanoTime();
    }

    private long nanosToMillis(long nanos)
    {
        // nanos - creationTimeNanos = delta since init
        return createdAtMillis + TimeUnit.NANOSECONDS.toMillis(nanos - stateTimesNanos[0]);
    }

    public class Phase
    {
        public void start()
        {
            updateState(State.START);
        }
    }

    public static class Sessions
    {
        public static final Sessions EMPTY = new Sessions(0, 0, 0, 0, 0, 0, 0, 0);

        public final long bytesToReceive, bytesReceived;
        public final long bytesToSend, bytesSent;
        public final long filesToReceive, filesReceived;
        public final long filesToSend, filesSent;

        public Sessions(long bytesToReceive, long bytesReceived, long bytesToSend, long bytesSent, long filesToReceive, long filesReceived, long filesToSend, long filesSent)
        {
            this.bytesToReceive = bytesToReceive;
            this.bytesReceived = bytesReceived;
            this.bytesToSend = bytesToSend;
            this.bytesSent = bytesSent;
            this.filesToReceive = filesToReceive;
            this.filesReceived = filesReceived;
            this.filesToSend = filesToSend;
            this.filesSent = filesSent;
        }

        public BigDecimal receivedBytesPercent()
        {
            return div(bytesReceived, bytesToReceive);
        }

        public BigDecimal sentBytesPercent()
        {
            return div(bytesSent, bytesToSend);
        }

        public BigDecimal progress()
        {
            return div(bytesSent + bytesReceived, bytesToSend + bytesToReceive);
        }

        private static BigDecimal div(long a, long b)
        {
            // not "correct" but its what you would do if this happened...
            if (b == 0)
                return BigDecimal.ZERO;
            return BigDecimal.valueOf(a).divide(BigDecimal.valueOf(b), 4, RoundingMode.HALF_UP);
        }
    }
}
