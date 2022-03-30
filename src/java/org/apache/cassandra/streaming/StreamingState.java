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
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;
import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class StreamingState implements StreamEventHandler
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingState.class);

    public static final long ELEMENT_SIZE = ObjectSizes.measureDeep(new StreamingState(nextTimeUUID(), StreamOperation.OTHER, false));

    public enum Status
    {INIT, START, SUCCESS, FAILURE}

    private final long createdAtMillis = Clock.Global.currentTimeMillis();

    // while streaming is running, this is a cache of StreamInfo seen with progress state
    // the reason for the cache is that StreamSession drops data after tasks (recieve/send) complete, this makes
    // it so that current state of a future tracks work pending rather than work done, cache solves this by not deleting
    // when tasks complete
    // To lower memory costs, clear this after the stream completes
    private ConcurrentMap<InetSocketAddress, SessionInfo> streamProgress = new ConcurrentHashMap<>();

    private final TimeUUID id;
    private final boolean follower;
    private final StreamOperation operation;
    private Set<InetSocketAddress> peers = null;
    private Sessions sessions = Sessions.EMPTY;

    private Status status;
    private String completeMessage = null;

    private final long[] stateTimesNanos;
    private volatile long lastUpdatedAtNanos;

    // API for state changes
    public final Phase phase = new Phase();

    public StreamingState(StreamResultFuture result)
    {
        this(result.planId, result.streamOperation, result.getCoordinator().isFollower());
    }

    private StreamingState(TimeUUID planId, StreamOperation streamOperation, boolean follower)
    {
        this.id = planId;
        this.operation = streamOperation;
        this.follower = follower;
        this.stateTimesNanos = new long[Status.values().length];
        updateState(Status.INIT);
    }

    public TimeUUID id()
    {
        return id;
    }

    public boolean follower()
    {
        return follower;
    }

    public StreamOperation operation()
    {
        return operation;
    }

    public Set<InetSocketAddress> peers()
    {
        Set<InetSocketAddress> peers = this.peers;
        if (peers != null)
            return peers;
        ConcurrentMap<InetSocketAddress, SessionInfo> streamProgress = this.streamProgress;
        if (streamProgress != null)
            return streamProgress.keySet();
        return Collections.emptySet();
    }

    public Status status()
    {
        return status;
    }

    public Sessions sessions()
    {
        return sessions;
    }

    public boolean isComplete()
    {
        switch (status)
        {
            case SUCCESS:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    public StreamResultFuture future()
    {
        if (follower)
            return StreamManager.instance.getReceivingStream(id);
        else
            return StreamManager.instance.getInitiatorStream(id);
    }

    public float progress()
    {
        switch (status)
        {
            case INIT:
                return 0;
            case START:
                return Math.min(0.99f, sessions().progress().floatValue());
            case SUCCESS:
            case FAILURE:
                return 1;
            default:
                throw new AssertionError("unknown state: " + status);
        }
    }

    public EnumMap<Status, Long> stateTimesMillis()
    {
        EnumMap<Status, Long> map = new EnumMap<>(Status.class);
        for (int i = 0; i < stateTimesNanos.length; i++)
        {
            long nanos = stateTimesNanos[i];
            if (nanos != 0)
                map.put(Status.values()[i], nanosToMillis(nanos));
        }
        return map;
    }

    public long durationMillis()
    {
        long endNanos = lastUpdatedAtNanos;
        if (!isComplete())
            endNanos = Clock.Global.nanoTime();
        return TimeUnit.NANOSECONDS.toMillis(endNanos - stateTimesNanos[0]);
    }

    public long lastUpdatedAtMillis()
    {
        return nanosToMillis(lastUpdatedAtNanos);
    }

    public long lastUpdatedAtNanos()
    {
        return lastUpdatedAtNanos;
    }

    public String failureCause()
    {
        if (status == Status.FAILURE)
            return completeMessage;
        return null;
    }

    public String successMessage()
    {
        if (status == Status.SUCCESS)
            return completeMessage;
        return null;
    }

    @Override
    public String toString()
    {
        TableBuilder table = new TableBuilder();
        table.add("id", id.toString());
        table.add("status", status().name().toLowerCase());
        table.add("progress", (progress() * 100) + "%");
        table.add("duration_ms", Long.toString(durationMillis()));
        table.add("last_updated_ms", Long.toString(lastUpdatedAtMillis()));
        table.add("failure_cause", failureCause());
        table.add("success_message", successMessage());
        for (Map.Entry<Status, Long> e : stateTimesMillis().entrySet())
            table.add("status_" + e.getKey().name().toLowerCase() + "_ms", e.toString());
        return table.toString();
    }

    @Override
    public synchronized void handleStreamEvent(StreamEvent event)
    {
        ConcurrentMap<InetSocketAddress, SessionInfo> streamProgress = this.streamProgress;
        if (streamProgress == null)
        {
            logger.warn("Got stream event {} after the stream completed", event.eventType);
            return;
        }
        try
        {
            switch (event.eventType)
            {
                case STREAM_PREPARED:
                    streamPrepared((StreamEvent.SessionPreparedEvent) event);
                    break;
                case STREAM_COMPLETE:
                    // currently not taking track of state, so ignore
                    break;
                case FILE_PROGRESS:
                    streamProgress((StreamEvent.ProgressEvent) event);
                    break;
                default:
                    logger.warn("Unknown stream event type: {}", event.eventType);
            }
        }
        catch (Throwable t)
        {
            logger.warn("Unexpected exception handling stream event", t);
        }
        sessions = Sessions.create(streamProgress.values());
        lastUpdatedAtNanos = Clock.Global.nanoTime();
    }

    private void streamPrepared(StreamEvent.SessionPreparedEvent event)
    {
        SessionInfo session = new SessionInfo(event.session);
        streamProgress.putIfAbsent(session.peer, session);
    }

    private void streamProgress(StreamEvent.ProgressEvent event)
    {
        SessionInfo info = streamProgress.get(event.progress.peer);
        if (info != null)
        {
            info.updateProgress(event.progress);
        }
        else
        {
            logger.warn("[Stream #{}} ID#{}] Recieved stream progress before prepare; peer={}", id, event.progress.sessionIndex, event.progress.peer);
        }
    }

    @Override
    public synchronized void onSuccess(@Nullable StreamState state)
    {
        ConcurrentMap<InetSocketAddress, SessionInfo> streamProgress = this.streamProgress;
        if (streamProgress != null)
        {
            sessions = Sessions.create(streamProgress.values());
            peers = new HashSet<>(streamProgress.keySet());
            this.streamProgress = null;
            updateState(Status.SUCCESS);
        }
    }

    @Override
    public synchronized void onFailure(Throwable throwable)
    {
        ConcurrentMap<InetSocketAddress, SessionInfo> streamProgress = this.streamProgress;
        if (streamProgress != null)
        {
            sessions = Sessions.create(streamProgress.values());
            peers = new HashSet<>(streamProgress.keySet());
            this.streamProgress = null;
        }
        completeMessage = Throwables.getStackTraceAsString(throwable);
        updateState(Status.FAILURE);
    }

    private synchronized void updateState(Status state)
    {
        this.status = state;
        long now = Clock.Global.nanoTime();
        stateTimesNanos[state.ordinal()] = now;
        lastUpdatedAtNanos = now;
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
            updateState(Status.START);
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

        public static String columns()
        {
            return "  bytes_to_receive bigint, \n" +
                   "  bytes_received bigint, \n" +
                   "  bytes_to_send bigint, \n" +
                   "  bytes_sent bigint, \n" +
                   "  files_to_receive bigint, \n" +
                   "  files_received bigint, \n" +
                   "  files_to_send bigint, \n" +
                   "  files_sent bigint, \n";
        }

        public static Sessions create(Collection<SessionInfo> sessions)
        {
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
            if (0 == bytesToReceive && 0 == bytesReceived && 0 == filesToReceive && 0 == filesReceived && 0 == bytesToSend && 0 == bytesSent && 0 == filesToSend && 0 == filesSent)
                return EMPTY;
            return new Sessions(bytesToReceive, bytesReceived,
                                bytesToSend, bytesSent,
                                filesToReceive, filesReceived,
                                filesToSend, filesSent);
        }

        public boolean isEmpty()
        {
            return this == EMPTY;
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

        public void update(SimpleDataSet ds)
        {
            if (isEmpty())
                return;
            ds.column("bytes_to_receive", bytesToReceive)
              .column("bytes_received", bytesReceived)
              .column("bytes_to_send", bytesToSend)
              .column("bytes_sent", bytesSent)
              .column("files_to_receive", filesToReceive)
              .column("files_received", filesReceived)
              .column("files_to_send", filesToSend)
              .column("files_sent", filesSent);
        }
    }
}
