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
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cache.IMeasurableMemory;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.tools.nodetool.formatter.TableBuilder;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class StreamingState implements StreamEventHandler, IMeasurableMemory
{
    private static final Logger logger = LoggerFactory.getLogger(StreamingState.class);

    public static final long EMPTY = ObjectSizes.measureDeep(new StreamingState(nextTimeUUID(), StreamOperation.OTHER, false));

    public enum Status
    {INIT, START, SUCCESS, FAILURE}

    private final long createdAtMillis = Clock.Global.currentTimeMillis();

    private final TimeUUID id;
    private final boolean follower;
    private final StreamOperation operation;
    private final Set<InetSocketAddress> peers = Collections.newSetFromMap(new ConcurrentHashMap<>());
    @GuardedBy("this")
    private final Sessions sessions = new Sessions();

    private Status status;
    private String completeMessage = null;

    private final long[] stateTimesNanos;
    private volatile long lastUpdatedAtNanos;

    // API for state changes
    public final Phase phase = new Phase();

    @Override
    public long unsharedHeapSize()
    {
        long costOfPeers = peers().size() * (ObjectSizes.IPV6_SOCKET_ADDRESS_SIZE + 48); // 48 represents the datastructure cost computed by the JOL
        long costOfCompleteMessage = ObjectSizes.sizeOf(completeMessage());
        return costOfPeers + costOfCompleteMessage + EMPTY;
    }

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
        return this.peers;
    }

    public String completeMessage()
    {
        return this.completeMessage;
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

    @VisibleForTesting
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
        lastUpdatedAtNanos = Clock.Global.nanoTime();
    }

    private void streamPrepared(StreamEvent.SessionPreparedEvent event)
    {
        SessionInfo session = event.session;
        peers.add(session.peer);
        // only update stats on ACK to avoid duplication
        if (event.prepareDirection != StreamSession.PrepareDirection.ACK)
            return;
        sessions.bytesToReceive += session.getTotalSizeToReceive();
        sessions.bytesToSend += session.getTotalSizeToSend();

        sessions.filesToReceive += session.getTotalFilesToReceive();
        sessions.filesToSend += session.getTotalFilesToSend();
    }

    private void streamProgress(StreamEvent.ProgressEvent event)
    {
        ProgressInfo info = event.progress;

        if (info.direction == ProgressInfo.Direction.IN)
        {
            // receiving
            sessions.bytesReceived += info.deltaBytes;
            if (info.isCompleted())
                sessions.filesReceived++;
        }
        else
        {
            // sending
            sessions.bytesSent += info.deltaBytes;
            if (info.isCompleted())
                sessions.filesSent++;
        }
    }

    @Override
    public synchronized void onSuccess(@Nullable StreamState state)
    {
        updateState(Status.SUCCESS);
    }

    @Override
    public synchronized void onFailure(Throwable throwable)
    {
        completeMessage = Throwables.getStackTraceAsString(throwable);
        updateState(Status.FAILURE);
        //we know the size is now very different from the estimate so recompute by adding again
        StreamManager.instance.addStreamingStateAgain(this);
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
        public long bytesToReceive, bytesReceived;
        public long bytesToSend, bytesSent;
        public long filesToReceive, filesReceived;
        public long filesToSend, filesSent;

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

        public boolean isEmpty()
        {
            return bytesToReceive == 0 && bytesToSend == 0 && filesToReceive == 0 && filesToSend == 0;
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
