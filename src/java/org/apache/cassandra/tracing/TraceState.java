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
package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.MessageFormatter;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.WrappedRunnable;
import org.apache.cassandra.utils.progress.ProgressEvent;
import org.apache.cassandra.utils.progress.ProgressEventNotifier;
import org.apache.cassandra.utils.progress.ProgressListener;

/**
 * ThreadLocal state for a tracing session. The presence of an instance of this class as a ThreadLocal denotes that an
 * operation is being traced.
 */
public class TraceState implements ProgressEventNotifier
{
    private static final Logger logger = LoggerFactory.getLogger(TraceState.class);
    private static final int WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS =
    Integer.valueOf(System.getProperty("cassandra.wait_for_tracing_events_timeout_secs", "0"));

    public final UUID sessionId;
    public final InetAddress coordinator;
    public final Stopwatch watch;
    public final ByteBuffer sessionIdBytes;
    public final Tracing.TraceType traceType;
    public final int ttl;

    private boolean notify;
    private final List<ProgressListener> listeners = new CopyOnWriteArrayList<>();
    private String tag;

    public enum Status
    {
        IDLE,
        ACTIVE,
        STOPPED
    }

    private volatile Status status;

    // Multiple requests can use the same TraceState at a time, so we need to reference count.
    // See CASSANDRA-7626 for more details.
    private final AtomicInteger references = new AtomicInteger(1);

    public TraceState(InetAddress coordinator, UUID sessionId, Tracing.TraceType traceType)
    {
        assert coordinator != null;
        assert sessionId != null;

        this.coordinator = coordinator;
        this.sessionId = sessionId;
        sessionIdBytes = ByteBufferUtil.bytes(sessionId);
        this.traceType = traceType;
        this.ttl = traceType.getTTL();
        watch = Stopwatch.createStarted();
        this.status = Status.IDLE;
}

    /**
     * Activate notification with provided {@code tag} name.
     *
     * @param tag Tag name to add when emitting notification
     */
    public void enableActivityNotification(String tag)
    {
        assert traceType == Tracing.TraceType.REPAIR;
        notify = true;
        this.tag = tag;
    }

    @Override
    public void addProgressListener(ProgressListener listener)
    {
        assert traceType == Tracing.TraceType.REPAIR;
        listeners.add(listener);
    }

    @Override
    public void removeProgressListener(ProgressListener listener)
    {
        assert traceType == Tracing.TraceType.REPAIR;
        listeners.remove(listener);
    }

    public int elapsed()
    {
        long elapsed = watch.elapsed(TimeUnit.MICROSECONDS);
        return elapsed < Integer.MAX_VALUE ? (int) elapsed : Integer.MAX_VALUE;
    }

    public synchronized void stop()
    {
        waitForPendingEvents();

        status = Status.STOPPED;
        notifyAll();
    }

    /*
     * Returns immediately if there has been trace activity since the last
     * call, otherwise waits until there is trace activity, or until the
     * timeout expires.
     * @param timeout timeout in milliseconds
     * @return activity status
     */
    public synchronized Status waitActivity(long timeout)
    {
        if (status == Status.IDLE)
        {
            try
            {
                wait(timeout);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException();
            }
        }
        if (status == Status.ACTIVE)
        {
            status = Status.IDLE;
            return Status.ACTIVE;
        }
        return status;
    }

    private synchronized void notifyActivity()
    {
        status = Status.ACTIVE;
        notifyAll();
    }

    public void trace(String format, Object arg)
    {
        trace(MessageFormatter.format(format, arg).getMessage());
    }

    public void trace(String format, Object arg1, Object arg2)
    {
        trace(MessageFormatter.format(format, arg1, arg2).getMessage());
    }

    public void trace(String format, Object[] args)
    {
        trace(MessageFormatter.arrayFormat(format, args).getMessage());
    }

    public void trace(String message)
    {
        if (notify)
            notifyActivity();

        final String threadName = Thread.currentThread().getName();
        final int elapsed = elapsed();

        executeMutation(TraceKeyspace.makeEventMutation(sessionIdBytes, message, elapsed, threadName, ttl));
        if (logger.isTraceEnabled())
            logger.trace("Adding <{}> to trace events", message);

        for (ProgressListener listener : listeners)
        {
            listener.progress(tag, ProgressEvent.createNotification(message));
        }
    }

    static void executeMutation(final Mutation mutation)
    {
        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            protected void runMayThrow() throws Exception
            {
                mutateWithCatch(mutation);
            }
        });
    }

    /**
     * Called from {@link org.apache.cassandra.net.OutboundTcpConnection} for non-local traces (traces
     * that are not initiated by local node == coordinator).
     */
    public static void mutateWithTracing(final ByteBuffer sessionId, final String message, final int elapsed, final int ttl)
    {
        final String threadName = Thread.currentThread().getName();

        StageManager.getStage(Stage.TRACING).execute(new WrappedRunnable()
        {
            public void runMayThrow()
            {
                mutateWithCatch(TraceKeyspace.makeEventMutation(sessionId, message, elapsed, threadName, ttl));
            }
        });
    }

    static void mutateWithCatch(Mutation mutation)
    {
        try
        {
            StorageProxy.mutate(Collections.singletonList(mutation), ConsistencyLevel.ANY);
        }
        catch (OverloadedException e)
        {
            Tracing.logger.warn("Too many nodes are overloaded to save trace events");
        }
    }

    /**
     * Post a no-op event to the TRACING stage, so that we can be sure that any previous mutations
     * have at least been applied to one replica. This works because the tracking executor only
     * has one thread in its pool, see {@link StageManager#tracingExecutor()}.
     */
    protected void waitForPendingEvents()
    {
        if (WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS <= 0)
            return;

        try
        {
            if (logger.isTraceEnabled())
                logger.trace("Waiting for up to {} seconds for trace events to complete",
                             +WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS);

            StageManager.getStage(Stage.TRACING).submit(StageManager.NO_OP_TASK)
                        .get(WAIT_FOR_PENDING_EVENTS_TIMEOUT_SECS, TimeUnit.SECONDS);
        }
        catch (Throwable t)
        {
            JVMStabilityInspector.inspectThrowable(t);
            logger.debug("Failed to wait for tracing events to complete: {}", t);
        }
    }


    public boolean acquireReference()
    {
        while (true)
        {
            int n = references.get();
            if (n <= 0)
                return false;
            if (references.compareAndSet(n, n + 1))
                return true;
        }
    }

    public int releaseReference()
    {
        waitForPendingEvents();
        return references.decrementAndGet();
    }
}
