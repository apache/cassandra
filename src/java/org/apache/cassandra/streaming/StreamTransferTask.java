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
import java.util.concurrent.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.streaming.messages.OutgoingStreamMessage;

/**
 * StreamTransferTask sends streams for a given table
 */
public class StreamTransferTask extends StreamTask
{
    private static final Logger logger = LoggerFactory.getLogger(StreamTransferTask.class);
    private static final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("StreamingTransferTaskTimeouts"));

    private final AtomicInteger sequenceNumber = new AtomicInteger(0);
    private boolean aborted = false;

    @VisibleForTesting
    protected final Map<Integer, OutgoingStreamMessage> streams = new HashMap<>();
    private final Map<Integer, ScheduledFuture> timeoutTasks = new HashMap<>();

    private long totalSize;

    public StreamTransferTask(StreamSession session, TableId tableId)
    {
        super(session, tableId);
    }

    public synchronized void addTransferStream(OutgoingStream stream)
    {
        Preconditions.checkArgument(tableId.equals(stream.getTableId()));
        OutgoingStreamMessage message = new OutgoingStreamMessage(tableId, session, stream, sequenceNumber.getAndIncrement());
        message = StreamHook.instance.reportOutgoingStream(session, stream, message);
        streams.put(message.header.sequenceNumber, message);
        totalSize += message.stream.getSize();
    }

    /**
     * Received ACK for stream at {@code sequenceNumber}.
     *
     * @param sequenceNumber sequence number of stream
     */
    public void complete(int sequenceNumber)
    {
        boolean signalComplete;
        synchronized (this)
        {
            ScheduledFuture timeout = timeoutTasks.remove(sequenceNumber);
            if (timeout != null)
                timeout.cancel(false);

            OutgoingStreamMessage stream = streams.remove(sequenceNumber);
            if (stream != null)
                stream.complete();

            logger.debug("recevied sequenceNumber {}, remaining files {}", sequenceNumber, streams.keySet());
            signalComplete = streams.isEmpty();
        }

        // all file sent, notify session this task is complete.
        if (signalComplete)
            session.taskCompleted(this);
    }

    public synchronized void abort()
    {
        if (aborted)
            return;
        aborted = true;

        for (ScheduledFuture future : timeoutTasks.values())
            future.cancel(false);
        timeoutTasks.clear();

        Throwable fail = null;
        for (OutgoingStreamMessage stream : streams.values())
        {
            try
            {
                stream.complete();
            }
            catch (Throwable t)
            {
                if (fail == null) fail = t;
                else fail.addSuppressed(t);
            }
        }
        streams.clear();
        if (fail != null)
            Throwables.propagate(fail);
    }

    public synchronized int getTotalNumberOfFiles()
    {
        return streams.size();
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public synchronized Collection<OutgoingStreamMessage> getFileMessages()
    {
        // We may race between queuing all those messages and the completion of the completion of
        // the first ones. So copy the values to avoid a ConcurrentModificationException
        return new ArrayList<>(streams.values());
    }

    public synchronized OutgoingStreamMessage createMessageForRetry(int sequenceNumber)
    {
        // remove previous time out task to be rescheduled later
        ScheduledFuture future = timeoutTasks.remove(sequenceNumber);
        if (future != null)
            future.cancel(false);
        return streams.get(sequenceNumber);
    }

    /**
     * Schedule timeout task to release reference for stream sent.
     * When not receiving ACK after sending to receiver in given time,
     * the task will release reference.
     *
     * @param sequenceNumber sequence number of stream sent.
     * @param time time to timeout
     * @param unit unit of given time
     * @return scheduled future for timeout task
     */
    public synchronized ScheduledFuture scheduleTimeout(final int sequenceNumber, long time, TimeUnit unit)
    {
        if (!streams.containsKey(sequenceNumber))
            return null;

        ScheduledFuture future = timeoutExecutor.schedule(new Runnable()
        {
            public void run()
            {
                synchronized (StreamTransferTask.this)
                {
                    // remove so we don't cancel ourselves
                    timeoutTasks.remove(sequenceNumber);
                    StreamTransferTask.this.complete(sequenceNumber);
                }
            }
        }, time, unit);

        ScheduledFuture prev = timeoutTasks.put(sequenceNumber, future);
        assert prev == null;
        return future;
    }
}
