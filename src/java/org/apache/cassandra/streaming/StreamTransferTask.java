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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.Pair;

/**
 * StreamTransferTask sends sections of SSTable files in certain ColumnFamily.
 */
public class StreamTransferTask extends StreamTask
{
    private final ScheduledExecutorService timeoutExecutor = Executors.newSingleThreadScheduledExecutor();

    private final AtomicInteger sequenceNumber = new AtomicInteger(0);

    private final Map<Integer, OutgoingFileMessage> files = new ConcurrentHashMap<>();

    private final Map<Integer, ScheduledFuture> timeoutTasks = new ConcurrentHashMap<>();

    private long totalSize;

    public StreamTransferTask(StreamSession session, UUID cfId)
    {
        super(session, cfId);
    }

    public void addTransferFile(SSTableReader sstable, long estimatedKeys, List<Pair<Long, Long>> sections)
    {
        assert sstable != null && cfId.equals(sstable.metadata.cfId);
        OutgoingFileMessage message = new OutgoingFileMessage(sstable, sequenceNumber.getAndIncrement(), estimatedKeys, sections);
        files.put(message.header.sequenceNumber, message);
        totalSize += message.header.size();
    }

    /**
     * Received ACK for file at {@code sequenceNumber}.
     *
     * @param sequenceNumber sequence number of file
     */
    public synchronized void complete(int sequenceNumber)
    {
        OutgoingFileMessage file = files.remove(sequenceNumber);
        if (file != null)
        {
            file.sstable.releaseReference();
            // all file sent, notify session this task is complete.
            if (files.isEmpty())
            {
                timeoutExecutor.shutdownNow();
                session.taskCompleted(this);
            }
        }
    }

    public void abort()
    {
        for (OutgoingFileMessage file : files.values())
        {
            file.sstable.releaseReference();
        }
        timeoutExecutor.shutdownNow();
    }

    public int getTotalNumberOfFiles()
    {
        return files.size();
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public Collection<OutgoingFileMessage> getFileMessages()
    {
        // We may race between queuing all those messages and the completion of the completion of
        // the first ones. So copy the values to avoid a ConcurrentModificationException
        return new ArrayList<>(files.values());
    }

    public synchronized OutgoingFileMessage createMessageForRetry(int sequenceNumber)
    {
        // remove previous time out task to be rescheduled later
        ScheduledFuture future = timeoutTasks.get(sequenceNumber);
        if (future != null)
            future.cancel(false);
        return files.get(sequenceNumber);
    }

    /**
     * Schedule timeout task to release reference for file sent.
     * When not receiving ACK after sending to receiver in given time,
     * the task will release reference.
     *
     * @param sequenceNumber sequence number of file sent.
     * @param time time to timeout
     * @param unit unit of given time
     * @return scheduled future for timeout task
     */
    public synchronized ScheduledFuture scheduleTimeout(final int sequenceNumber, long time, TimeUnit unit)
    {
        if (timeoutExecutor.isShutdown())
            return null;

        ScheduledFuture future = timeoutExecutor.schedule(new Runnable()
        {
            public void run()
            {
                StreamTransferTask.this.complete(sequenceNumber);
                timeoutTasks.remove(sequenceNumber);
            }
        }, time, unit);
        timeoutTasks.put(sequenceNumber, future);
        return future;
    }
}
