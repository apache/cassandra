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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.streaming.messages.OutgoingFileMessage;
import org.apache.cassandra.utils.Pair;

/**
 * StreamTransferTask sends sections of SSTable files in certain ColumnFamily.
 */
public class StreamTransferTask extends StreamTask
{
    private final AtomicInteger sequenceNumber = new AtomicInteger(0);

    private final Map<Integer, OutgoingFileMessage> files = new HashMap<>();

    private long totalSize;

    public StreamTransferTask(StreamSession session, UUID cfId)
    {
        super(session, cfId);
    }

    public void addTransferFile(SSTableReader sstable, long estimatedKeys, List<Pair<Long, Long>> sections, long repairedAt)
    {
        assert sstable != null && cfId.equals(sstable.metadata.cfId);
        OutgoingFileMessage message = new OutgoingFileMessage(sstable, sequenceNumber.getAndIncrement(), estimatedKeys, sections, repairedAt);
        files.put(message.header.sequenceNumber, message);
        totalSize += message.header.size();
    }

    /**
     * Received ACK for file at {@code sequenceNumber}.
     *
     * @param sequenceNumber sequence number of file
     */
    public void complete(int sequenceNumber)
    {
        files.remove(sequenceNumber);
        // all file sent, notify session this task is complete.
        if (files.isEmpty())
            session.taskCompleted(this);
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

    public OutgoingFileMessage createMessageForRetry(int sequenceNumber)
    {
        return files.get(sequenceNumber);
    }
}
