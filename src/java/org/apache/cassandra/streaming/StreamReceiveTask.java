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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogEntry;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.audit.AuditLogManager;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.JVMStabilityInspector;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.utils.ExecutorUtils.awaitTermination;
import static org.apache.cassandra.utils.ExecutorUtils.shutdown;

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
public class StreamReceiveTask extends StreamTask
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReceiveTask.class);

    private static final ExecutorService executor = executorFactory().pooled("StreamReceiveTask", Integer.MAX_VALUE);

    private final StreamReceiver receiver;

    // number of streams to receive
    private final int totalStreams;

    // total size of streams to receive
    private final long totalSize;

    // true if task is done (either completed or aborted)
    private volatile boolean done = false;

    private int remoteStreamsReceived = 0;
    private long bytesReceived = 0;

    public StreamReceiveTask(StreamSession session, TableId tableId, int totalStreams, long totalSize)
    {
        super(session, tableId);
        this.receiver = ColumnFamilyStore.getIfExists(tableId).getStreamManager().createStreamReceiver(session, totalStreams);
        this.totalStreams = totalStreams;
        this.totalSize = totalSize;
    }

    /**
     * Process received stream.
     *
     * @param stream Stream received.
     */
    public synchronized void received(IncomingStream stream)
    {
        Preconditions.checkState(!session.isPreview(), "we should never receive sstables when previewing");

        if (done)
        {
            logger.warn("[{}] Received stream {} on already finished stream received task. Aborting stream.", session.planId(),
                        stream.getName());
            receiver.discardStream(stream);
            return;
        }

        remoteStreamsReceived += stream.getNumFiles();
        bytesReceived += stream.getSize();
        Preconditions.checkArgument(tableId.equals(stream.getTableId()));
        logger.debug("received {} of {} total files, {} of total bytes {}", remoteStreamsReceived, totalStreams,
                     bytesReceived, stream.getSize());

        receiver.received(stream);

        if (remoteStreamsReceived == totalStreams)
        {
            done = true;
            executor.submit(new OnCompletionRunnable(this));
        }
    }

    public int getTotalNumberOfFiles()
    {
        return totalStreams;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    public synchronized StreamReceiver getReceiver()
    {
        if (done)
            throw new RuntimeException(String.format("Stream receive task %s of cf %s already finished.", session.planId(), tableId));
        return receiver;
    }

    private static class OnCompletionRunnable implements Runnable
    {
        private final StreamReceiveTask task;

        public OnCompletionRunnable(StreamReceiveTask task)
        {
            this.task = task;
        }

        public boolean isStreamingBulkLoad()
        {
            StreamOperation streamOperation = task.session.streamOperation();
            if (streamOperation == null) {
                return false;
            }
            if (streamOperation == StreamOperation.BULK_LOAD) {
                return true;
            }
            return false;
        }

        AuditLogEntry getSstableLoaderAuditLogEntry(ColumnFamilyStore cfs)
        {
           return new AuditLogEntry.Builder(QueryState.forInternalCalls())
                    .setSource(task.session.getConnecting())
                    .setType(AuditLogEntryType.SSTABLELOADER)
                    .setOperation("SSTABLELOADER")
                    .setTimestamp(System.currentTimeMillis())
                    .setScope(cfs.getTableName())
                    .setKeyspace(cfs.keyspace.getName())
                    .build();
        }

        public void run()
        {
            AuditLogManager auditLogManager = AuditLogManager.instance;
            ColumnFamilyStore cfs = null;
            try
            {
                if (ColumnFamilyStore.getIfExists(task.tableId) == null)
                {
                    // schema was dropped during streaming
                    task.receiver.abort();
                    task.session.taskCompleted(task);
                    return;
                }

                task.receiver.finished();
                task.session.taskCompleted(task);

                // We only want to auditLog streaming event if its triggered by SSTABLELOADER
                if (isStreamingBulkLoad())
                {
                    if (auditLogManager.isEnabled())
                    {
                        AuditLogEntry auditEntry = getSstableLoaderAuditLogEntry(cfs);
                        auditLogManager.log(auditEntry);
                    }
                    StreamingMetrics.bulkLoadTaskCompleted.inc();
                }
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                task.session.onError(t);
                if (isStreamingBulkLoad())
                {
                    if (auditLogManager.isEnabled())
                    {
                        AuditLogEntry auditEntry = getSstableLoaderAuditLogEntry(cfs);
                        auditLogManager.logsstableloadfailure(auditEntry, t);
                    }
                    StreamingMetrics.bulkLoadTaskFailed.inc();
                }
            }
            finally
            {
                task.receiver.cleanup();
            }
        }
    }

    /**
     * Abort this task.
     * If the task already received all files and
     * {@link org.apache.cassandra.streaming.StreamReceiveTask.OnCompletionRunnable} task is submitted,
     * then task cannot be aborted.
     */
    public synchronized void abort()
    {
        if (done)
            return;

        done = true;
        receiver.abort();
    }

    @VisibleForTesting
    public static void shutdownAndWait(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
    {
        shutdown(executor);
        awaitTermination(timeout, unit, executor);
    }
}
