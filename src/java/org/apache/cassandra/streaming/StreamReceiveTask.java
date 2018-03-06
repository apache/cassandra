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
import java.util.concurrent.Executors;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.JVMStabilityInspector;

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
public class StreamReceiveTask extends StreamTask
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReceiveTask.class);

    private static final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("StreamReceiveTask"));

    private final StreamAggregator aggregator;

    // number of streams to receive
    private final int totalStreams;

    // total size of streams to receive
    private final long totalSize;

    // true if task is done (either completed or aborted)
    private volatile boolean done = false;

    private int remoteStreamsReceived = 0;

    public StreamReceiveTask(StreamSession session, TableId tableId, int totalStreams, long totalSize)
    {
        super(session, tableId);
        this.aggregator = ColumnFamilyStore.getIfExists(tableId).getStreamManager().createIncomingAggregator(session, totalStreams);
        this.totalStreams = totalStreams;
        this.totalSize = totalSize;
    }

    /**
     * Process received file.
     *
     * @param stream Stream received.
     */
    public synchronized void received(IncomingStream stream)
    {
        Preconditions.checkState(!session.isPreview(), "we should never receive sstables when previewing");

        if (done)
        {
            logger.warn("[{}] Received sstable {} on already finished stream received task. Aborting sstable.", session.planId(),
                        stream.getName());
            aggregator.discardStream(stream);
            return;
        }

        remoteStreamsReceived++;
        Preconditions.checkArgument(tableId.equals(stream.getTableId()));
        logger.debug("recevied {} of {} total files", remoteStreamsReceived, totalStreams);

        aggregator.received(stream);

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

    public synchronized StreamAggregator getAggregator()
    {
        if (done)
            throw new RuntimeException(String.format("Stream receive task %s of cf %s already finished.", session.planId(), tableId));
        return aggregator;
    }

    private static class OnCompletionRunnable implements Runnable
    {
        private final StreamReceiveTask task;

        public OnCompletionRunnable(StreamReceiveTask task)
        {
            this.task = task;
        }


        private boolean hasViews(ColumnFamilyStore cfs)
        {
            return !Iterables.isEmpty(View.findAll(cfs.metadata.keyspace, cfs.getTableName()));
        }

        private boolean hasCDC(ColumnFamilyStore cfs)
        {
            return cfs.metadata().params.cdc;
        }

        public void run()
        {
            try
            {
                if (ColumnFamilyStore.getIfExists(task.tableId) == null)
                {
                    // schema was dropped during streaming
                    task.aggregator.abort();
                    task.session.taskCompleted(task);
                    return;
                }

                task.aggregator.finished();;
                task.session.taskCompleted(task);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                task.session.onError(t);
            }
            finally
            {
                task.aggregator.cleanup();
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
        aggregator.abort();
    }
}
