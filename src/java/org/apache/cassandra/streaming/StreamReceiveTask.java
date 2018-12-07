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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.common.collect.Iterables;

import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;

/**
 * Task that manages receiving files for the session for certain ColumnFamily.
 */
public class StreamReceiveTask extends StreamTask
{
    private static final Logger logger = LoggerFactory.getLogger(StreamReceiveTask.class);

    private static final ExecutorService executor = Executors.newCachedThreadPool(new NamedThreadFactory("StreamReceiveTask"));

    // number of files to receive
    private final int totalFiles;
    // total size of files to receive
    private final long totalSize;

    // Transaction tracking new files received
    private final LifecycleTransaction txn;

    // true if task is done (either completed or aborted)
    private volatile boolean done = false;

    //  holds references to SSTables received
    protected Collection<SSTableReader> sstables;

    private int remoteSSTablesReceived = 0;

    public StreamReceiveTask(StreamSession session, UUID cfId, int totalFiles, long totalSize)
    {
        super(session, cfId);
        this.totalFiles = totalFiles;
        this.totalSize = totalSize;
        // this is an "offline" transaction, as we currently manually expose the sstables once done;
        // this should be revisited at a later date, so that LifecycleTransaction manages all sstable state changes
        this.txn = LifecycleTransaction.offline(OperationType.STREAM);
        this.sstables = new ArrayList<>(totalFiles);
    }

    /**
     * Process received file.
     *
     * @param sstable SSTable file received.
     */
    public synchronized void received(SSTableMultiWriter sstable)
    {
        if (done)
        {
            logger.warn("[{}] Received sstable {} on already finished stream received task. Aborting sstable.", session.planId(),
                        sstable.getFilename());
            Throwables.maybeFail(sstable.abort(null));
            return;
        }

        remoteSSTablesReceived++;
        assert cfId.equals(sstable.getCfId());

        Collection<SSTableReader> finished = null;
        try
        {
            finished = sstable.finish(true);
        }
        catch (Throwable t)
        {
            Throwables.maybeFail(sstable.abort(t));
        }
        txn.update(finished, false);
        sstables.addAll(finished);

        if (remoteSSTablesReceived == totalFiles)
        {
            done = true;
            executor.submit(new OnCompletionRunnable(this));
        }
    }

    public int getTotalNumberOfFiles()
    {
        return totalFiles;
    }

    public long getTotalSize()
    {
        return totalSize;
    }

    /**
     * @return a LifecycleNewTracker whose operations are synchronised on this StreamReceiveTask.
     */
    public synchronized LifecycleNewTracker createLifecycleNewTracker()
    {
        if (done)
            throw new RuntimeException(String.format("Stream receive task %s of cf %s already finished.", session.planId(), cfId));

        return new LifecycleNewTracker()
        {
            @Override
            public void trackNew(SSTable table)
            {
                synchronized (StreamReceiveTask.this)
                {
                    txn.trackNew(table);
                }
            }

            @Override
            public void untrackNew(SSTable table)
            {
                synchronized (StreamReceiveTask.this)
                {
                    txn.untrackNew(table);
                }
            }

            public OperationType opType()
            {
                return txn.opType();
            }
        };
    }

    private static class OnCompletionRunnable implements Runnable
    {
        private final StreamReceiveTask task;

        public OnCompletionRunnable(StreamReceiveTask task)
        {
            this.task = task;
        }

        public void run()
        {
            boolean hasViews = false;
            boolean hasCDC = false;
            ColumnFamilyStore cfs = null;
            try
            {
                Pair<String, String> kscf = Schema.instance.getCF(task.cfId);
                if (kscf == null)
                {
                    // schema was dropped during streaming
                    task.sstables.clear();
                    task.abortTransaction();
                    task.session.taskCompleted(task);
                    return;
                }
                cfs = Keyspace.open(kscf.left).getColumnFamilyStore(kscf.right);
                hasViews = !Iterables.isEmpty(View.findAll(kscf.left, kscf.right));
                hasCDC = cfs.metadata.params.cdc;

                Collection<SSTableReader> readers = task.sstables;

                try (Refs<SSTableReader> refs = Refs.ref(readers))
                {
                    /*
                     * We have a special path for views and for CDC.
                     *
                     * For views, since the view requires cleaning up any pre-existing state, we must put all partitions
                     * through the same write path as normal mutations. This also ensures any 2is are also updated.
                     *
                     * For CDC-enabled tables, we want to ensure that the mutations are run through the CommitLog so they
                     * can be archived by the CDC process on discard.
                     */
                    if (hasViews || hasCDC)
                    {
                        for (SSTableReader reader : readers)
                        {
                            Keyspace ks = Keyspace.open(reader.getKeyspaceName());
                            try (ISSTableScanner scanner = reader.getScanner())
                            {
                                while (scanner.hasNext())
                                {
                                    try (UnfilteredRowIterator rowIterator = scanner.next())
                                    {
                                        Mutation m = new Mutation(PartitionUpdate.fromIterator(rowIterator, ColumnFilter.all(cfs.metadata)));

                                        // MV *can* be applied unsafe if there's no CDC on the CFS as we flush below
                                        // before transaction is done.
                                        //
                                        // If the CFS has CDC, however, these updates need to be written to the CommitLog
                                        // so they get archived into the cdc_raw folder
                                        ks.apply(m, hasCDC, true, false);
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        task.finishTransaction();

                        logger.debug("[Stream #{}] Received {} sstables from {} ({})", task.session.planId(), readers.size(), task.session.peer, readers);
                        // add sstables and build secondary indexes
                        cfs.addSSTables(readers);
                        cfs.indexManager.buildAllIndexesBlocking(readers);

                        //invalidate row and counter cache
                        if (cfs.isRowCacheEnabled() || cfs.metadata.isCounter())
                        {
                            List<Bounds<Token>> boundsToInvalidate = new ArrayList<>(readers.size());
                            readers.forEach(sstable -> boundsToInvalidate.add(new Bounds<Token>(sstable.first.getToken(), sstable.last.getToken())));
                            Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(boundsToInvalidate);

                            if (cfs.isRowCacheEnabled())
                            {
                                int invalidatedKeys = cfs.invalidateRowCache(nonOverlappingBounds);
                                if (invalidatedKeys > 0)
                                    logger.debug("[Stream #{}] Invalidated {} row cache entries on table {}.{} after stream " +
                                                 "receive task completed.", task.session.planId(), invalidatedKeys,
                                                 cfs.keyspace.getName(), cfs.getTableName());
                            }

                            if (cfs.metadata.isCounter())
                            {
                                int invalidatedKeys = cfs.invalidateCounterCache(nonOverlappingBounds);
                                if (invalidatedKeys > 0)
                                    logger.debug("[Stream #{}] Invalidated {} counter cache entries on table {}.{} after stream " +
                                                 "receive task completed.", task.session.planId(), invalidatedKeys,
                                                 cfs.keyspace.getName(), cfs.getTableName());
                            }
                        }
                    }
                }
                task.session.taskCompleted(task);
            }
            catch (Throwable t)
            {
                JVMStabilityInspector.inspectThrowable(t);
                task.session.onError(t);
            }
            finally
            {
                // We don't keep the streamed sstables since we've applied them manually so we abort the txn and delete
                // the streamed sstables.
                if (hasViews || hasCDC)
                {
                    if (cfs != null)
                        cfs.forceBlockingFlush();
                    task.abortTransaction();
                }
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
        abortTransaction();
        sstables.clear();
    }

    private synchronized void abortTransaction()
    {
        txn.abort();
    }

    private synchronized void finishTransaction()
    {
        txn.finish();
    }
}
