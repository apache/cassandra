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

package org.apache.cassandra.db.streaming;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.lifecycle.LifecycleNewTracker;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.ThrottledUnfilteredIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.view.View;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ISSTableScanner;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SSTableMultiWriter;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.streaming.IncomingStream;
import org.apache.cassandra.streaming.StreamReceiver;
import org.apache.cassandra.streaming.StreamSession;
import org.apache.cassandra.utils.CloseableIterator;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.config.CassandraRelevantProperties.REPAIR_MUTATION_REPAIR_ROWS_PER_BATCH;

public class CassandraStreamReceiver implements StreamReceiver
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraStreamReceiver.class);

    private static final int MAX_ROWS_PER_BATCH = REPAIR_MUTATION_REPAIR_ROWS_PER_BATCH.getInt();

    private final ColumnFamilyStore cfs;
    private final StreamSession session;

    // Transaction tracking new files received
    private final LifecycleTransaction txn;

    //  holds references to SSTables received
    protected final Collection<SSTableReader> sstables;

    protected volatile boolean receivedEntireSSTable;

    private final boolean requiresWritePath;


    public CassandraStreamReceiver(ColumnFamilyStore cfs, StreamSession session, int totalFiles)
    {
        this.cfs = cfs;
        this.session = session;
        // this is an "offline" transaction, as we currently manually expose the sstables once done;
        // this should be revisited at a later date, so that LifecycleTransaction manages all sstable state changes
        this.txn = LifecycleTransaction.offline(OperationType.STREAM);
        this.sstables = new ArrayList<>(totalFiles);
        this.requiresWritePath = requiresWritePath(cfs);
    }

    public static CassandraStreamReceiver fromReceiver(StreamReceiver receiver)
    {
        Preconditions.checkArgument(receiver instanceof CassandraStreamReceiver);
        return (CassandraStreamReceiver) receiver;
    }

    private static CassandraIncomingFile getFile(IncomingStream stream)
    {
        Preconditions.checkArgument(stream instanceof CassandraIncomingFile, "Wrong stream type: {}", stream);
        return (CassandraIncomingFile) stream;
    }

    @Override
    public synchronized void received(IncomingStream stream)
    {
        CassandraIncomingFile file = getFile(stream);

        Collection<SSTableReader> finished = null;
        SSTableMultiWriter sstable = file.getSSTable();
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
        receivedEntireSSTable = file.isEntireSSTable();
    }

    @Override
    public void discardStream(IncomingStream stream)
    {
        CassandraIncomingFile file = getFile(stream);
        Throwables.maybeFail(file.getSSTable().abort(null));
    }

    /**
     * @return a LifecycleNewTracker whose operations are synchronised on this StreamReceiveTask.
     */
    public synchronized LifecycleNewTracker createLifecycleNewTracker()
    {
        return new LifecycleNewTracker()
        {
            @Override
            public void trackNew(SSTable table)
            {
                synchronized (CassandraStreamReceiver.this)
                {
                    txn.trackNew(table);
                }
            }

            @Override
            public void untrackNew(SSTable table)
            {
                synchronized (CassandraStreamReceiver.this)
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


    @Override
    public synchronized void abort()
    {
        sstables.clear();
        txn.abort();
    }

    private boolean hasViews(ColumnFamilyStore cfs)
    {
        return !Iterables.isEmpty(View.findAll(cfs.metadata.keyspace, cfs.getTableName()));
    }

    private boolean hasCDC(ColumnFamilyStore cfs)
    {
        return cfs.metadata().params.cdc;
    }

    // returns true iif it is a cdc table and cdc on repair is enabled.
    private boolean cdcRequiresWriteCommitLog(ColumnFamilyStore cfs)
    {
        return DatabaseDescriptor.isCDCOnRepairEnabled() && hasCDC(cfs);
    }

    /*
     * We have a special path for views and for CDC.
     *
     * For views, since the view requires cleaning up any pre-existing state, we must put all partitions
     * through the same write path as normal mutations. This also ensures any 2is are also updated.
     *
     * For CDC-enabled tables and write path for CDC is enabled, we want to ensure that the mutations are
     * run through the CommitLog, so they can be archived by the CDC process on discard.
     */
    private boolean requiresWritePath(ColumnFamilyStore cfs)
    {
        return cdcRequiresWriteCommitLog(cfs)
               || cfs.streamToMemtable()
               || (session.streamOperation().requiresViewBuild() && hasViews(cfs));
    }

    private void sendThroughWritePath(ColumnFamilyStore cfs, Collection<SSTableReader> readers)
    {
        boolean writeCDCCommitLog = cdcRequiresWriteCommitLog(cfs);
        ColumnFilter filter = ColumnFilter.all(cfs.metadata());
        for (SSTableReader reader : readers)
        {
            Keyspace ks = Keyspace.open(reader.getKeyspaceName());
            // When doing mutation-based repair we split each partition into smaller batches
            // ({@link Stream MAX_ROWS_PER_BATCH}) to avoid OOMing and generating heap pressure
            try (ISSTableScanner scanner = reader.getScanner();
                 CloseableIterator<UnfilteredRowIterator> throttledPartitions = ThrottledUnfilteredIterator.throttle(scanner, MAX_ROWS_PER_BATCH))
            {
                while (throttledPartitions.hasNext())
                {
                    // MV *can* be applied unsafe if there's no CDC on the CFS as we flush
                    // before transaction is done.
                    //
                    // If the CFS has CDC, however, these updates need to be written to the CommitLog
                    // so they get archived into the cdc_raw folder
                    ks.apply(new Mutation(PartitionUpdate.fromIterator(throttledPartitions.next(), filter)),
                             writeCDCCommitLog,
                             true,
                             false);
                }
            }
        }
    }

    public synchronized void finishTransaction()
    {
        txn.finish();
    }

    @Override
    public void finished()
    {
        boolean requiresWritePath = requiresWritePath(cfs);
        Collection<SSTableReader> readers = sstables;

        try (Refs<SSTableReader> refs = Refs.ref(readers))
        {
            if (requiresWritePath)
            {
                sendThroughWritePath(cfs, readers);
            }
            else
            {
                // Validate SSTable-attached indexes that should have streamed in an already complete state. When we
                // don't stream the entire SSTable, validation is unnecessary, as the indexes have just been written
                // via the SSTable flush observer, and an error there would have aborted the streaming transaction.
                if (receivedEntireSSTable)
                    // If we do validate, any exception thrown doing so will also abort the streaming transaction:
                    cfs.indexManager.validateSSTableAttachedIndexes(readers, true, true);

                finishTransaction();

                // add sstables (this will build non-SSTable-attached secondary indexes too, see CASSANDRA-10130)
                logger.debug("[Stream #{}] Received {} sstables from {} ({})", session.planId(), readers.size(), session.peer, readers);
                cfs.addSSTables(readers);

                //invalidate row and counter cache
                if (cfs.isRowCacheEnabled() || cfs.metadata().isCounter())
                {
                    List<Bounds<Token>> boundsToInvalidate = new ArrayList<>(readers.size());
                    readers.forEach(sstable -> boundsToInvalidate.add(new Bounds<Token>(sstable.getFirst().getToken(), sstable.getLast().getToken())));
                    Set<Bounds<Token>> nonOverlappingBounds = Bounds.getNonOverlappingBounds(boundsToInvalidate);

                    if (cfs.isRowCacheEnabled())
                    {
                        int invalidatedKeys = cfs.invalidateRowCache(nonOverlappingBounds);
                        if (invalidatedKeys > 0)
                            logger.debug("[Stream #{}] Invalidated {} row cache entries on table {}.{} after stream " +
                                         "receive task completed.", session.planId(), invalidatedKeys,
                                         cfs.getKeyspaceName(), cfs.getTableName());
                    }

                    if (cfs.metadata().isCounter())
                    {
                        int invalidatedKeys = cfs.invalidateCounterCache(nonOverlappingBounds);
                        if (invalidatedKeys > 0)
                            logger.debug("[Stream #{}] Invalidated {} counter cache entries on table {}.{} after stream " +
                                         "receive task completed.", session.planId(), invalidatedKeys,
                                         cfs.getKeyspaceName(), cfs.getTableName());
                    }
                }
            }
        }
    }

    @Override
    public void cleanup()
    {
        // We don't keep the streamed sstables since we've applied them manually so we abort the txn and delete
        // the streamed sstables.
        if (requiresWritePath)
        {
            cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.STREAMS_RECEIVED);
            abort();
        }
    }
}
