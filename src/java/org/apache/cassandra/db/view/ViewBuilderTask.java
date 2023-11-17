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

package org.apache.cassandra.db.view;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterators;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.TimeUUID;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class ViewBuilderTask extends CompactionInfo.Holder implements Callable<Long>
{
    private static final Logger logger = LoggerFactory.getLogger(ViewBuilderTask.class);

    private static final int ROWS_BETWEEN_CHECKPOINTS = 1000;

    private final ColumnFamilyStore baseCfs;
    private final View view;
    private final Range<Token> range;
    private final TimeUUID compactionId;
    private volatile Token prevToken;
    private volatile long keysBuilt = 0;
    private volatile boolean isStopped = false;
    private volatile boolean isCompactionInterrupted = false;

    @VisibleForTesting
    public ViewBuilderTask(ColumnFamilyStore baseCfs, View view, Range<Token> range, Token lastToken, long keysBuilt)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        this.range = range;
        this.compactionId = nextTimeUUID();
        this.prevToken = lastToken;
        this.keysBuilt = keysBuilt;
    }

    private void buildKey(DecoratedKey key)
    {
        ReadQuery selectQuery = view.getReadQuery();

        if (!selectQuery.selectsKey(key))
        {
            logger.trace("Skipping {}, view query filters", key);
            return;
        }

        long nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = view.getSelectStatement().internalReadForView(key, nowInSec);

        // We're rebuilding everything from what's on disk, so we read everything, consider that as new updates
        // and pretend that there is nothing pre-existing.
        UnfilteredRowIterator empty = UnfilteredRowIterators.noRowsIterator(baseCfs.metadata(), key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, false);

        try (ReadExecutionController orderGroup = command.executionController();
             UnfilteredRowIterator data = UnfilteredPartitionIterators.getOnlyElement(command.executeLocally(orderGroup), command))
        {
            Iterator<Collection<Mutation>> mutations = baseCfs.keyspace.viewManager
                                                       .forTable(baseCfs.metadata.id)
                                                       .generateViewUpdates(Collections.singleton(view), data, empty, nowInSec, true);

            AtomicLong noBase = new AtomicLong(Long.MAX_VALUE);
            mutations.forEachRemaining(m -> StorageProxy.mutateMV(key.getKey(), m, true, noBase, nanoTime()));
        }
    }

    public Long call()
    {
        String ksName = baseCfs.metadata.keyspace;

        if (prevToken == null)
            logger.debug("Starting new view build for range {}", range);
        else
            logger.debug("Resuming view build for range {} from token {} with {} covered keys", range, prevToken, keysBuilt);

        /*
         * It's possible for view building to start before MV creation got propagated to other nodes. For this reason
         * we should wait for schema to converge before attempting to send any view mutations to other nodes, or else
         * face UnknownTableException upon Mutation deserialization on the nodes that haven't processed the schema change.
         */
        boolean schemaConverged = Gossiper.instance.waitForSchemaAgreement(10, TimeUnit.SECONDS, () -> this.isStopped);
        if (!schemaConverged)
            logger.warn("Failed to get schema to converge before building view {}.{}", baseCfs.getKeyspaceName(), view.name);

        Function<org.apache.cassandra.db.lifecycle.View, Iterable<SSTableReader>> function;
        function = org.apache.cassandra.db.lifecycle.View.select(SSTableSet.CANONICAL, s -> range.intersects(s.getBounds()));

        try (ColumnFamilyStore.RefViewFragment viewFragment = baseCfs.selectAndReference(function);
             Refs<SSTableReader> sstables = viewFragment.refs;
             ReducingKeyIterator keyIter = new ReducingKeyIterator(sstables))
        {
            PeekingIterator<DecoratedKey> iter = Iterators.peekingIterator(keyIter);
            while (!isStopped && iter.hasNext())
            {
                DecoratedKey key = iter.next();
                Token token = key.getToken();
                //skip tokens already built or not present in range
                if (range.contains(token) && (prevToken == null || token.compareTo(prevToken) > 0))
                {
                    buildKey(key);
                    ++keysBuilt;
                    //build other keys sharing the same token
                    while (iter.hasNext() && iter.peek().getToken().equals(token))
                    {
                        key = iter.next();
                        buildKey(key);
                        ++keysBuilt;
                    }
                    if (keysBuilt % ROWS_BETWEEN_CHECKPOINTS == 1)
                        SystemKeyspace.updateViewBuildStatus(ksName, view.name, range, token, keysBuilt);
                    prevToken = token;
                }
            }
        }

        finish();

        return keysBuilt;
    }

    private void finish()
    {
        String ksName = baseCfs.getKeyspaceName();
        if (!isStopped)
        {
            // Save the completed status using the end of the range as last token. This way it will be possible for
            // future view build attempts to don't even create a task for this range
            SystemKeyspace.updateViewBuildStatus(ksName, view.name, range, range.right, keysBuilt);

            logger.debug("Completed build of view({}.{}) for range {} after covering {} keys ", ksName, view.name, range, keysBuilt);
        }
        else
        {
            logger.debug("Stopped build for view({}.{}) for range {} after covering {} keys", ksName, view.name, range, keysBuilt);

            // If it's stopped due to a compaction interruption we should throw that exception.
            // Otherwise we assume that the task has been stopped due to a schema update and we can finish successfully.
            if (isCompactionInterrupted)
                throw new StoppedException(ksName, view.name, getCompactionInfo());
        }
    }

    @Override
    public CompactionInfo getCompactionInfo()
    {
        // we don't know the sstables at construction of ViewBuilderTask and we could change this to return once we know the
        // but since we basically only cancel view builds on truncation where we cancel all compactions anyway, this seems reasonable

        // If there's splitter, calculate progress based on last token position
        if (range.left.getPartitioner().splitter().isPresent())
        {
            long progress = prevToken == null ? 0 : Math.round(prevToken.getPartitioner().splitter().get().positionInRange(prevToken, range) * 1000);
            return CompactionInfo.withoutSSTables(baseCfs.metadata(), OperationType.VIEW_BUILD, progress, 1000, Unit.RANGES, compactionId);
        }

        // When there is no splitter, estimate based on number of total keys but
        // take the max with keysBuilt + 1 to avoid having more completed than total
        long keysTotal = Math.max(keysBuilt + 1, baseCfs.estimatedKeysForRange(range));
        return CompactionInfo.withoutSSTables(baseCfs.metadata(), OperationType.VIEW_BUILD, keysBuilt, keysTotal, Unit.KEYS, compactionId);
    }

    @Override
    public void stop()
    {
        stop(true);
    }

    public boolean isGlobal()
    {
        return false;
    }

    synchronized void stop(boolean isCompactionInterrupted)
    {
        isStopped = true;
        this.isCompactionInterrupted = isCompactionInterrupted;
    }

    long keysBuilt()
    {
        return keysBuilt;
    }

    /**
     * {@link CompactionInterruptedException} with {@link Object#equals(Object)} and {@link Object#hashCode()}
     * implementations that consider equals all the exceptions produced by the same view build, independently of their
     * token range.
     * <p>
     * This is used to avoid Guava's {@link Futures#allAsList(Iterable)} log spamming when multiple build tasks fail
     * due to compaction interruption.
     */
    static class StoppedException extends CompactionInterruptedException
    {
        private final String ksName, viewName;

        private StoppedException(String ksName, String viewName, CompactionInfo info)
        {
            super(info);
            this.ksName = ksName;
            this.viewName = viewName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (!(o instanceof StoppedException))
                return false;

            StoppedException that = (StoppedException) o;
            return Objects.equal(this.ksName, that.ksName) && Objects.equal(this.viewName, that.viewName);
        }

        @Override
        public int hashCode()
        {
            return 31 * ksName.hashCode() + viewName.hashCode();
        }
    }
}
