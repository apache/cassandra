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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.compaction.CompactionInfo;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.pager.QueryPager;
import org.apache.cassandra.transport.Server;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.Refs;

public class ViewBuilder extends CompactionInfo.Holder
{
    private final ColumnFamilyStore baseCfs;
    private final View view;
    private final UUID compactionId;
    private volatile Token prevToken = null;

    private static final Logger logger = LoggerFactory.getLogger(ViewBuilder.class);

    private volatile boolean isStopped = false;

    public ViewBuilder(ColumnFamilyStore baseCfs, View view)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        compactionId = UUIDGen.getTimeUUID();
    }

    private void buildKey(DecoratedKey key)
    {
        AtomicLong noBase = new AtomicLong(Long.MAX_VALUE);
        ReadQuery selectQuery = view.getReadQuery();
        if (!selectQuery.selectsKey(key))
            return;

        QueryPager pager = view.getSelectStatement().internalReadForView(key, FBUtilities.nowInSeconds()).getPager(null, Server.CURRENT_VERSION);

        while (!pager.isExhausted())
        {
           try (ReadExecutionController executionController = pager.executionController();
                PartitionIterator partitionIterator = pager.fetchPageInternal(128, executionController))
           {
               if (!partitionIterator.hasNext())
                   return;

               try (RowIterator rowIterator = partitionIterator.next())
               {
                   FilteredPartition partition = FilteredPartition.create(rowIterator);
                   TemporalRow.Set temporalRows = view.getTemporalRowSet(partition, null, true);

                   Collection<Mutation> mutations = view.createMutations(partition, temporalRows, true);

                   if (mutations != null)
                       StorageProxy.mutateMV(key.getKey(), mutations, true, noBase);
               }
           }
        }
    }

    public void run()
    {
        UUID localHostId = SystemKeyspace.getLocalHostId();
        String ksname = baseCfs.metadata.ksName, viewName = view.name;

        if (SystemKeyspace.isViewBuilt(ksname, viewName))
        {
            if (!SystemKeyspace.isViewStatusReplicated(ksname, viewName))
                updateDistributed(ksname, viewName, localHostId);
            return;
        }

        Iterable<Range<Token>> ranges = StorageService.instance.getLocalRanges(baseCfs.metadata.ksName);
        final Pair<Integer, Token> buildStatus = SystemKeyspace.getViewBuildStatus(ksname, viewName);
        Token lastToken;
        Function<org.apache.cassandra.db.lifecycle.View, Iterable<SSTableReader>> function;
        if (buildStatus == null)
        {
            baseCfs.forceBlockingFlush();
            function = org.apache.cassandra.db.lifecycle.View.select(SSTableSet.CANONICAL);
            int generation = Integer.MIN_VALUE;

            try (Refs<SSTableReader> temp = baseCfs.selectAndReference(function).refs)
            {
                for (SSTableReader reader : temp)
                {
                    generation = Math.max(reader.descriptor.generation, generation);
                }
            }

            SystemKeyspace.beginViewBuild(ksname, viewName, generation);
            lastToken = null;
        }
        else
        {
            function = new Function<org.apache.cassandra.db.lifecycle.View, Iterable<SSTableReader>>()
            {
                @Nullable
                public Iterable<SSTableReader> apply(org.apache.cassandra.db.lifecycle.View view)
                {
                    Iterable<SSTableReader> readers = org.apache.cassandra.db.lifecycle.View.select(SSTableSet.CANONICAL).apply(view);
                    if (readers != null)
                        return Iterables.filter(readers, ssTableReader -> ssTableReader.descriptor.generation <= buildStatus.left);
                    return null;
                }
            };
            lastToken = buildStatus.right;
        }

        prevToken = lastToken;
        try (Refs<SSTableReader> sstables = baseCfs.selectAndReference(function).refs;
             ReducingKeyIterator iter = new ReducingKeyIterator(sstables))
        {
            SystemDistributedKeyspace.startViewBuild(ksname, viewName, localHostId);
            while (!isStopped && iter.hasNext())
            {
                DecoratedKey key = iter.next();
                Token token = key.getToken();
                if (lastToken == null || lastToken.compareTo(token) < 0)
                {
                    for (Range<Token> range : ranges)
                    {
                        if (range.contains(token))
                        {
                            buildKey(key);

                            if (prevToken == null || prevToken.compareTo(token) != 0)
                            {
                                SystemKeyspace.updateViewBuildStatus(ksname, viewName, key.getToken());
                                prevToken = token;
                            }
                        }
                    }
                    lastToken = null;
                }
            }

            if (!isStopped)
            {
                SystemKeyspace.finishViewBuildStatus(ksname, viewName);
                updateDistributed(ksname, viewName, localHostId);
            }
        }
        catch (Exception e)
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> CompactionManager.instance.submitViewBuilder(this),
                                                         5,
                                                         TimeUnit.MINUTES);
            logger.warn("Materialized View failed to complete, sleeping 5 minutes before restarting", e);
        }
    }

    private void updateDistributed(String ksname, String viewName, UUID localHostId)
    {
        try
        {
            SystemDistributedKeyspace.successfulViewBuild(ksname, viewName, localHostId);
            SystemKeyspace.setViewBuiltReplicated(ksname, viewName);
        }
        catch (Exception e)
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(() -> CompactionManager.instance.submitViewBuilder(this),
                                                         5,
                                                         TimeUnit.MINUTES);
            logger.warn("Failed to updated the distributed status of view, sleeping 5 minutes before retrying", e);
        }
    }

    public CompactionInfo getCompactionInfo()
    {
        long rangesLeft = 0, rangesTotal = 0;
        Token lastToken = prevToken;

        // This approximation is not very accurate, but since we do not have a method which allows us to calculate the
        // percentage of a range covered by a second range, this is the best approximation that we can calculate.
        // Instead, we just count the total number of ranges that haven't been seen by the node (we use the order of
        // the tokens to determine whether they have been seen yet or not), and the total number of ranges that a node
        // has.
        for (Range<Token> range : StorageService.instance.getLocalRanges(baseCfs.keyspace.getName()))
        {
            rangesLeft++;
            rangesTotal++;
            // This will reset rangesLeft, so that the number of ranges left will be less than the total ranges at the
            // end of the method.
            if (lastToken == null || range.contains(lastToken))
                rangesLeft = 0;
        }
        return new CompactionInfo(baseCfs.metadata, OperationType.VIEW_BUILD, rangesLeft, rangesTotal, "ranges", compactionId);
    }

    public void stop()
    {
        isStopped = true;
    }
}
