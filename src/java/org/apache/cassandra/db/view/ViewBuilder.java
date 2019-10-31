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
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

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
import org.apache.cassandra.db.compaction.CompactionInfo.Unit;
import org.apache.cassandra.db.lifecycle.SSTableSet;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.ReducingKeyIterator;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
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

    public ViewBuilder(ColumnFamilyStore baseCfs, View view)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        compactionId = UUIDGen.getTimeUUID();
    }

    private void buildKey(DecoratedKey key)
    {
        ReadQuery selectQuery = view.getReadQuery();

        if (!selectQuery.selectsKey(key))
        {
            logger.trace("Skipping {}, view query filters", key);
            return;
        }

        int nowInSec = FBUtilities.nowInSeconds();
        SinglePartitionReadCommand command = view.getSelectStatement().internalReadForView(key, nowInSec);

        // We're rebuilding everything from what's on disk, so we read everything, consider that as new updates
        // and pretend that there is nothing pre-existing.
        UnfilteredRowIterator empty = UnfilteredRowIterators.noRowsIterator(baseCfs.metadata, key, Rows.EMPTY_STATIC_ROW, DeletionTime.LIVE, false);

        try (ReadExecutionController orderGroup = command.executionController();
             UnfilteredRowIterator data = UnfilteredPartitionIterators.getOnlyElement(command.executeLocally(orderGroup), command))
        {
            Iterator<Collection<Mutation>> mutations = baseCfs.keyspace.viewManager
                                                      .forTable(baseCfs.metadata)
                                                      .generateViewUpdates(Collections.singleton(view), data, empty, nowInSec, true);

            AtomicLong noBase = new AtomicLong(Long.MAX_VALUE);
            mutations.forEachRemaining(m -> StorageProxy.mutateMV(key.getKey(), m, true, noBase, System.nanoTime()));
        }
    }

    public void run()
    {
        logger.debug("Starting view builder for {}.{}", baseCfs.metadata.ksName, view.name);
        logger.trace("Running view builder for {}.{}", baseCfs.metadata.ksName, view.name);
        UUID localHostId = SystemKeyspace.getLocalHostId();
        String ksname = baseCfs.metadata.ksName, viewName = view.name;

        if (SystemKeyspace.isViewBuilt(ksname, viewName))
        {
            logger.debug("View already marked built for {}.{}", baseCfs.metadata.ksName, view.name);
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
            logger.debug("Starting new view build. flushing base table {}.{}", baseCfs.metadata.ksName, baseCfs.name);
            lastToken = null;

            //We don't track the generation number anymore since if a rebuild is stopped and
            //restarted the max generation filter may yield no sstables due to compactions.
            //We only care about max generation *during* a build, not across builds.
            //see CASSANDRA-13405
            SystemKeyspace.beginViewBuild(ksname, viewName, 0);
        }
        else
        {
            lastToken = buildStatus.right;
            logger.debug("Resuming view build from token {}. flushing base table {}.{}", lastToken, baseCfs.metadata.ksName, baseCfs.name);
        }

        baseCfs.forceBlockingFlush();
        function = org.apache.cassandra.db.lifecycle.View.selectFunction(SSTableSet.CANONICAL);

        prevToken = lastToken;
        long keysBuilt = 0;
        try (Refs<SSTableReader> sstables = baseCfs.selectAndReference(function).refs;
             ReducingKeyIterator iter = new ReducingKeyIterator(sstables))
        {
            SystemDistributedKeyspace.startViewBuild(ksname, viewName, localHostId);
            while (!isStopRequested() && iter.hasNext())
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
                            ++keysBuilt;

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

            if (!isStopRequested())
            {
                logger.debug("Marking view({}.{}) as built covered {} keys ", ksname, viewName, keysBuilt);
                SystemKeyspace.finishViewBuildStatus(ksname, viewName);
                updateDistributed(ksname, viewName, localHostId);
            }
            else
            {
                logger.debug("Stopped build for view({}.{}) after covering {} keys", ksname, viewName, keysBuilt);
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
        long rangesCompleted = 0, rangesTotal = 0;
        Token lastToken = prevToken;

        // This approximation is not very accurate, but since we do not have a method which allows us to calculate the
        // percentage of a range covered by a second range, this is the best approximation that we can calculate.
        // Instead, we just count the total number of ranges that haven't been seen by the node (we use the order of
        // the tokens to determine whether they have been seen yet or not), and the total number of ranges that a node
        // has.
        for (Range<Token> range : StorageService.instance.getLocalRanges(baseCfs.keyspace.getName()))
        {
            rangesTotal++;
             if ((lastToken != null) && lastToken.compareTo(range.right) > 0)
                 rangesCompleted++;
          }
         return new CompactionInfo(baseCfs.metadata, OperationType.VIEW_BUILD, rangesCompleted, rangesTotal, Unit.RANGES, compactionId);
    }

    public boolean isGlobal()
    {
        return false;
    }
}
