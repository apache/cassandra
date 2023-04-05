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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.compaction.CompactionInterruptedException;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replicas;
import org.apache.cassandra.schema.SystemDistributedKeyspace;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

import static java.util.stream.Collectors.toList;

/**
 * Builds a materialized view for the local token ranges.
 * <p>
 * The build is split in at least {@link #NUM_TASKS} {@link ViewBuilderTask tasks}, suitable of being parallelized by
 * the {@link CompactionManager} which will execute them.
 */
class ViewBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(ViewBuilder.class);

    private static final int NUM_TASKS = Runtime.getRuntime().availableProcessors() * 4;

    private final ColumnFamilyStore baseCfs;
    private final View view;
    private final String ksName;
    private final UUID localHostId = SystemKeyspace.getOrInitializeLocalHostId();
    private final Set<Range<Token>> builtRanges = Sets.newConcurrentHashSet();
    private final Map<Range<Token>, Pair<Token, Long>> pendingRanges = Maps.newConcurrentMap();
    private final Set<ViewBuilderTask> tasks = Sets.newConcurrentHashSet();
    private volatile long keysBuilt = 0;
    private volatile boolean isStopped = false;
    private volatile Future<?> future = ImmediateFuture.success(null);

    ViewBuilder(ColumnFamilyStore baseCfs, View view)
    {
        this.baseCfs = baseCfs;
        this.view = view;
        ksName = baseCfs.metadata.keyspace;
    }

    public void start()
    {
        if (SystemKeyspace.isViewBuilt(ksName, view.name))
        {
            logger.debug("View already marked built for {}.{}", ksName, view.name);
            if (!SystemKeyspace.isViewStatusReplicated(ksName, view.name))
                updateDistributed();
        }
        else
        {
            SystemDistributedKeyspace.startViewBuild(ksName, view.name, localHostId);

            logger.debug("Starting build of view({}.{}). Flushing base table {}.{}",
                         ksName, view.name, ksName, baseCfs.name);
            baseCfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.VIEW_BUILD_STARTED);

            loadStatusAndBuild();
        }
    }

    private void loadStatusAndBuild()
    {
        loadStatus();
        build();
    }

    private void loadStatus()
    {
        builtRanges.clear();
        pendingRanges.clear();
        SystemKeyspace.getViewBuildStatus(ksName, view.name)
                      .forEach((range, pair) ->
                               {
                                   Token lastToken = pair.left;
                                   if (lastToken != null && lastToken.equals(range.right))
                                   {
                                       builtRanges.add(range);
                                       keysBuilt += pair.right;
                                   }
                                   else
                                   {
                                       pendingRanges.put(range, pair);
                                   }
                               });
    }

    private synchronized void build()
    {
        if (isStopped)
        {
            logger.debug("Stopped build for view({}.{}) after covering {} keys", ksName, view.name, keysBuilt);
            return;
        }

        // Get the local ranges for which the view hasn't already been built nor it's building
        RangesAtEndpoint replicatedRanges = StorageService.instance.getLocalReplicas(ksName);
        Replicas.temporaryAssertFull(replicatedRanges);
        Set<Range<Token>> newRanges = replicatedRanges.ranges()
                                                      .stream()
                                                      .map(r -> r.subtractAll(builtRanges))
                                                      .flatMap(Set::stream)
                                                      .map(r -> r.subtractAll(pendingRanges.keySet()))
                                                      .flatMap(Set::stream)
                                                      .collect(Collectors.toSet());
        // If there are no new nor pending ranges we should finish the build
        if (newRanges.isEmpty() && pendingRanges.isEmpty())
        {
            finish();
            return;
        }

        // Split the new local ranges and add them to the pending set
        DatabaseDescriptor.getPartitioner()
                          .splitter()
                          .map(s -> s.split(newRanges, NUM_TASKS))
                          .orElse(newRanges)
                          .forEach(r -> pendingRanges.put(r, Pair.<Token, Long>create(null, 0L)));

        // Submit a new view build task for each building range.
        // We keep record of all the submitted tasks to be able of stopping them.
        List<Future<Long>> futures = pendingRanges.entrySet()
                                                  .stream()
                                                  .map(e -> new ViewBuilderTask(baseCfs,
                                                                                view,
                                                                                e.getKey(),
                                                                                e.getValue().left,
                                                                                e.getValue().right))
                                                  .peek(tasks::add)
                                                  .map(CompactionManager.instance::submitViewBuilder)
                                                  .collect(toList());

        // Add a callback to process any eventual new local range and mark the view as built, doing a delayed retry if
        // the tasks don't succeed
        Future<List<Long>> future = FutureCombiner.allOf(futures);
        future.addCallback(new FutureCallback<List<Long>>()
        {
            public void onSuccess(List<Long> result)
            {
                keysBuilt += result.stream().mapToLong(x -> x).sum();
                builtRanges.addAll(pendingRanges.keySet());
                pendingRanges.clear();
                build();
            }

            public void onFailure(Throwable t)
            {
                if (t instanceof CompactionInterruptedException)
                {
                    internalStop(true);
                    keysBuilt = tasks.stream().mapToLong(ViewBuilderTask::keysBuilt).sum();
                    logger.info("Interrupted build for view({}.{}) after covering {} keys", ksName, view.name, keysBuilt);
                }
                else
                {
                    ScheduledExecutors.nonPeriodicTasks.schedule(() -> loadStatusAndBuild(), 5, TimeUnit.MINUTES);
                    logger.warn("Materialized View failed to complete, sleeping 5 minutes before restarting", t);
                }
            }
        });
        this.future = future;
    }

    private void finish()
    {
        logger.debug("Marking view({}.{}) as built after covering {} keys ", ksName, view.name, keysBuilt);
        SystemKeyspace.finishViewBuildStatus(ksName, view.name);
        updateDistributed();
    }

    private void updateDistributed()
    {
        try
        {
            SystemDistributedKeyspace.successfulViewBuild(ksName, view.name, localHostId);
            SystemKeyspace.setViewBuiltReplicated(ksName, view.name);
        }
        catch (Exception e)
        {
            ScheduledExecutors.nonPeriodicTasks.schedule(this::updateDistributed, 5, TimeUnit.MINUTES);
            logger.warn("Failed to update the distributed status of view, sleeping 5 minutes before retrying", e);
        }
    }

    /**
     * Stops the view building.
     */
    void stop()
    {
        boolean wasStopped;
        synchronized (this)
        {
            wasStopped = isStopped;
            internalStop(false);
        }
        // TODO: very unclear what the goal is here. why do we wait only if we were the first to invoke stop?
        // but we wait outside the synchronized block to avoid a deadlock with `build` in the future callback
        if (!wasStopped)
            FBUtilities.waitOnFuture(future);
    }

    private void internalStop(boolean isCompactionInterrupted)
    {
        isStopped = true;
        tasks.forEach(task -> task.stop(isCompactionInterrupted));
    }
}
