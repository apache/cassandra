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
package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ExclusiveAsyncExecutor;
import accord.api.Journal;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.local.CommandStores;
import accord.local.NodeCommandStoreService;
import accord.local.ShardDistributor;
import accord.utils.RandomSource;
import accord.utils.Reduce;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResult;
import accord.utils.async.AsyncResults;

import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.Shutdownable;
import org.apache.cassandra.config.AccordConfig;
import org.apache.cassandra.config.AccordConfig.QueueShardModel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.journal.Descriptor;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore.DurablyAppliedTo;
import org.apache.cassandra.service.accord.execution.AccordExecutor;
import org.apache.cassandra.service.accord.execution.AccordExecutor.AccordExecutorFactory;
import org.apache.cassandra.service.accord.execution.AccordExecutorAsyncSubmit;
import org.apache.cassandra.service.accord.execution.AccordExecutorSemiSyncSubmit;
import org.apache.cassandra.service.accord.execution.AccordExecutorSignalLoop;
import org.apache.cassandra.service.accord.execution.AccordExecutorSimple;
import org.apache.cassandra.service.accord.execution.AccordExecutorSyncSubmit;

import static org.apache.cassandra.config.AccordConfig.QueueShardModel.THREAD_PER_SHARD;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccord;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.service.accord.execution.AccordExecutor.constant;
import static org.apache.cassandra.service.accord.journal.ReplayMarkers.saveDirectory;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class AccordCommandStores extends CommandStores implements CacheSize, Shutdownable
{
    private static final Logger logger = LoggerFactory.getLogger(AccordCommandStores.class);

    private final AccordExecutor[] executors;
    private final int mask;

    private long cacheSize, workingSetSize;
    private boolean shrinkingOn;

    AccordCommandStores(NodeCommandStoreService node, Agent agent, DataStore store, RandomSource random,
                        ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenerFactory,
                        Journal journal, AccordExecutor[] executors)
    {
        super(node, agent, store, random, journal, shardDistributor, progressLogFactory, listenerFactory,
              AccordCommandStore.factory(id -> executors[id % executors.length]));
        this.executors = executors;
        this.mask = Integer.highestOneBit(executors.length) - 1;

        cacheSize = DatabaseDescriptor.getAccordCacheSizeInMiB() << 20;
        workingSetSize = DatabaseDescriptor.getAccordWorkingSetSizeInMiB() << 20;
        shrinkingOn = DatabaseDescriptor.getAccordCacheShrinkingOn();
        refreshCapacities();
        ScheduledExecutors.scheduledTasks.scheduleWithFixedDelay(() -> {
            for (AccordExecutor executor : executors)
            {
                executor.executeDirectlyWithLock(() -> {
                    executor.cacheExclusive().processNoEvictQueue();
                });
            }
        }, 1L, 1L, TimeUnit.SECONDS);
    }

    static Factory factory()
    {
        return (NodeCommandStoreService time, Agent agent, DataStore store, RandomSource random, Journal journal, ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenersFactory) -> {
            AccordConfig config = getAccord();
            AccordExecutor[] executors = new AccordExecutor[executorShards(config)];
            AccordExecutorFactory factory;
            int maxThreads = Integer.MAX_VALUE;
            switch (config.queue_submission_model)
            {
                default: throw new AssertionError("Unhandled QueueSubmissionModel: " + config.queue_submission_model);
                case SYNC: factory = AccordExecutorSyncSubmit::new; break;
                case SEMI_SYNC: factory = AccordExecutorSemiSyncSubmit::new; break;
                case ASYNC: factory = AccordExecutorAsyncSubmit::new; break;
                case SIGNAL:
                    long spinIntervalNanos = config.queue_spin_interval == null ? 0 : config.queue_spin_interval.to(TimeUnit.NANOSECONDS);
                    long stopCheckIntervalNanos = config.queue_stop_check_interval == null ? 0 : config.queue_stop_check_interval.to(TimeUnit.NANOSECONDS);
                    factory = (executorId, mode, threads, name, agent0) -> new AccordExecutorSignalLoop(executorId, mode, threads, spinIntervalNanos, stopCheckIntervalNanos, TimeUnit.NANOSECONDS, name, agent0);
                    break;
                case EXEC_ST:
                    factory = AccordExecutorSimple::new;
                    maxThreads = 1;
                    break;
            }

            for (int id = 0; id < executors.length; id++)
            {
                QueueShardModel shardModel = config.queue_shard_model;
                String baseName = AccordExecutor.class.getSimpleName() + '[' + id;
                switch (shardModel)
                {
                    case THREAD_PER_SHARD:
                    case THREAD_PER_SHARD_SYNC_QUEUE:
                        executors[id] = factory.get(id, shardModel == THREAD_PER_SHARD ? RUN_WITHOUT_LOCK : RUN_WITH_LOCK, 1, constant(baseName + ']'), agent);
                        break;
                    case THREAD_POOL_PER_SHARD:
                        int threads = Math.min(maxThreads, Math.max(DatabaseDescriptor.getAccordConcurrentOps() / executors.length, 1));
                        executors[id] = factory.get(id, RUN_WITHOUT_LOCK, threads, num -> baseName + ',' + num + ']', agent);
                        break;
                }
            }

            return new AccordCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, listenersFactory, journal, executors);
        };
    }

    @Override
    public ExclusiveAsyncExecutor someExclusiveExecutor()
    {
        int idx = ((int) Thread.currentThread().getId()) & mask;
        return executors[idx].newExclusiveExecutor();
    }

    public synchronized void setCapacity(long bytes)
    {
        cacheSize = bytes;
        refreshCapacities();
    }

    public synchronized void setWorkingSetSize(long bytes)
    {
        workingSetSize = bytes;
        refreshCapacities();
    }

    public synchronized void setCapacityAndWorkingSetSize(long newCacheSize, long newWorkingSetSize)
    {
        cacheSize = newCacheSize;
        workingSetSize = newWorkingSetSize;
        refreshCapacities();
    }

    public synchronized void setShrinking(boolean on)
    {
        shrinkingOn = on;
    }

    @Override
    public long capacity()
    {
        return cacheSize;
    }

    @Override
    public int size()
    {
        int size = 0;
        for (AccordExecutor executor : executors)
            size += executor.size();
        return size;
    }

    @Override
    public long weightedSize()
    {
        long size = 0;
        for (AccordExecutor executor : executors)
            size += executor.weightedSize();
        return size;
    }

    synchronized void refreshCapacities()
    {
        long capacityPerExecutor = cacheSize / executors.length;
        long workingSetPerExecutor = workingSetSize < 0 ? Long.MAX_VALUE : workingSetSize / executors.length;
        for (AccordExecutor executor : executors)
        {
            executor.executeDirectlyWithLock(() -> {
                executor.setCapacity(capacityPerExecutor);
                executor.setWorkingSetSize(workingSetPerExecutor);
                executor.cacheExclusive().setShrinkingOn(shrinkingOn);
            });
        }
    }

    public List<AccordExecutor> executors()
    {
        return Arrays.asList(executors.clone());
    }

    public void waitForQuiescence()
    {
        for (AccordExecutor executor : this.executors)
            executor.waitForQuiescence();
    }

    @Override
    public boolean isTerminated()
    {
        return Stream.of(executors).allMatch(AccordExecutor::isTerminated);
    }

    public Set<TableId> shutdownStores()
    {
        markShuttingDown();
        Set<TableId> tableIds = new HashSet<>();
        List<AsyncResult<Void>> async = new ArrayList<>();
        for (ShardHolder shard : current())
        {
            AccordCommandStore commandStore = (AccordCommandStore) shard.store;
            tableIds.add(commandStore.tableId());
            async.add(commandStore.shutdownAsync());
        }
        if (!async.isEmpty())
            AccordService.getBlocking(AsyncResults.reduce(async, Reduce.toNull()));
        return tableIds;
    }

    public boolean awaitStoreTermination(long deadlineNanos)
    {
        for (ShardHolder shard : current())
        {
            AccordCommandStore commandStore = (AccordCommandStore) shard.store;
            if (!commandStore.awaitTerminationUntil(deadlineNanos))
            {
                logger.warn("{} timeout awaiting durability: {}", commandStore, DurablyAppliedTo.summarise(commandStore.unsafeGetRedundantBefore()));
                return false;
            }
        }
        return true;
    }

    public void shutdownExecutors()
    {
        for (AccordExecutor executor : executors)
            executor.shutdown();
    }

    @Override
    public void shutdown()
    {
        shutdownStores();
        shutdownExecutors();
    }

    @Override
    public Object shutdownNow()
    {
        shutdown();
        return null;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit units) throws InterruptedException
    {
        long deadline = nanoTime() + units.toNanos(timeout);
        for (AccordExecutor executor : executors)
        {
            long wait = Math.max(1, deadline - nanoTime());
            if (!executor.awaitTermination(wait, TimeUnit.NANOSECONDS))
                return false;
        }
        return true;
    }

    public AsyncChain<Boolean> saveState(Descriptor descriptor)
    {
        saveDirectory().createDirectoriesIfNotExists();
        List<AsyncChain<Boolean>> chains = new ArrayList<>();
        for (ShardHolder shard : current())
        {
            AccordCommandStore commandStore = (AccordCommandStore)shard.store;
            chains.add(commandStore.saveState(descriptor));
        }
        return AsyncChains.reduce(chains, Boolean::logicalAnd, true);
    }

    public AsyncChain<List<Map.Entry<Integer, Long>>> restoreState()
    {
        List<AsyncChain<Map.Entry<Integer, Long>>> chains = new ArrayList<>();
        for (ShardHolder shard : current())
        {
            AccordCommandStore commandStore = (AccordCommandStore)shard.store;
            chains.add(commandStore.restoreState());
        }
        return AsyncChains.allOf(chains);
    }

    private static int executorShards(AccordConfig config)
    {
        switch (config.queue_shard_model)
        {
            default: throw new AssertionError("Unhandled queue_shard_model: " + config.queue_shard_model);
            case THREAD_PER_SHARD:
            case THREAD_PER_SHARD_SYNC_QUEUE:
                return config.queue_shard_count.or(DatabaseDescriptor::getAvailableProcessors);
            case THREAD_POOL_PER_SHARD:
                return Math.max(1, config.queue_shard_count.or(DatabaseDescriptor.getAvailableProcessors() / 8));
        }
    }
}
