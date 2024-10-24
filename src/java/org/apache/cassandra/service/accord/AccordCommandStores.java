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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.topology.Topology;
import accord.utils.RandomSource;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.AccordSpec.QueueShardModel;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordCacheMetrics;
import org.apache.cassandra.metrics.CacheSizeMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordExecutor.AccordExecutorFactory;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.config.AccordSpec.QueueShardModel.THREAD_PER_SHARD;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordQueueSubmissionModel;
import static org.apache.cassandra.config.DatabaseDescriptor.getAccordQueueShardCount;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITHOUT_LOCK;
import static org.apache.cassandra.service.accord.AccordExecutor.Mode.RUN_WITH_LOCK;
import static org.apache.cassandra.service.accord.AccordExecutor.constant;
import static org.apache.cassandra.service.accord.AccordExecutor.constantFactory;

public class AccordCommandStores extends CommandStores implements CacheSize
{
    public static final String ACCORD_STATE_CACHE = "AccordStateCache";

    private final CacheSizeMetrics cacheSizeMetrics;
    private final AccordExecutor[] executors;
    private long cacheSize, workingSetSize;
    private int maxQueuedLoads, maxQueuedRangeLoads;
    private boolean shrinkingOn;

    AccordCommandStores(NodeCommandStoreService node, Agent agent, DataStore store, RandomSource random,
                        ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenerFactory,
                        AccordJournal journal, AccordExecutor[] executors)
    {
        super(node, agent, store, random, shardDistributor, progressLogFactory, listenerFactory,
              AccordCommandStore.factory(journal, id -> executors[id % executors.length]));
        this.executors = executors;
        this.cacheSizeMetrics = new CacheSizeMetrics(ACCORD_STATE_CACHE, this);
        cacheSize = DatabaseDescriptor.getAccordCacheSizeInMiB() << 20;
        workingSetSize = DatabaseDescriptor.getAccordWorkingSetSizeInMiB() << 20;
        maxQueuedLoads = DatabaseDescriptor.getAccordMaxQueuedLoadCount();
        maxQueuedRangeLoads = DatabaseDescriptor.getAccordMaxQueuedRangeLoadCount();
        shrinkingOn = DatabaseDescriptor.getAccordCacheShrinkingOn();
        refreshCapacities();
    }

    static Factory factory(AccordJournal journal)
    {
        return (time, agent, store, random, shardDistributor, progressLogFactory, listenerFactory) -> {
            AccordExecutor[] executors = new AccordExecutor[getAccordQueueShardCount()];
            AccordExecutorFactory factory;
            int maxThreads = Integer.MAX_VALUE;
            switch (getAccordQueueSubmissionModel())
            {
                default: throw new AssertionError("Unhandled QueueSubmissionModel: " + getAccordQueueSubmissionModel());
                case SYNC: factory = AccordExecutorSyncSubmit::new; break;
                case SEMI_SYNC: factory = AccordExecutorSemiSyncSubmit::new; break;
                case ASYNC: factory = AccordExecutorAsyncSubmit::new; break;
                case EXEC_ST:
                    factory = AccordExecutorSimple::new;
                    maxThreads = 1;
                    break;
            }

            for (int id = 0; id < executors.length; id++)
            {
                AccordCacheMetrics metrics = new AccordCacheMetrics(ACCORD_STATE_CACHE);
                QueueShardModel shardModel = DatabaseDescriptor.getAccordQueueShardModel();
                String baseName = AccordExecutor.class.getSimpleName() + '[' + id;
                int threads = Math.min(maxThreads, Math.max(DatabaseDescriptor.getAccordConcurrentOps() / getAccordQueueShardCount(), 1));
                switch (shardModel)
                {
                    case THREAD_PER_SHARD:
                    case THREAD_PER_SHARD_SYNC_QUEUE:
                        executors[id] = factory.get(id, shardModel == THREAD_PER_SHARD ? RUN_WITHOUT_LOCK : RUN_WITH_LOCK, 1, constant(baseName + ']'), metrics, constantFactory(Stage.READ.executor()), constantFactory(Stage.MUTATION.executor()), constantFactory(Stage.READ.executor()), agent);
                        break;
                    case THREAD_POOL_PER_SHARD:
                        executors[id] = factory.get(id, RUN_WITHOUT_LOCK, threads, num -> baseName + ',' + num + ']', metrics, AccordExecutor::submitIOToSelf, AccordExecutor::submitIOToSelf, AccordExecutor::submitIOToSelf, agent);
                        break;
                    case THREAD_POOL_PER_SHARD_EXCLUDES_IO:
                        executors[id] = factory.get(id, RUN_WITHOUT_LOCK, threads, num -> baseName + ',' + num + ']', metrics, constantFactory(Stage.READ.executor()), constantFactory(Stage.MUTATION.executor()), constantFactory(Stage.READ.executor()), agent);
                        break;
                }
            }

            return new AccordCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, listenerFactory, journal, executors);
        };
    }

    @Override
    protected boolean shouldBootstrap(Node node, Topology previous, Topology updated, Range range)
    {
        if (!super.shouldBootstrap(node, previous, updated, range))
            return false;
        // we see new ranges when a new keyspace is added, so avoid bootstrap in these cases
        return contains(previous, ((AccordRoutingKey)  range.start()).table());
    }

    private static boolean contains(Topology previous, TableId searchTable)
    {
        for (Range range : previous.ranges())
        {
            TableId table = ((AccordRoutingKey)  range.start()).table();
            if (table.equals(searchTable))
                return true;
        }
        return false;
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

    public synchronized void setMaxQueuedLoads(int total, int range)
    {
        maxQueuedLoads = total;
        maxQueuedRangeLoads = range;
        refreshCapacities();
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
        int maxLoadsPerExecutor = (maxQueuedLoads + executors.length - 1) / executors.length;
        int maxRangeLoadsPerExecutor = (maxQueuedRangeLoads + executors.length - 1) / executors.length;
        for (AccordExecutor executor : executors)
        {
            executor.executeDirectlyWithLock(() -> {
                executor.setCapacity(capacityPerExecutor);
                executor.setWorkingSetSize(workingSetPerExecutor);
                executor.setMaxQueuedLoads(maxLoadsPerExecutor, maxRangeLoadsPerExecutor);
                executor.cacheExclusive().setShrinkingOn(shrinkingOn);
            });
        }
    }

    public List<AccordExecutor> executors()
    {
        return Arrays.asList(executors.clone());
    }

    public void waitForQuiescense()
    {
        boolean hadPending;
        try
        {
            do
            {
                hadPending = false;
                List<Future<?>> futures = new ArrayList<>();
                for (AccordExecutor executor : this.executors)
                {
                    hadPending |= executor.hasTasks();
                    futures.add(executor.submit(() -> {}));
                }
                for (Future<?> future : futures)
                    future.get();
                futures.clear();
            }
            while (hadPending);
        }
        catch (ExecutionException e)
        {
            throw new IllegalStateException("Should have never been thrown", e);
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        for (AccordExecutor executor : executors)
        {
            executor.shutdown();
            try
            {
                executor.awaitTermination(1, TimeUnit.MINUTES);
            }
            catch (InterruptedException e)
            {
                throw new RuntimeException(e);
            }
        }
        //TODO shutdown isn't useful by itself, we need a way to "wait" as well.  Should be AutoCloseable or offer awaitTermination as well (think Shutdownable interface)
    }
}
