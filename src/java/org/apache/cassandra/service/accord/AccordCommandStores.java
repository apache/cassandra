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
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.LocalListeners;
import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeCommandStoreService;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.topology.Topology;
import accord.utils.RandomSource;
import org.apache.cassandra.cache.CacheSize;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.metrics.AccordStateCacheMetrics;
import org.apache.cassandra.metrics.CacheSizeMetrics;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordCommandStore.CommandStoreExecutor;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;
import static org.apache.cassandra.concurrent.Stage.ACCORD_MIGRATION;
import static org.apache.cassandra.concurrent.Stage.ACCORD_RANGE_LOADER;
import static org.apache.cassandra.concurrent.Stage.MUTATION;
import static org.apache.cassandra.concurrent.Stage.READ;

public class AccordCommandStores extends CommandStores implements CacheSize
{
    public static final String ACCORD_STATE_CACHE = "AccordStateCache";

    private final CacheSizeMetrics cacheSizeMetrics;
    private final CommandStoreExecutor[] executors;
    private long cacheSize;

    AccordCommandStores(NodeCommandStoreService node, Agent agent, DataStore store, RandomSource random,
                        ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, LocalListeners.Factory listenerFactory,
                        AccordJournal journal, CommandStoreExecutor[] executors)
    {
        super(node, agent, store, random, shardDistributor, progressLogFactory, listenerFactory,
              AccordCommandStore.factory(journal, id -> executors[id % executors.length]));
        setCapacity(DatabaseDescriptor.getAccordCacheSizeInMiB() << 20);
        this.executors = executors;
        this.cacheSizeMetrics = new CacheSizeMetrics(ACCORD_STATE_CACHE, this);
    }

    static Factory factory(AccordJournal journal)
    {
        return (time, agent, store, random, shardDistributor, progressLogFactory, listenerFactory) -> {
            CommandStoreExecutor[] executors = new CommandStoreExecutor[DatabaseDescriptor.getAccordShardCount()];
            for (int id = 0; id < executors.length; id++)
            {
                AccordStateCacheMetrics metrics = new AccordStateCacheMetrics(ACCORD_STATE_CACHE);
                AccordStateCache stateCache = new AccordStateCache(Stage.READ.executor(), Stage.MUTATION.executor(), 8 << 20, metrics);
                executors[id] = new CommandStoreExecutor(stateCache, executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + id + ']'));
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
        refreshCacheSizes();
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
        for (CommandStoreExecutor executor : executors)
            size += executor.size();
        return size;
    }

    @Override
    public long weightedSize()
    {
        long size = 0;
        for (CommandStoreExecutor executor : executors)
            size += executor.weightedSize();
        return size;
    }

    synchronized void refreshCacheSizes()
    {
        if (count() == 0)
            return;
        long perExecutor = cacheSize / executors.length;
        // TODO (low priority, safety): we might transiently breach our limit if we increase one store before decreasing another
        for (CommandStoreExecutor executor : executors)
            executor.execute(() -> executor.setCapacity(perExecutor));
    }

    public void waitForQuiescense()
    {
        boolean hadPending;
        try
        {
            List<ExecutorPlus> executors = new ArrayList<>();
            for (CommandStoreExecutor executor : this.executors)
                executors.add(executor.delegate);

            executors.add(READ.executor());
            executors.add(MUTATION.executor());
            executors.add(ACCORD_MIGRATION.executor());
            executors.add(ACCORD_RANGE_LOADER.executor());

            do
            {
                hadPending = false;
                List<Future<?>> futures = new ArrayList<>();
                for (ExecutorPlus executor : executors)
                {
                    if (!hadPending && (executor.getPendingTaskCount() > 0 || executor.getActiveTaskCount() > 0))
                        hadPending = true;
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
        for (CommandStoreExecutor executor : executors)
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
