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

import java.util.function.Supplier;

import accord.api.Agent;
import accord.api.ConfigurationService.EpochReady;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.CommandStores;
import accord.local.Node;
import accord.local.NodeTimeService;
import accord.local.ShardDistributor;
import accord.primitives.Range;
import accord.topology.Topology;
import accord.utils.RandomSource;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;

public class AccordCommandStores extends CommandStores
{
    AccordCommandStores(NodeTimeService time, Agent agent, DataStore store, RandomSource random,
                        ShardDistributor shardDistributor, ProgressLog.Factory progressLogFactory, AccordJournal journal)
    {
        super(time, agent, store, random, shardDistributor, progressLogFactory, AccordCommandStore.factory(journal));
        setCacheSize(maxCacheSize());
    }

    static Factory factory(AccordJournal journal)
    {
        return (time, agent, store, random, shardDistributor, progressLogFactory) ->
               new AccordCommandStores(time, agent, store, random, shardDistributor, progressLogFactory, journal);
    }

    @Override
    protected boolean shouldBootstrap(Node node, Topology previous, Topology updated, Range range)
    {
        if (!super.shouldBootstrap(node, previous, updated, range))
            return false;
        // we see new ranges when a new keyspace is added, so avoid bootstrap in these cases
        return contains(previous, ((AccordRoutingKey)  range.start()).keyspace());
    }

    private static boolean contains(Topology previous, String searchKeyspace)
    {
        for (Range range : previous.ranges())
        {
            String keyspace = ((AccordRoutingKey)  range.start()).keyspace();
            if (keyspace.equals(searchKeyspace))
                return true;
        }
        return false;
    }

    private long cacheSize;

    synchronized void setCacheSize(long bytes)
    {
        cacheSize = bytes;
        refreshCacheSizes();
    }

    synchronized void refreshCacheSizes()
    {
        if (count() == 0)
            return;
        long perStore = cacheSize / count();
        // TODO (low priority, safety): we might transiently breach our limit if we increase one store before decreasing another
        forEach(commandStore -> ((AccordSafeCommandStore) commandStore).commandStore().setCacheSize(perStore));
    }

    private static long maxCacheSize()
    {
        return 5 << 20; // TODO (required): make configurable
    }

    @Override
    public synchronized Supplier<EpochReady> updateTopology(Node node, Topology newTopology, boolean startSync)
    {
        Supplier<EpochReady> start = super.updateTopology(node, newTopology, startSync);
        return () -> {
            EpochReady ready = start.get();
            ready.metadata.addCallback(() -> {
                synchronized (this)
                {
                    refreshCacheSizes();
                }
            });
            return ready;
        };
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        //TODO shutdown isn't useful by itself, we need a way to "wait" as well.  Should be AutoCloseable or offer awaitTermination as well (think Shutdownable interface)
    }
}
