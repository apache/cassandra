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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import accord.api.Agent;
import accord.api.Store;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.txn.Timestamp;
import org.apache.cassandra.concurrent.NamedThreadFactory;

public class AccordCommandStores extends CommandStores
{
    private final ExecutorService[] executors;

    public AccordCommandStores(int numShards, Node.Id node, Function<Timestamp, Timestamp> uniqueNow, Agent agent, Store store)
    {
        super(numShards, node, uniqueNow, agent, store);
        this.executors = new ExecutorService[numShards];
        for (int i=0; i<numShards; i++)
        {
            int index = i;
            executors[i] = Executors.newSingleThreadExecutor(r -> {
                Thread thread = new Thread(r);
                thread.setName(NamedThreadFactory.globalPrefix() + CommandStore.class.getSimpleName() + '[' + node + ':' + index + ']');
                return thread;
            });
        }
    }

    @Override
    protected CommandStore createCommandStore(int generation, int index, KeyRanges ranges)
    {
        return new AccordCommandStore(generation,
                                      index,
                                      numShards,
                                      node,
                                      uniqueNow,
                                      agent,
                                      store,
                                      ranges,
                                      this::getLocalTopology,
                                      executors[index]);
    }

    void setCacheSize(long bytes)
    {
        setup(commandStore -> ((AccordCommandStore) commandStore).setCacheSize(bytes));
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        for (ExecutorService executor : executors)
            executor.shutdown();
    }
}
