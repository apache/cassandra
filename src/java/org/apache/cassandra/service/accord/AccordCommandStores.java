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

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.local.CommandStore;
import accord.local.CommandStores;
import accord.local.Node;
import org.apache.cassandra.concurrent.ExecutorFactory;
import org.apache.cassandra.utils.ExecutorUtils;

public class AccordCommandStores extends CommandStores
{
    private final ExecutorService[] executors;

    public AccordCommandStores(int numShards, Node node, Agent agent, DataStore store,
                               ProgressLog.Factory progressLogFactory)
    {
        super(numShards, node, agent, store, progressLogFactory);
        this.executors = new ExecutorService[numShards];
        for (int i=0; i<numShards; i++)
        {
            executors[i] = ExecutorFactory.Global.executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + node + ':' + i + ']');
        }
    }

    @Override
    protected CommandStore createCommandStore(int generation, int index, int numShards, Node node, Agent agent, DataStore store, ProgressLog.Factory progressLogFactory, CommandStore.RangesForEpoch rangesForEpoch)
    {
        return new AccordCommandStore(generation, index, numShards, node::uniqueNow, node.topology()::epoch, agent, store, progressLogFactory, rangesForEpoch, executors[index]);
    }

    void setCacheSize(long bytes)
    {
        setup(commandStore -> ((AccordCommandStore) commandStore).setCacheSize(bytes));
    }

    @Override
    public synchronized void shutdown()
    {
        super.shutdown();
        //TODO shutdown isn't useful by itself, we need a way to "wait" as well.  Should be AutoCloseable or offer awaitTermination as well (think Shutdownable interface)
        ExecutorUtils.shutdown(executors);
    }
}
