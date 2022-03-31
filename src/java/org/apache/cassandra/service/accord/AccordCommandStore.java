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

import java.io.IOException;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import com.google.common.base.Preconditions;

import accord.api.Agent;
import accord.api.Key;
import accord.api.Store;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.local.Node;
import accord.topology.KeyRanges;
import accord.topology.Topology;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.AsyncPromise;
import org.apache.cassandra.utils.concurrent.Future;

public class AccordCommandStore extends CommandStore
{
    public static long maxCacheSize()
    {
        return 5 << 20; // TODO: make configurable
    }

    private class ProcessingContext extends AsyncPromise<Void> implements Runnable
    {
        private final Consumer<? super CommandStore> consumer;
        private final HashSet<AccordCommand> commands = new HashSet<>();
        private final HashSet<AccordCommandsForKey> commandsForKeys = new HashSet<>();

        public ProcessingContext(Consumer<? super CommandStore> consumer)
        {
            this.consumer = consumer;
        }

        private void persistChanges() throws IOException
        {
            // TODO: accumulate updates and return partition updates
            for (AccordCommand command : commands)
                command.save();
            for (AccordCommandsForKey commandsForKey : commandsForKeys)
                commandsForKey.save();
        }

        private void releaseResources()
        {
            commands.forEach(commandCache::release);
            commandsForKeys.forEach(commandsForKeyCache::release);
        }

        @Override
        public void run()
        {
            Preconditions.checkState(currentCtx == null);
            try
            {
                consumer.accept(AccordCommandStore.this);
                persistChanges();
                releaseResources();
                setSuccess(null);
            }
            catch (Throwable e)
            {
                tryFailure(e);
            }
            finally
            {
                Preconditions.checkState(currentCtx == this);
                currentCtx = null;
            }
        }
    }

    private static long getThreadId(ExecutorService executor)
    {
        try
        {
            return executor.submit(() -> Thread.currentThread().getId()).get();
        }
        catch (InterruptedException e)
        {
            throw new AssertionError(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    private final long threadId;
    private final ExecutorService executor;
    private final AccordStateCache stateCache;
    private final AccordStateCache.Instance<TxnId, AccordCommand> commandCache;
    private final AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> commandsForKeyCache;
    private ProcessingContext currentCtx = null;

    public AccordCommandStore(int generation,
                              int index,
                              int numShards,
                              Node.Id nodeId,
                              Function<Timestamp, Timestamp> uniqueNow,
                              Agent agent,
                              Store store,
                              KeyRanges ranges,
                              Supplier<Topology> localTopologySupplier, ExecutorService executor)
    {
        super(generation, index, numShards, nodeId, uniqueNow, agent, store, ranges, localTopologySupplier);
        this.executor = executor;
        this.threadId = getThreadId(executor);
        this.stateCache = new AccordStateCache(maxCacheSize() / numShards);
        this.commandCache = stateCache.instance(TxnId.class,
                                                AccordCommand.class,
                                                txnId -> AccordKeyspace.loadCommand(this, txnId));
        this.commandsForKeyCache = stateCache.instance(PartitionKey.class,
                                                       AccordCommandsForKey.class,
                                                       key -> AccordKeyspace.loadCommandsForKey(this, key));
    }

    public void checkThreadId()
    {
        Preconditions.checkState(Thread.currentThread().getId() == threadId);
    }

    public Executor executor()
    {
        return executor;
    }

    public AccordStateCache.Instance<TxnId, AccordCommand> commandCache()
    {
        return commandCache;
    }

    public AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> commandsForKeyCache()
    {
        return commandsForKeyCache;
    }

    @Override
    public Command command(TxnId txnId)
    {
        // FIXME: these should be pre-loaded and fetched from the context
        Preconditions.checkState(currentCtx != null);
        AccordCommand command = commandCache.acquire(txnId);
        Preconditions.checkArgument(command != null);
        currentCtx.commands.add(command);
        return command;
    }

    @Override
    public CommandsForKey commandsForKey(Key key)
    {
        // FIXME: these should be pre-loaded and fetched from the context
        Preconditions.checkState(currentCtx != null);
        Preconditions.checkArgument(key instanceof PartitionKey);
        AccordCommandsForKey commandsForKey = commandsForKeyCache.acquire((PartitionKey) key);
        Preconditions.checkArgument(commandsForKey != null);
        currentCtx.commandsForKeys.add(commandsForKey);
        return commandsForKey;
    }

    @Override
    protected void onRangeUpdate(KeyRanges previous, KeyRanges current)
    {
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public Future<Void> process(Consumer<? super CommandStore> consumer)
    {
        ProcessingContext ctx = new ProcessingContext(consumer);
        executor.execute(ctx);
        return ctx;
    }

    @Override
    public void shutdown()
    {
        throw new UnsupportedOperationException("TODO");
    }
}
