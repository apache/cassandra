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

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Agent;
import accord.api.DataStore;
import accord.api.ProgressLog;
import accord.impl.CommandsForKey;
import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandStores.RangesForEpoch;
import accord.local.CommandStores.RangesForEpochHolder;
import accord.local.NodeTimeService;
import accord.local.PreLoadContext;
import accord.local.SafeCommandStore;
import accord.primitives.RoutableKey;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.service.accord.async.AsyncOperation;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.concurrent.UncheckedInterruptedException;

import static org.apache.cassandra.concurrent.ExecutorFactory.Global.executorFactory;

public class AccordCommandStore implements CommandStore
{
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

    private final int id;
    private final long threadId;
    public final String loggingId;
    private final ExecutorService executor;
    private final AccordStateCache stateCache;
    private final AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache;
    private final AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache;
    private AsyncOperation<?> currentOperation = null;
    private AccordSafeCommandStore current = null;
    private long lastSystemTimestampMicros = Long.MIN_VALUE;

    private final NodeTimeService time;
    private final Agent agent;
    private final DataStore dataStore;
    private final ProgressLog progressLog;
    private final RangesForEpochHolder rangesForEpochHolder;

    public AccordCommandStore(int id,
                              NodeTimeService time,
                              Agent agent,
                              DataStore dataStore,
                              ProgressLog.Factory progressLogFactory,
                              RangesForEpochHolder rangesForEpoch)
    {
        this.id = id;
        this.time = time;
        this.agent = agent;
        this.dataStore = dataStore;
        this.progressLog = progressLogFactory.create(this);
        this.rangesForEpochHolder = rangesForEpoch;
        this.loggingId = String.format("[%s]", id);
        this.executor = executorFactory().sequential(CommandStore.class.getSimpleName() + '[' + id + ']');
        this.threadId = getThreadId(this.executor);
        this.stateCache = new AccordStateCache(8<<20);
        this.commandCache = stateCache.instance(TxnId.class, accord.local.Command.class, AccordSafeCommand::new, AccordObjectSizes::command);
        this.commandsForKeyCache = stateCache.instance(RoutableKey.class, CommandsForKey.class, AccordSafeCommandsForKey::new, AccordObjectSizes::commandsForKey);
    }

    @Override
    public int id()
    {
        return id;
    }

    public void setCacheSize(long bytes)
    {
        checkInStoreThread();
        stateCache.setMaxSize(bytes);
    }

    public long getCacheSize()
    {
        return stateCache.getMaxSize();
    }

    public void checkInStoreThread()
    {
        Invariants.checkState(Thread.currentThread().getId() == threadId);
    }

    public void checkNotInStoreThread()
    {
        Invariants.checkState(Thread.currentThread().getId() != threadId);
    }

    public ExecutorService executor()
    {
        return executor;
    }

    public AccordStateCache.Instance<TxnId, Command, AccordSafeCommand> commandCache()
    {
        return commandCache;
    }

    public AccordStateCache.Instance<RoutableKey, CommandsForKey, AccordSafeCommandsForKey> commandsForKeyCache()
    {
        return commandsForKeyCache;
    }

    @VisibleForTesting
    public AccordStateCache cache()
    {
        return stateCache;
    }

    @VisibleForTesting
    public void clearCache()
    {
        stateCache.clear();
    }

    public void setCurrentOperation(AsyncOperation<?> operation)
    {
        Invariants.checkState(currentOperation == null);
        currentOperation = operation;
    }

    public AsyncOperation<?> getContext()
    {
        Invariants.checkState(currentOperation != null);
        return currentOperation;
    }

    public void unsetCurrentOperation(AsyncOperation<?> operation)
    {
        Invariants.checkState(currentOperation == operation);
        currentOperation = null;
    }

    public long nextSystemTimestampMicros()
    {
        lastSystemTimestampMicros = Math.max(TimeUnit.MILLISECONDS.toMicros(Clock.Global.currentTimeMillis()), lastSystemTimestampMicros + 1);
        return lastSystemTimestampMicros;
    }
    @Override
    public <T> AsyncChain<T> submit(PreLoadContext loadCtx, Function<? super SafeCommandStore, T> function)
    {
        return AsyncOperation.create(this, loadCtx, function);
    }

    public DataStore dataStore()
    {
        return dataStore;
    }

    @Override
    public Agent agent()
    {
        return agent;
    }

    NodeTimeService time()
    {
        return time;
    }

    ProgressLog progressLog()
    {
        return progressLog;
    }

    RangesForEpoch ranges()
    {
        return rangesForEpochHolder.get();
    }

    @Override
    public AsyncChain<Void> execute(PreLoadContext preLoadContext, Consumer<? super SafeCommandStore> consumer)
    {
        return AsyncOperation.create(this, preLoadContext, consumer);
    }

    public void executeBlocking(Runnable runnable)
    {
        try
        {
            executor.submit(runnable).get();
        }
        catch (InterruptedException e)
        {
            throw new UncheckedInterruptedException(e);
        }
        catch (ExecutionException e)
        {
            throw new RuntimeException(e);
        }
    }

    public AccordSafeCommandStore beginOperation(PreLoadContext preLoadContext,
                                                 Map<TxnId, AccordSafeCommand> commands,
                                                 Map<RoutableKey, AccordSafeCommandsForKey> commandsForKeys)
    {
        Invariants.checkState(current == null);
        commands.values().forEach(AccordSafeState::preExecute);
        commandsForKeys.values().forEach(AccordSafeState::preExecute);
        current = new AccordSafeCommandStore(preLoadContext, commands, commandsForKeys, this);
        return current;
    }

    public void completeOperation(AccordSafeCommandStore store,
                                  Map<TxnId, AccordSafeCommand> commands,
                                  Map<RoutableKey, AccordSafeCommandsForKey> commandsForKeys)
    {
        Invariants.checkState(current == store);
        current.complete();
        current = null;
    }

    public void abortCurrentOperation()
    {
        current = null;
    }

    @Override
    public void shutdown()
    {
        executor.shutdown();
    }
}
