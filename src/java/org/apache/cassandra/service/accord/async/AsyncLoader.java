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

package org.apache.cassandra.service.accord.async;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import accord.txn.TxnId;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static com.google.common.collect.Iterators.all;


public abstract class AsyncLoader
{
    enum State
    {
        INITIALIZED,
        ACQUIRING,
        LOADING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    protected final AccordCommandStore commandStore;
    protected Future<?> readFuture;
    protected final Map<TxnId, AccordCommand> commands = new HashMap<>();
    protected final Map<PartitionKey, AccordCommandsForKey> keyCommands = new HashMap<>();

    public AsyncLoader(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
    }

    protected abstract Iterator<TxnId> commandsToLoad();

    protected abstract Iterator<PartitionKey> keyCommandsToLoad();

    protected boolean acquireCommand(TxnId txnId)
    {
        AccordCommand command = commandStore.commandCache().acquire(txnId);
        if (command == null)
            return false;
        commands.put(command.txnId(), command);
        return true;
    }

    protected boolean acquireCommandsForKey(PartitionKey key)
    {
        AccordCommandsForKey commandsForKey = commandStore.commandsForKeyCache().acquire(key);
        if (commandsForKey == null)
            return false;
        keyCommands.put(key, commandsForKey);
        return true;
    }

    void releaseResources()
    {
        commands.values().forEach(commandStore.commandCache()::release);
        commands.clear();
        keyCommands.values().forEach(commandStore.commandsForKeyCache()::release);
        keyCommands.clear();
    }

    /**
     * identify required objects, retrieve from cache where possible
     */
    public void setup()
    {
        if (!all(commandsToLoad(), this::acquireCommand))
        {
            releaseResources();
            return;
        }
        if (!all(keyCommandsToLoad(), this::acquireCommandsForKey))
        {
            releaseResources();
            return;
        }
    }

    private boolean attemptAcquire()
    {
        if (!all(commandsToLoad(), this::acquireCommand))
        {
            releaseResources();
            return false;
        }
        if (!all(keyCommandsToLoad(), this::acquireCommandsForKey))
        {
            releaseResources();
            return false;
        }
        return true;
    }

    private Future<?> maybeDispatchReads()
    {
        List<Future<?>> futures = null;
        for (AccordCommand command : commands.values())
        {
            if (command.isLoaded())
                continue;

            if (futures == null) futures = new ArrayList<>();
            Future<?> future = Stage.READ.submit(() -> AccordKeyspace.loadCommand(command));
            futures.add(future);
        }

        for (AccordCommandsForKey commandsForKey : keyCommands.values())
        {
            throw new UnsupportedOperationException("TODO: convert commands for key to stored field");
        }


        return futures != null ? FutureCombiner.allOf(futures) : null;
    }


    public boolean load(BiConsumer<Object, Throwable> callback)
    {
        commandStore.checkThreadId();

        switch (state)
        {
            case INITIALIZED:
                state = State.ACQUIRING;
            case ACQUIRING:
                if (!attemptAcquire())
                {
                    commandStore.executor().execute(() -> callback.accept(null, null));
                    break;
                }
                state = State.LOADING;
            case LOADING:
                if (readFuture == null)
                    readFuture = maybeDispatchReads();

                if (readFuture != null && !readFuture.isDone())
                {
                    // FIXME: this should call back into the AsyncOperation so a separate executor task isn't submitted
                    readFuture.addCallback(callback, commandStore.executor());
                    break;
                }
                // FIXME: the transition from loading to finished feels awkward
                state = State.FINISHED;
            case FINISHED:
                break;
            default:
                throw new IllegalStateException();
        }

        return state == State.FINISHED;
    }

    public boolean isFinished()
    {
        return state == State.FINISHED;
    }
}
